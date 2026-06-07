//! The worker: a bounded claim-and-dispatch loop over a [`Store`].
//!
//! A [`Worker`] keeps an in-flight set of at most `N` running handlers and feeds
//! it from the queue. Each iteration fills every free slot by claiming one job at
//! a time, then waits until a handler finishes, new work might be available, or a
//! shutdown is signalled. When a handler finishes, the worker settles its outcome
//! against the store. Horizontal scale comes from running more worker processes,
//! each its own loop.

mod registry;

use crate::error::Error;
use crate::outcome::Outcome;
use crate::store::{Settlement, Store};
use crate::task::Handler;
use chrono::{DateTime, Utc};
use registry::{Registry, RunInput, RunReport};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

/// The unconstrained priority floor: `priority >= 0` admits every tier, so the
/// claim is ordered strictly by priority then age. The anti-starvation rotation
/// that raises this floor to reserve slots for lower tiers arrives in a later
/// phase.
const UNCONSTRAINED_FLOOR: i16 = 0;

/// Worker configuration, all set at construction with conservative defaults.
#[derive(Debug, Clone)]
struct WorkerConfig {
    concurrency: usize,
    poll_max: Duration,
    lease: Duration,
}

impl Default for WorkerConfig {
    fn default() -> WorkerConfig {
        WorkerConfig {
            concurrency: default_concurrency(),
            poll_max: Duration::from_secs(30),
            lease: Duration::from_secs(15 * 60),
        }
    }
}

/// The default in-flight bound: `max(1, min(8, cores / 2))`.
///
/// A safety floor rather than an optimum: low enough that thread-blocking
/// handlers cannot starve the runtime on a small host, capped so it stays modest
/// on a large one. Raise it for I/O-bound work.
fn default_concurrency() -> usize {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    (cores / 2).clamp(1, 8)
}

/// Builds a [`Worker`] over a shared state `S` and a [`Store`].
pub struct WorkerBuilder<S> {
    state: S,
    store: Arc<dyn Store>,
    registry: Registry<S>,
    config: WorkerConfig,
}

impl<S> WorkerBuilder<S>
where
    S: Send + Sync + 'static,
{
    /// Register a handler type. This both teaches the worker how to deserialize
    /// and run the kind and adds it to the claim filter.
    #[must_use]
    pub fn register<T>(mut self) -> Self
    where
        T: Handler<S>,
    {
        self.registry.register::<T>();
        self
    }

    /// The maximum number of jobs run concurrently. Defaults to
    /// `max(1, min(8, cores / 2))`; a value of `0` is clamped to `1`.
    #[must_use]
    pub fn concurrency(mut self, n: usize) -> Self {
        self.config.concurrency = n.max(1);
        self
    }

    /// The upper bound on how long the loop waits when nothing is scheduled.
    /// Defaults to 30 seconds.
    #[must_use]
    pub fn poll_max(mut self, d: Duration) -> Self {
        self.config.poll_max = d;
        self
    }

    /// The default claim lease. Defaults to 15 minutes; a task may request a
    /// longer one through `Task::lease`.
    #[must_use]
    pub fn lease(mut self, d: Duration) -> Self {
        self.config.lease = d;
        self
    }

    /// Finish building the worker.
    pub fn build(self) -> Worker<S> {
        Worker {
            state: Arc::new(self.state),
            store: self.store,
            registry: Arc::new(self.registry),
            config: self.config,
            identity: worker_identity(),
        }
    }
}

/// A worker that claims, runs, and settles jobs for its registered kinds.
pub struct Worker<S> {
    state: Arc<S>,
    store: Arc<dyn Store>,
    registry: Arc<Registry<S>>,
    config: WorkerConfig,
    identity: String,
}

impl<S> Worker<S>
where
    S: Send + Sync + 'static,
{
    /// Start building a worker over `state` and `store`.
    pub fn builder(state: S, store: Arc<dyn Store>) -> WorkerBuilder<S> {
        WorkerBuilder {
            state,
            store,
            registry: Registry::new(),
            config: WorkerConfig::default(),
        }
    }

    /// Run the claim/dispatch loop until `shutdown` is triggered, then drain the
    /// in-flight handlers and return.
    pub async fn run(self, shutdown: CancellationToken) {
        let kinds = self.registry.kinds();
        // A worker with no registered kinds can never claim anything; running its
        // loop would just spin on empty waits.
        if kinds.is_empty() {
            return;
        }

        let mut running: JoinSet<FinishedRun> = JoinSet::new();

        'outer: loop {
            // Fill every free slot, one claimed row per slot, until the queue has
            // nothing claimable right now or we are shutting down.
            while running.len() < self.config.concurrency {
                if shutdown.is_cancelled() {
                    break 'outer;
                }
                match self
                    .store
                    .claim_next(
                        &kinds,
                        UNCONSTRAINED_FLOOR,
                        self.config.lease,
                        &self.identity,
                    )
                    .await
                {
                    Ok(Some(job)) => self.spawn(&mut running, job, &shutdown),
                    Ok(None) => break,
                    Err(error) => {
                        // Never crash on a transient storage error; back off to
                        // the next wait and try again.
                        tracing::warn!(%error, "claim failed; will retry");
                        break;
                    }
                }
            }

            // Wait for the soonest of: a handler finishing, the poll interval, or
            // shutdown. The NOTIFY-driven wakeup and the next-visible-at timeout
            // arrive in a later phase; for now a bounded poll picks up new work.
            let poll = self.config.poll_max.min(Duration::from_millis(200));
            if running.is_empty() {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(poll) => {}
                }
            } else {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    Some(joined) = running.join_next() => self.reap(joined).await,
                    _ = tokio::time::sleep(poll) => {}
                }
            }
        }

        // Drain: let the in-flight handlers run to completion and settle them.
        // Cooperative cancellation and the forced-release deadline arrive in a
        // later phase.
        while let Some(joined) = running.join_next().await {
            self.reap(joined).await;
        }
    }

    /// Spawn a claimed job's handler into the in-flight set.
    fn spawn(
        &self,
        running: &mut JoinSet<FinishedRun>,
        job: crate::store::JobRecord,
        shutdown: &CancellationToken,
    ) {
        let id = job.id;
        let kind = job.kind.clone();
        let failure_count = job.failure_count;

        let input = RunInput {
            payload: job.payload,
            carry: job.carry,
            run_count: job.run_count.max(0) as u32,
            // History loading lands with the journal phase; empty for now.
            history: Vec::new(),
            state: Arc::clone(&self.state),
            cancel: shutdown.clone(),
        };

        let report = self.registry.dispatch(&kind, input);
        running.spawn(async move {
            let report = match report {
                Ok(future) => future.await,
                Err(error) => Err(error),
            };
            FinishedRun {
                id,
                failure_count,
                report,
            }
        });
    }

    /// Handle one joined task: settle it, or log a lost panic.
    async fn reap(&self, joined: Result<FinishedRun, tokio::task::JoinError>) {
        match joined {
            Ok(finished) => {
                if let Err(error) = self.settle(finished).await {
                    tracing::warn!(%error, "settle failed");
                }
            }
            Err(join_error) => {
                // A panicked or aborted handler loses its identity here; the job
                // stays claimed and is recovered by lease expiry in a later phase.
                tracing::error!(%join_error, "handler task did not complete cleanly");
            }
        }
    }

    /// Compute and apply the settlement for one finished run, guarded by claim
    /// ownership.
    async fn settle(&self, finished: FinishedRun) -> Result<(), Error> {
        let now = Utc::now();
        let next_failures = finished.failure_count.saturating_add(1);

        let settlement = match finished.report {
            // The payload or carry could not be decoded: the job is unrunnable, so
            // give up on it rather than spin.
            Err(error) => {
                tracing::error!(%error, "job could not be dispatched; marking dead");
                Settlement::Dead {
                    finished_at: now,
                    failure_count: next_failures,
                }
            }
            Ok(report) => self.settlement_for(&report, now, next_failures),
        };

        self.store
            .settle(finished.id, &self.identity, settlement)
            .await?;
        Ok(())
    }

    /// Route a successful run's outcome (or its failure) to a settlement.
    ///
    /// The retry delay is a placeholder until the backoff phase supplies the
    /// curve; the dead-on-permanent and pause paths are already their final shape.
    fn settlement_for(
        &self,
        report: &RunReport,
        now: DateTime<Utc>,
        next_failures: i32,
    ) -> Settlement {
        match &report.result {
            Ok(Outcome::Completed { .. }) => Settlement::Complete { finished_at: now },
            Ok(Outcome::Pause { resume_in, .. }) => Settlement::Pause {
                visible_at: add_duration(now, *resume_in),
                carry: report.carry.clone(),
            },
            Err(error) if error.is_permanent() => Settlement::Dead {
                finished_at: now,
                failure_count: next_failures,
            },
            Err(_retryable) => Settlement::Retry {
                // TODO(P2): apply the Fibonacci backoff curve and proportional
                // jitter here instead of retrying immediately.
                visible_at: now,
                failure_count: next_failures,
            },
        }
    }
}

/// The result of one finished handler task, carrying what settlement needs.
struct FinishedRun {
    id: Ulid,
    failure_count: i32,
    report: Result<RunReport, Error>,
}

/// Add a `std::time::Duration` to a UTC instant, saturating on overflow.
fn add_duration(now: DateTime<Utc>, delta: Duration) -> DateTime<Utc> {
    match chrono::Duration::from_std(delta) {
        Ok(delta) => now.checked_add_signed(delta).unwrap_or(now),
        Err(_) => now,
    }
}

/// The `host:pid` identity recorded on every claim, for diagnostics.
fn worker_identity() -> String {
    let host = std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "unknown".to_owned());
    format!("{host}:{pid}", pid = std::process::id())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::Context;
    use crate::outcome::TaskError;
    use crate::queue::Queue;
    use crate::test_support::FakeStore;
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Shared state that records peak concurrency and completion count.
    #[derive(Clone)]
    struct Counters {
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        ran: Arc<AtomicUsize>,
    }

    impl Counters {
        fn new() -> Counters {
            Counters {
                active: Arc::new(AtomicUsize::new(0)),
                peak: Arc::new(AtomicUsize::new(0)),
                ran: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    struct SlowJob;

    impl crate::task::Task for SlowJob {
        const KIND: &'static str = "slow";
        type Carry = ();
    }

    impl Handler<Counters> for SlowJob {
        async fn handle(
            &self,
            _ctx: &mut Context<()>,
            state: &Counters,
        ) -> Result<Outcome, TaskError> {
            let now = state.active.fetch_add(1, Ordering::SeqCst) + 1;
            state.peak.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(40)).await;
            state.active.fetch_sub(1, Ordering::SeqCst);
            state.ran.fetch_add(1, Ordering::SeqCst);
            Ok(Outcome::completed())
        }
    }

    /// Poll until `cond` holds or the deadline passes.
    async fn wait_until(mut cond: impl FnMut() -> bool) {
        for _ in 0..200 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition not met within the deadline");
    }

    #[tokio::test]
    async fn loop_completes_all_jobs_within_the_concurrency_bound() {
        let store = FakeStore::new();
        let queue = Queue::new(Arc::new(store.clone()));
        for _ in 0..6 {
            queue.enqueue(SlowJob).await.expect("enqueue");
        }

        let counters = Counters::new();
        let worker = Worker::builder(counters.clone(), Arc::new(store.clone()))
            .register::<SlowJob>()
            .concurrency(2)
            .build();

        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(worker.run(shutdown.clone()));

        wait_until(|| counters.ran.load(Ordering::SeqCst) == 6).await;
        shutdown.cancel();
        handle.await.expect("worker loop joins");

        assert_eq!(counters.ran.load(Ordering::SeqCst), 6);
        assert!(
            counters.peak.load(Ordering::SeqCst) <= 2,
            "peak concurrency {} exceeded the bound of 2",
            counters.peak.load(Ordering::SeqCst)
        );
        assert_eq!(store.count(crate::store::Status::Completed), 6);
    }

    #[tokio::test]
    async fn worker_with_no_kinds_returns_immediately() {
        let store = FakeStore::new();
        let worker: Worker<Counters> = Worker::builder(Counters::new(), Arc::new(store)).build();
        // Should return without needing a shutdown signal.
        worker.run(CancellationToken::new()).await;
    }
}
