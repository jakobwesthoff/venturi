//! The worker: a bounded claim-and-dispatch loop over a [`Store`].
//!
//! A [`Worker`] keeps an in-flight set of at most `N` running handlers and feeds
//! it from the queue. Each iteration fills every free slot by claiming one job at
//! a time, then waits until a handler finishes, new work might be available, or a
//! shutdown is signalled. When a handler finishes, the worker settles its outcome
//! against the store. Horizontal scale comes from running more worker processes,
//! each its own loop.

mod registry;

use crate::backoff::{Backoff, retry_delay};
use crate::context::JournalEntry;
use crate::error::Error;
use crate::outcome::Outcome;
use crate::store::{JournalAppend, JournalOutcome, Settlement, Store};
use crate::task::Handler;
use chrono::{DateTime, Utc};
use registry::{Registry, RunInput, RunReport};
use std::collections::HashMap;
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

/// The default failure backstop: a high ceiling on retryable failures before a
/// job is forced to dead. It is a failsafe against a task that never recognizes a
/// permanent failure; a task is expected to end itself sooner via
/// `TaskError::permanent`.
const DEFAULT_BACKSTOP: u32 = 20;

/// The worker-level proportional jitter fraction applied to retry delays.
const DEFAULT_JITTER_FRACTION: f64 = 0.5;

/// Worker configuration, all set at construction with conservative defaults.
#[derive(Debug, Clone)]
struct WorkerConfig {
    concurrency: usize,
    poll_max: Duration,
    lease: Duration,
    shutdown_timeout: Duration,
    backoff: Backoff,
    jitter_fraction: f64,
    backstop: Option<u32>,
}

impl Default for WorkerConfig {
    fn default() -> WorkerConfig {
        WorkerConfig {
            concurrency: default_concurrency(),
            poll_max: Duration::from_secs(30),
            lease: Duration::from_secs(15 * 60),
            shutdown_timeout: Duration::from_secs(30),
            backoff: Backoff::default(),
            jitter_fraction: DEFAULT_JITTER_FRACTION,
            backstop: Some(DEFAULT_BACKSTOP),
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

    /// The grace window handlers get to wind down on a graceful shutdown before
    /// stragglers are force-aborted and released. Defaults to 30 seconds.
    #[must_use]
    pub fn shutdown_timeout(mut self, d: Duration) -> Self {
        self.config.shutdown_timeout = d;
        self
    }

    /// The worker-level default retry backoff (base and cap). A task may override
    /// it through `Task::backoff`. Defaults to a 1-second base and 5-minute cap.
    #[must_use]
    pub fn backoff(mut self, backoff: Backoff) -> Self {
        self.config.backoff = backoff;
        self
    }

    /// The absolute backstop on retryable failures before a job is forced to dead.
    ///
    /// `Some(n)` caps a job at `n` failed executions; `None` disables the
    /// backstop, leaving the give-up decision entirely to the task. Defaults to a
    /// high value so a task's own `TaskError::permanent` is the primary mechanism.
    #[must_use]
    pub fn backstop(mut self, backstop: Option<u32>) -> Self {
        self.config.backstop = backstop;
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
        // Track the job behind each in-flight task so a forced shutdown can
        // release the stragglers it has to abort.
        let mut inflight: HashMap<tokio::task::Id, InflightJob> = HashMap::new();

        'outer: loop {
            // Recover abandoned claims first, returning their work to the pool.
            self.recover_stale().await;

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
                    Ok(Some(job)) => {
                        self.apply_task_lease(&job).await;
                        let history = self.history_for(job.id).await;
                        let tracked = InflightJob {
                            id: job.id,
                            kind: job.kind.clone(),
                            run_no: job.run_count,
                        };
                        let task_id = self.spawn(&mut running, job, history, &shutdown);
                        inflight.insert(task_id, tracked);
                    }
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
                    Some(joined) = running.join_next_with_id() => {
                        self.reap(joined, &mut inflight).await;
                    }
                    _ = tokio::time::sleep(poll) => {}
                }
            }
        }

        self.drain(running, inflight).await;
    }

    /// Drain on shutdown: the cancel signal is already raised, so give handlers
    /// `shutdown_timeout` to wind down on their own (typically a `Pause`,
    /// settled normally); at the deadline, force-abort whatever remains and
    /// release it so another worker can pick it up immediately.
    async fn drain(
        &self,
        mut running: JoinSet<FinishedRun>,
        mut inflight: HashMap<tokio::task::Id, InflightJob>,
    ) {
        let deadline = tokio::time::sleep(self.config.shutdown_timeout);
        tokio::pin!(deadline);

        while !running.is_empty() {
            tokio::select! {
                joined = running.join_next_with_id() => match joined {
                    Some(result) => self.reap(result, &mut inflight).await,
                    None => break,
                },
                _ = &mut deadline => {
                    // Cooperative wind-down ran out of time; stop waiting.
                    break;
                }
            }
        }

        // Abort any straggler and release its job. The ownership guard makes a
        // release a no-op if the handler in fact settled in the meantime.
        running.abort_all();
        while running.join_next_with_id().await.is_some() {}
        for (_, job) in inflight.drain() {
            self.release(&job).await;
        }
    }

    /// Load a job's journal as the history a handler sees, tolerating a read
    /// failure by handing the handler an empty history rather than failing the run.
    async fn history_for(&self, id: Ulid) -> Vec<JournalEntry> {
        match self.store.journal(id).await {
            Ok(records) => records.into_iter().map(JournalEntry::from_record).collect(),
            Err(error) => {
                tracing::warn!(%error, "could not load job history; proceeding with none");
                Vec::new()
            }
        }
    }

    /// Recover abandoned claims: re-pend each expired lease as a failed execution
    /// with backoff and a `stale-recovered` journal entry. Runs opportunistically
    /// at the start of each loop iteration, so the system self-heals without a
    /// dedicated process.
    async fn recover_stale(&self) {
        let stale = match self.store.find_stale().await {
            Ok(stale) => stale,
            Err(error) => {
                tracing::warn!(%error, "could not scan for stale claims");
                return;
            }
        };

        let now = Utc::now();
        for job in stale {
            let next_failures = job.failure_count.saturating_add(1);
            let attempt = next_failures.max(0) as u32;
            let delay = retry_delay(
                &self.config.backoff,
                self.config.jitter_fraction,
                attempt,
                job.id,
            );
            let note = format!(
                "lease expired; worker {} presumed dead",
                job.claimed_by.as_deref().unwrap_or("unknown"),
            );
            let journal = JournalAppend {
                kind: job.kind.clone(),
                run_no: job.run_count,
                recorded_at: now,
                outcome: JournalOutcome::StaleRecovered,
                note: Some(note),
                attachment: None,
            };
            if let Err(error) = self
                .store
                .recover(job.id, add_duration(now, delay), next_failures, journal)
                .await
            {
                tracing::warn!(%error, "stale-claim recovery failed");
            }
        }
    }

    /// Apply a per-task lease override after the claim, which only stamps the
    /// worker default. A no-op when the task uses the default.
    async fn apply_task_lease(&self, job: &crate::store::JobRecord) {
        let Some(lease) = self.registry.lease_for(&job.kind, &job.payload) else {
            return;
        };
        if lease == self.config.lease {
            return;
        }
        if let Err(error) = self.store.extend_lease(job.id, &self.identity, lease).await {
            tracing::warn!(%error, "could not apply task lease override");
        }
    }

    /// Spawn a claimed job's handler into the in-flight set, returning the spawned
    /// task's id so the caller can track the job behind it.
    fn spawn(
        &self,
        running: &mut JoinSet<FinishedRun>,
        job: crate::store::JobRecord,
        history: Vec<JournalEntry>,
        shutdown: &CancellationToken,
    ) -> tokio::task::Id {
        let id = job.id;
        let kind = job.kind.clone();
        let run_no = job.run_count;
        let failure_count = job.failure_count;

        let input = RunInput {
            payload: job.payload,
            carry: job.carry,
            run_count: job.run_count.max(0) as u32,
            history,
            state: Arc::clone(&self.state),
            cancel: shutdown.clone(),
        };

        let report = self.registry.dispatch(&kind, input);
        running
            .spawn(async move {
                let report = match report {
                    Ok(future) => future.await,
                    Err(error) => Err(error),
                };
                FinishedRun {
                    id,
                    kind,
                    run_no,
                    failure_count,
                    report,
                }
            })
            .id()
    }

    /// Handle one joined task: drop its in-flight tracking and settle it, or log a
    /// lost panic.
    async fn reap(
        &self,
        joined: Result<(tokio::task::Id, FinishedRun), tokio::task::JoinError>,
        inflight: &mut HashMap<tokio::task::Id, InflightJob>,
    ) {
        match joined {
            Ok((task_id, finished)) => {
                inflight.remove(&task_id);
                if let Err(error) = self.settle(finished).await {
                    tracing::warn!(%error, "settle failed");
                }
            }
            Err(join_error) => {
                // A panicked or aborted handler does not settle itself. Drop its
                // tracking; the job stays claimed and is recovered by lease expiry.
                inflight.remove(&join_error.id());
                tracing::error!(%join_error, "handler task did not complete cleanly");
            }
        }
    }

    /// Release a job abandoned by a forced shutdown: back to pending immediately,
    /// recorded as a `released` event, not a failure. Guarded by claim ownership.
    async fn release(&self, job: &InflightJob) {
        let now = Utc::now();
        let journal = JournalAppend {
            kind: job.kind.clone(),
            run_no: job.run_no,
            recorded_at: now,
            outcome: JournalOutcome::Released,
            note: Some("released by graceful shutdown".to_owned()),
            attachment: None,
        };
        let settlement = Settlement::Release { visible_at: now };
        if let Err(error) = self
            .store
            .settle(job.id, &self.identity, settlement, journal)
            .await
        {
            tracing::warn!(%error, "release failed");
        }
    }

    /// Compute and apply the settlement for one finished run, guarded by claim
    /// ownership, recording one journal entry for the execution.
    async fn settle(&self, finished: FinishedRun) -> Result<(), Error> {
        let now = Utc::now();
        let next_failures = finished.failure_count.saturating_add(1);

        let (settlement, note, attachment) = match finished.report {
            // The payload or carry could not be decoded: the job is unrunnable, so
            // give up on it rather than spin.
            Err(error) => {
                tracing::error!(%error, "job could not be dispatched; marking dead");
                let settlement = Settlement::Dead {
                    finished_at: now,
                    failure_count: next_failures,
                };
                (settlement, Some(error.to_string()), None)
            }
            Ok(report) => {
                let settlement = self.settlement_for(&report, finished.id, now, next_failures);
                (settlement, note_for(&report.result), report.attachment)
            }
        };

        let journal = JournalAppend {
            kind: finished.kind,
            run_no: finished.run_no,
            recorded_at: now,
            outcome: journal_outcome_for(&settlement),
            note,
            attachment,
        };

        self.store
            .settle(finished.id, &self.identity, settlement, journal)
            .await?;
        Ok(())
    }

    /// Route a successful run's outcome (or its failure) to a settlement.
    ///
    /// A completion is terminal; a pause re-pends with the carry persisted and is
    /// not a failure; a permanent error goes straight to dead. A retryable error
    /// is rescheduled with the Fibonacci backoff and deterministic jitter, unless
    /// it has reached the worker's failure backstop, which forces it to dead.
    fn settlement_for(
        &self,
        report: &RunReport,
        id: Ulid,
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
            Err(_retryable) => self.retry_or_backstop(report, id, now, next_failures),
        }
    }

    /// Schedule a retryable failure with backoff, or force it to dead once it
    /// reaches the failure backstop.
    fn retry_or_backstop(
        &self,
        report: &RunReport,
        id: Ulid,
        now: DateTime<Utc>,
        next_failures: i32,
    ) -> Settlement {
        let attempt = next_failures.max(0) as u32;

        if let Some(max) = self.config.backstop
            && attempt >= max
        {
            return Settlement::Dead {
                finished_at: now,
                failure_count: next_failures,
            };
        }

        let backoff = report.backoff.unwrap_or(self.config.backoff);
        let delay = retry_delay(&backoff, self.config.jitter_fraction, attempt, id);
        Settlement::Retry {
            visible_at: add_duration(now, delay),
            failure_count: next_failures,
            carry: report.carry.clone(),
        }
    }
}

/// The result of one finished handler task, carrying what settlement needs.
struct FinishedRun {
    id: Ulid,
    kind: String,
    run_no: i32,
    failure_count: i32,
    report: Result<RunReport, Error>,
}

/// What the worker remembers about an in-flight job so it can release the job if
/// a forced shutdown has to abort its handler.
struct InflightJob {
    id: Ulid,
    kind: String,
    run_no: i32,
}

/// The journal outcome that corresponds to a settlement transition.
fn journal_outcome_for(settlement: &Settlement) -> JournalOutcome {
    match settlement {
        Settlement::Complete { .. } => JournalOutcome::Completed,
        Settlement::Pause { .. } => JournalOutcome::Paused,
        Settlement::Retry { .. } => JournalOutcome::Retried,
        Settlement::Dead { .. } => JournalOutcome::Dead,
        Settlement::Release { .. } => JournalOutcome::Released,
    }
}

/// The journal note for a run: the outcome's note on success or pause, the error
/// message on failure.
fn note_for(result: &Result<Outcome, crate::outcome::TaskError>) -> Option<String> {
    match result {
        Ok(Outcome::Completed { note }) | Ok(Outcome::Pause { note, .. }) => note.clone(),
        Err(error) => Some(error.message().to_owned()),
    }
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

    // --- Settlement routing (P2) ---

    /// A handler whose behaviour is chosen per run by the shared `Mode`.
    #[derive(Clone)]
    enum Mode {
        AlwaysRetryable,
        AlwaysPermanent,
        PauseThenComplete,
    }

    #[derive(Serialize, Deserialize)]
    struct Routed;

    impl crate::task::Task for Routed {
        const KIND: &'static str = "routed";
        type Carry = u32;
    }

    impl Handler<Mode> for Routed {
        async fn handle(&self, ctx: &mut Context<u32>, mode: &Mode) -> Result<Outcome, TaskError> {
            match mode {
                Mode::AlwaysRetryable => {
                    Err(TaskError::retryable(std::io::Error::other("transient")))
                }
                Mode::AlwaysPermanent => Err(TaskError::permanent("gone for good")),
                Mode::PauseThenComplete => {
                    if *ctx.carry() == 0 {
                        *ctx.carry_mut() = 1;
                        Ok(Outcome::pause_in(Duration::ZERO))
                    } else {
                        Ok(Outcome::completed())
                    }
                }
            }
        }
    }

    async fn run_until<F: FnMut() -> bool>(
        store: &FakeStore,
        mode: Mode,
        backstop: Option<u32>,
        cond: F,
    ) {
        let worker = Worker::builder(mode, Arc::new(store.clone()))
            .register::<Routed>()
            .backstop(backstop)
            .build();
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(worker.run(shutdown.clone()));
        wait_until(cond).await;
        shutdown.cancel();
        handle.await.expect("worker joins");
    }

    #[tokio::test]
    async fn permanent_failure_goes_straight_to_dead() {
        let store = FakeStore::new();
        let queue = Queue::new(Arc::new(store.clone()));
        let id = queue.enqueue(Routed).await.expect("enqueue");

        let store_for_cond = store.clone();
        run_until(&store, Mode::AlwaysPermanent, Some(20), move || {
            store_for_cond.count(crate::store::Status::Dead) == 1
        })
        .await;

        let job = store.job(id).expect("job exists");
        assert_eq!(job.status, crate::store::Status::Dead);
        assert_eq!(job.failure_count, 1);
        assert!(job.finished_at.is_some());
    }

    #[tokio::test]
    async fn retryable_failures_reach_dead_at_the_backstop() {
        let store = FakeStore::new();
        let queue = Queue::new(Arc::new(store.clone()));
        let id = queue.enqueue(Routed).await.expect("enqueue");

        // Backstop of 2: the first failure retries, the second forces dead. The
        // first two attempts have zero backoff so this converges promptly.
        let store_for_cond = store.clone();
        run_until(&store, Mode::AlwaysRetryable, Some(2), move || {
            store_for_cond.count(crate::store::Status::Dead) == 1
        })
        .await;

        let job = store.job(id).expect("job exists");
        assert_eq!(job.status, crate::store::Status::Dead);
        assert_eq!(job.failure_count, 2);
    }

    // --- Graceful shutdown (P5) ---

    /// A handler that checkpoints by pausing as soon as shutdown is signalled.
    #[derive(Serialize, Deserialize)]
    struct Cooperative;

    impl crate::task::Task for Cooperative {
        const KIND: &'static str = "cooperative";
        type Carry = ();
    }

    impl Handler<()> for Cooperative {
        async fn handle(&self, ctx: &mut Context<()>, _state: &()) -> Result<Outcome, TaskError> {
            // Wait for shutdown, then wind down cleanly with a pause.
            ctx.cancelled().await;
            Ok(Outcome::pause_in(Duration::from_secs(1)))
        }
    }

    #[tokio::test]
    async fn cooperative_handler_settles_normally_on_shutdown() {
        let store = FakeStore::new();
        let queue = Queue::new(Arc::new(store.clone()));
        let id = queue.enqueue(Cooperative).await.expect("enqueue");

        let worker = Worker::builder((), Arc::new(store.clone()))
            .register::<Cooperative>()
            .shutdown_timeout(Duration::from_secs(5))
            .build();

        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(worker.run(shutdown.clone()));

        // Let the handler claim and begin awaiting, then ask for shutdown.
        wait_until(|| {
            store
                .job(id)
                .is_some_and(|j| j.status == crate::store::Status::Claimed)
        })
        .await;
        shutdown.cancel();
        handle.await.expect("worker joins");

        // It paused (the cooperative wind-down), so it is pending, not released.
        let job = store.job(id).expect("job");
        assert_eq!(job.status, crate::store::Status::Pending);
        let journal = store
            .journal(id)
            .await
            .expect("journal")
            .into_iter()
            .map(|e| e.outcome)
            .collect::<Vec<_>>();
        assert_eq!(journal, vec![crate::store::JournalOutcome::Paused]);
    }

    #[tokio::test]
    async fn pause_repends_without_failure_and_persists_carry() {
        let store = FakeStore::new();
        let queue = Queue::new(Arc::new(store.clone()));
        let id = queue.enqueue(Routed).await.expect("enqueue");

        let store_for_cond = store.clone();
        run_until(&store, Mode::PauseThenComplete, Some(20), move || {
            store_for_cond.count(crate::store::Status::Completed) == 1
        })
        .await;

        let job = store.job(id).expect("job exists");
        assert_eq!(job.status, crate::store::Status::Completed);
        // Two runs: the pause then the completion. The pause is not a failure.
        assert_eq!(job.run_count, 2);
        assert_eq!(job.failure_count, 0);
        // The carry mutated during the paused run survived to the next run.
        assert_eq!(job.carry, serde_json::json!(1));
    }
}
