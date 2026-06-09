//! The worker: a bounded claim-and-dispatch loop over a [`Store`].
//!
//! A [`Worker`] keeps an in-flight set of at most `N` running handlers and feeds
//! it from the queue. Each iteration fills every free slot by claiming one job at
//! a time, then waits until a handler finishes, new work might be available, or a
//! shutdown is signalled. When a handler finishes, the worker settles its outcome
//! against the store. Horizontal scale comes from running more worker processes,
//! each its own loop.
//!
//! # Clock assumption
//!
//! Two clocks are in play. Claim eligibility and lease expiry are evaluated in
//! database time (the store compares against the database's `now()`), while the
//! scheduling instants the worker writes back — retry/pause `visible_at`,
//! `finished_at`, and journal `recorded_at` — are taken from the worker's
//! `Utc::now()`. The two are assumed reasonably aligned (e.g. NTP-synced hosts).
//! Under worker clock skew, retry and pause schedules shift relative to the
//! lease arithmetic by roughly the skew; lease ownership stays correct because
//! its guard is evaluated entirely in database time.

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

/// The default failure backstop: a high ceiling on retryable failures before a
/// job is forced to dead. It is a failsafe against a task that never recognizes a
/// permanent failure; a task is expected to end itself sooner via
/// `TaskError::permanent`.
const DEFAULT_BACKSTOP: u32 = 20;

/// The worker-level proportional jitter fraction applied to retry delays.
const DEFAULT_JITTER_FRACTION: f64 = 0.5;

/// The default anti-starvation ratio: higher tiers are favoured by roughly this
/// per tier, while lower tiers keep a guaranteed share.
const DEFAULT_PRIORITY_RATIO: u32 = 4;

/// The floor a configured claim lease is clamped to. A near-zero lease expires at
/// or before the claim commits, so `recover_stale` could re-pend a job mid-run
/// (a guaranteed duplicate execution); one second keeps the lease in the future.
const MIN_LEASE: Duration = Duration::from_secs(1);

/// The ceiling a configured claim lease is clamped to. The lease feeds a
/// PostgreSQL `interval '1 second' * lease_secs`, which overflows for absurd
/// values; 365 days stays far below that bound while exceeding any real lease.
const MAX_LEASE: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// The floor a configured poll ceiling is clamped to. A zero `poll_max` makes the
/// idle wait zero, busy-spinning the claim loop; one millisecond keeps it bounded.
const MIN_POLL_MAX: Duration = Duration::from_millis(1);

/// Bound a claim lease to the supported range, shared by the worker-default
/// setter and the per-task `Task::lease` override so both reject the same
/// degenerate values: a near-zero lease expires before the run settles (letting
/// another worker reclaim it mid-run), and an absurd one overflows the backing
/// `interval '1 second' * lease_secs` arithmetic.
fn clamp_lease(lease: Duration) -> Duration {
    lease.clamp(MIN_LEASE, MAX_LEASE)
}

/// The priority floors, by numeric tier, used by the rotation: 0 admits all
/// tiers (high-first), 1 reserves a claim for Normal and Low, 2 for Low only.
const FLOOR_ALL: i16 = 0;
const FLOOR_NORMAL: i16 = 1;
const FLOOR_LOW: i16 = 2;

/// How a handler panic is settled.
///
/// A panic is caught at the task boundary and turned into a failed execution; this
/// chooses which kind. The default, [`PanicPolicy::Retry`], is consistent with how
/// a mid-run process crash is handled (recovered as a failed execution with
/// backoff) and with the retryable-by-default error model, so a transient panic
/// recovers and a deterministic one still reaches dead at the backstop. Under a
/// build that aborts on panic the process ends instead, and either policy falls to
/// lease recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PanicPolicy {
    /// Settle a panic as a retryable failure, scheduled with backoff and bounded
    /// by the backstop. The default.
    Retry,
    /// Settle a panic as a permanent failure: the job moves straight to dead.
    Dead,
}

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
    priority_ratio: Option<u32>,
    panic_policy: PanicPolicy,
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
            priority_ratio: Some(DEFAULT_PRIORITY_RATIO),
            panic_policy: PanicPolicy::Retry,
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
        self.registry.register::<T>(None);
        self
    }

    /// Register a handler type with a per-kind concurrency cap of `max`.
    ///
    /// The worker runs at most `max` jobs of this kind at once, typically because
    /// each holds a slot in a small local resource. The cap is local to this
    /// worker; across several workers the effective limit is `max` times the
    /// number of workers. At-cap kinds are excluded from claiming until one of
    /// their in-flight jobs settles, so their jobs stay pending rather than
    /// claimed-and-idle.
    ///
    /// A `max` of `0` is clamped to `1`; a zero cap would exclude the kind from
    /// claiming entirely.
    #[must_use]
    pub fn register_capped<T>(mut self, max: usize) -> Self
    where
        T: Handler<S>,
    {
        self.registry.register::<T>(Some(max.max(1)));
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
    /// Defaults to 30 seconds. A value below 1ms is clamped up to 1ms so the idle
    /// loop always waits rather than busy-spinning its claim queries.
    #[must_use]
    pub fn poll_max(mut self, d: Duration) -> Self {
        self.config.poll_max = d.max(MIN_POLL_MAX);
        self
    }

    /// The default claim lease. Defaults to 15 minutes; a task may request a
    /// longer one through `Task::lease`.
    ///
    /// Clamped to `[1s, 365d]`: a near-zero lease would expire before the job is
    /// processed and let another worker reclaim it mid-run (duplicate execution),
    /// and an absurdly large one overflows the backing interval arithmetic.
    #[must_use]
    pub fn lease(mut self, d: Duration) -> Self {
        self.config.lease = clamp_lease(d);
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
    /// it through `Task::backoff`. Defaults to a 500ms base and 2-minute cap.
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

    /// The anti-starvation ratio for priority scheduling.
    ///
    /// `Some(r)` favours higher tiers by roughly `r` per tier while guaranteeing
    /// lower tiers a nonzero share, by periodically reserving a claim for them.
    /// `None` is strict priority: higher tiers always win and a sustained stream
    /// of them can starve lower tiers indefinitely. Defaults to `Some(4)`.
    #[must_use]
    pub fn priority_ratio(mut self, ratio: Option<u32>) -> Self {
        self.config.priority_ratio = ratio;
        self
    }

    /// The proportional jitter applied to retry delays, in `[0.0, 1.0]`.
    ///
    /// A retry delay is spread into `[delay * (1 - fraction), delay]`,
    /// deterministically from the job's id, so independently scheduled retries do
    /// not thunder together. `0.0` disables jitter (exact backoff); `1.0` allows
    /// the full range down to near-immediate. Defaults to `0.5`. Values outside the
    /// range are clamped.
    #[must_use]
    pub fn jitter_fraction(mut self, fraction: f64) -> Self {
        self.config.jitter_fraction = fraction.clamp(0.0, 1.0);
        self
    }

    /// How a handler panic is settled. Defaults to [`PanicPolicy::Retry`].
    ///
    /// [`PanicPolicy::Dead`] sends a panicking job straight to dead instead, for
    /// deployments that treat a panic as an unrecoverable programmer error rather
    /// than a transient failure.
    #[must_use]
    pub fn panic_policy(mut self, policy: PanicPolicy) -> Self {
        self.config.panic_policy = policy;
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

        // The dedicated wakeup source. A backend without push notification gives a
        // notifier that never fires, leaving the bounded poll to pick up new work.
        let mut notifier = match self.store.notifier().await {
            Ok(notifier) => notifier,
            Err(error) => {
                tracing::warn!(%error, "could not set up notifications; polling only");
                Box::new(crate::store::NeverNotifier)
            }
        };

        let mut running: JoinSet<FinishedRun> = JoinSet::new();
        // Track the job behind each in-flight task so a forced shutdown can
        // release the stragglers it has to abort.
        let mut inflight: HashMap<tokio::task::Id, InflightJob> = HashMap::new();
        // Per-kind in-flight counts, to honour per-kind concurrency caps.
        let mut by_kind: HashMap<String, usize> = HashMap::new();
        // The anti-starvation claim counter driving the priority-floor rotation.
        let mut claim_counter: u64 = 0;

        'outer: loop {
            // Recover abandoned claims first, returning their work to the pool.
            self.recover_stale().await;

            // Fill every free slot, one claimed row per slot, until the queue has
            // nothing claimable right now or we are shutting down.
            while running.len() < self.config.concurrency {
                if shutdown.is_cancelled() {
                    break 'outer;
                }

                // Exclude kinds at their per-kind cap so their jobs stay pending.
                let claimable = self.claimable_kinds(&kinds, &by_kind);
                if claimable.is_empty() {
                    break;
                }

                claim_counter = claim_counter.wrapping_add(1);
                let floor = floor_for(claim_counter, self.config.priority_ratio);

                match self.claim_with_fallback(&claimable, floor).await {
                    Ok(Some(job)) => {
                        let wait = (Utc::now() - job.created_at)
                            .to_std()
                            .unwrap_or(Duration::ZERO);
                        crate::observability::claimed(&job.kind, wait);
                        self.apply_task_lease(&job).await;
                        let history = self.history_for(job.id).await;
                        *by_kind.entry(job.kind.clone()).or_insert(0) += 1;
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

            // Wait for the soonest of: a handler finishing, a notification, the
            // next future-visible job becoming eligible, or shutdown.
            let wait = self.wait_duration(&kinds).await;
            if running.is_empty() {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = notifier.recv() => {}
                    _ = tokio::time::sleep(wait) => {}
                }
            } else {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    Some(joined) = running.join_next_with_id() => {
                        self.reap(joined, &mut inflight, &mut by_kind).await;
                    }
                    _ = notifier.recv() => {}
                    _ = tokio::time::sleep(wait) => {}
                }
            }
        }

        self.drain(running, inflight, by_kind).await;
    }

    /// The registered kinds whose in-flight count is below their per-kind cap.
    /// Uncapped kinds are always included.
    fn claimable_kinds(&self, kinds: &[String], by_kind: &HashMap<String, usize>) -> Vec<String> {
        kinds
            .iter()
            .filter(|kind| match self.registry.cap(kind) {
                Some(cap) => by_kind.get(*kind).copied().unwrap_or(0) < cap,
                None => true,
            })
            .cloned()
            .collect()
    }

    /// Claim at `floor`, falling back to an unconstrained claim if a reserved
    /// lower-tier claim finds nothing, so a reserved slot is never wasted.
    async fn claim_with_fallback(
        &self,
        kinds: &[String],
        floor: i16,
    ) -> Result<Option<crate::store::JobRecord>, Error> {
        let claimed = self
            .store
            .claim_next(kinds, floor, self.config.lease, &self.identity)
            .await?;
        if claimed.is_some() || floor == FLOOR_ALL {
            return Ok(claimed);
        }
        self.store
            .claim_next(kinds, FLOOR_ALL, self.config.lease, &self.identity)
            .await
    }

    /// How long to sleep when the loop cannot make progress by claiming: the
    /// soonest future eligibility among the worker's kinds, bounded by `poll_max`.
    async fn wait_duration(&self, kinds: &[String]) -> Duration {
        let until_next = match self.store.next_visible_at(kinds).await {
            Ok(Some(at)) => (at - Utc::now())
                .to_std()
                .unwrap_or(Duration::ZERO)
                .max(Duration::from_millis(5)),
            Ok(None) => self.config.poll_max,
            Err(error) => {
                tracing::warn!(%error, "could not compute next eligibility; using poll_max");
                self.config.poll_max
            }
        };
        until_next.min(self.config.poll_max)
    }

    /// Drain on shutdown: the cancel signal is already raised, so give handlers
    /// `shutdown_timeout` to wind down on their own (typically a `Pause`,
    /// settled normally); at the deadline, force-abort whatever remains and
    /// release it so another worker can pick it up immediately.
    async fn drain(
        &self,
        mut running: JoinSet<FinishedRun>,
        mut inflight: HashMap<tokio::task::Id, InflightJob>,
        mut by_kind: HashMap<String, usize>,
    ) {
        crate::observability::shutdown_drain(running.len());
        let deadline = tokio::time::sleep(self.config.shutdown_timeout);
        tokio::pin!(deadline);

        while !running.is_empty() {
            tokio::select! {
                joined = running.join_next_with_id() => match joined {
                    Some(result) => self.reap(result, &mut inflight, &mut by_kind).await,
                    None => break,
                },
                _ = &mut deadline => {
                    // Cooperative wind-down ran out of time; stop waiting.
                    break;
                }
            }
        }

        self.force_finish(running, inflight).await;
    }

    /// Abort whatever handlers are still running past the drain deadline and
    /// finish the bookkeeping for everything left in the [`JoinSet`].
    ///
    /// A handler that finished between the deadline and the abort still produced
    /// a settlement: `abort_all` is a no-op for it, and its task yields its
    /// `FinishedRun`, so it must be settled, not released. Only handlers that were
    /// genuinely aborted (their task yields a cancellation error) are released
    /// back to pending for another worker to retry.
    async fn force_finish(
        &self,
        mut running: JoinSet<FinishedRun>,
        mut inflight: HashMap<tokio::task::Id, InflightJob>,
    ) {
        running.abort_all();
        while let Some(joined) = running.join_next_with_id().await {
            // A finished handler yields `Ok` and must be settled; an aborted one
            // yields a cancellation error and is left in `inflight` for the
            // release loop below to return to pending.
            if let Ok((task_id, finished)) = joined {
                inflight.remove(&task_id);
                if let Err(error) = self.settle(finished).await {
                    tracing::warn!(%error, "settle failed during shutdown drain");
                }
            }
        }
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
            let kind = job.kind.clone();
            match self
                .store
                .recover(
                    job.id,
                    add_duration(now, delay),
                    next_failures,
                    job.run_count,
                    journal,
                )
                .await
            {
                Ok(true) => crate::observability::recovered(&kind),
                Ok(false) => {}
                Err(error) => tracing::warn!(%error, "stale-claim recovery failed"),
            }
        }
    }

    /// Apply a per-task lease override after the claim, which only stamps the
    /// worker default. A no-op when the task uses the default.
    ///
    /// The override is clamped to the same bounds as the worker default, so a task
    /// returning a degenerate `Task::lease` cannot stamp an already-expired or
    /// interval-overflowing lease.
    async fn apply_task_lease(&self, job: &crate::store::JobRecord) {
        let Some(lease) = self.registry.lease_for(&job.kind, &job.payload) else {
            return;
        };
        let lease = clamp_lease(lease);
        if lease == self.config.lease {
            return;
        }
        if let Err(error) = self
            .store
            .extend_lease(job.id, &self.identity, job.run_count, lease)
            .await
        {
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
            run_count: job.run_count,
            history,
            state: Arc::clone(&self.state),
            cancel: shutdown.clone(),
            panic_policy: self.config.panic_policy,
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

    /// Handle one joined task: drop its in-flight tracking (including its per-kind
    /// count) and settle it, or log a lost panic.
    async fn reap(
        &self,
        joined: Result<(tokio::task::Id, FinishedRun), tokio::task::JoinError>,
        inflight: &mut HashMap<tokio::task::Id, InflightJob>,
        by_kind: &mut HashMap<String, usize>,
    ) {
        match joined {
            Ok((task_id, finished)) => {
                if let Some(job) = inflight.remove(&task_id) {
                    release_slot(by_kind, &job.kind);
                }
                if let Err(error) = self.settle(finished).await {
                    tracing::warn!(%error, "settle failed");
                }
            }
            Err(join_error) => {
                // An aborted handler (force-aborted at shutdown) does not settle
                // itself; shutdown release or lease expiry returns its job to the
                // pool. Handler panics do not reach here: they are caught at the
                // task boundary and settled as a failed execution.
                if let Some(job) = inflight.remove(&join_error.id()) {
                    release_slot(by_kind, &job.kind);
                }
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
            .settle(job.id, &self.identity, job.run_no, settlement, journal)
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

        let (settlement, note, attachment, duration) = match finished.report {
            // The payload or carry could not be decoded: the job is unrunnable, so
            // give up on it rather than spin.
            Err(error) => {
                tracing::error!(%error, "job could not be dispatched; marking dead");
                let settlement = Settlement::Dead {
                    finished_at: now,
                    failure_count: next_failures,
                };
                (settlement, Some(error.to_string()), None, Duration::ZERO)
            }
            Ok(report) => {
                let settlement = self.settlement_for(&report, finished.id, now, next_failures);
                (
                    settlement,
                    note_for(&report.result),
                    report.attachment,
                    report.duration,
                )
            }
        };

        let outcome = journal_outcome_for(&settlement);
        crate::observability::settled(&finished.kind, outcome, duration);

        let journal = JournalAppend {
            kind: finished.kind,
            run_no: finished.run_no,
            recorded_at: now,
            outcome,
            note,
            attachment,
        };

        self.store
            .settle(
                finished.id,
                &self.identity,
                finished.run_no,
                settlement,
                journal,
            )
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
    run_no: u32,
    failure_count: i32,
    report: Result<RunReport, Error>,
}

/// What the worker remembers about an in-flight job so it can release the job if
/// a forced shutdown has to abort its handler.
struct InflightJob {
    id: Ulid,
    kind: String,
    run_no: u32,
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

/// The priority floor for a claim, given the rotation counter and ratio.
///
/// With ratio `r`, most claims are unconstrained (high-first); every `r`-th claim
/// reserves a slot for Normal and Low by excluding High, and every `r²`-th claim
/// reserves one for Low only. `None`, or a ratio below 2, is strict priority: no
/// reservation, so a sustained stream of high-priority work can starve lower tiers.
fn floor_for(counter: u64, ratio: Option<u32>) -> i16 {
    match ratio {
        Some(r) if r >= 2 => {
            let r = u64::from(r);
            if counter.is_multiple_of(r * r) {
                FLOOR_LOW
            } else if counter.is_multiple_of(r) {
                FLOOR_NORMAL
            } else {
                FLOOR_ALL
            }
        }
        _ => FLOOR_ALL,
    }
}

/// Decrement a kind's in-flight count, removing the entry when it reaches zero.
fn release_slot(by_kind: &mut HashMap<String, usize>, kind: &str) {
    if let Some(count) = by_kind.get_mut(kind) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            by_kind.remove(kind);
        }
    }
}

/// Add a `std::time::Duration` to a UTC instant, saturating to the far future on
/// overflow.
///
/// An overflowing delay means "schedule effectively forever from now" (a very
/// long `pause_in` or `resume_in`), so it saturates to [`DateTime::<Utc>::MAX_UTC`]
/// rather than collapsing to `now`. Collapsing to `now` would make the job
/// immediately eligible again and spin a tight claim/pause cycle.
fn add_duration(now: DateTime<Utc>, delta: Duration) -> DateTime<Utc> {
    match chrono::Duration::from_std(delta) {
        Ok(delta) => now
            .checked_add_signed(delta)
            .unwrap_or(DateTime::<Utc>::MAX_UTC),
        Err(_) => DateTime::<Utc>::MAX_UTC,
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

    #[test]
    fn clamp_lease_floors_and_caps_degenerate_values() {
        assert_eq!(clamp_lease(Duration::ZERO), MIN_LEASE);
        assert_eq!(clamp_lease(Duration::from_secs(u64::MAX)), MAX_LEASE);
        // A value already in range passes through untouched.
        let mid = Duration::from_secs(60);
        assert_eq!(clamp_lease(mid), mid);
    }

    #[test]
    fn builder_clamps_degenerate_lease_and_poll_max() {
        let store = Arc::new(FakeStore::new());

        let clamped = Worker::builder((), store.clone())
            .lease(Duration::ZERO)
            .poll_max(Duration::ZERO);
        assert_eq!(clamped.config.lease, MIN_LEASE);
        assert_eq!(clamped.config.poll_max, MIN_POLL_MAX);

        let capped = Worker::builder((), store).lease(Duration::from_secs(u64::MAX));
        assert_eq!(capped.config.lease, MAX_LEASE);
    }

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
    async fn shutdown_settles_a_handler_that_finished_before_the_abort() {
        let store = FakeStore::new();
        let queue = Queue::new(Arc::new(store.clone()));
        let id = queue.enqueue(SlowJob).await.expect("enqueue");

        let worker = Worker::builder(Counters::new(), Arc::new(store.clone()))
            .register::<SlowJob>()
            .build();

        // Claim the job as this worker so the store holds it `claimed` by us; a
        // release would re-pend it, a settle would complete it.
        let claimed = store
            .claim_next(
                &["slow".to_owned()],
                FLOOR_ALL,
                worker.config.lease,
                &worker.identity,
            )
            .await
            .expect("claim succeeds")
            .expect("a claimable job");
        assert_eq!(claimed.id, id);

        // Spawn the real handler, then let it run to completion so a finished
        // task sits unreaped in the JoinSet: the exact state the forced drain
        // faces when the deadline fires at the instant a handler returns.
        let shutdown = CancellationToken::new();
        let mut running = JoinSet::new();
        let run_no = claimed.run_count;
        let kind = claimed.kind.clone();
        let task_id = worker.spawn(&mut running, claimed, Vec::new(), &shutdown);
        let mut inflight = HashMap::new();
        inflight.insert(task_id, InflightJob { id, kind, run_no });
        tokio::time::sleep(Duration::from_millis(120)).await;

        worker.force_finish(running, inflight).await;

        assert_eq!(
            store.count(crate::store::Status::Completed),
            1,
            "a handler that finished before the abort must be settled, not re-pended"
        );
        assert_eq!(
            store.count(crate::store::Status::Pending),
            0,
            "the completed job must not be released back to pending"
        );
    }

    #[test]
    fn add_duration_saturates_to_the_far_future_on_overflow() {
        let now = Utc::now();
        // A delay too large for chrono represents "park effectively forever". It
        // must land far in the future, not collapse to `now`: collapsing to `now`
        // would make a long `pause_in` immediately eligible again and spin the
        // claim/pause cycle.
        let parked = add_duration(now, Duration::from_secs(u64::MAX));
        assert!(
            parked > now + chrono::Duration::days(365_000),
            "an overflowing delay must saturate to the far future, got {parked}"
        );

        // A `checked_add_signed` overflow (a representable std duration that still
        // pushes past chrono's range) saturates the same way.
        let near_max = DateTime::<Utc>::MAX_UTC - chrono::Duration::days(1);
        let pushed = add_duration(near_max, Duration::from_secs(7 * 24 * 60 * 60));
        assert!(
            pushed >= near_max,
            "an add that overflows the representable range must not move backward"
        );
    }

    #[tokio::test]
    async fn settle_rejects_a_stale_run_after_same_identity_reclaim() {
        use crate::store::{JournalAppend, JournalOutcome, Settlement, Status, Store};

        let store = FakeStore::new();
        let queue = Queue::new(Arc::new(store.clone()));
        let id = queue.enqueue(SlowJob).await.expect("enqueue");
        let kinds = vec!["slow".to_owned()];

        let make_journal = |run_no: u32, outcome: JournalOutcome| JournalAppend {
            kind: "slow".to_owned(),
            run_no,
            recorded_at: Utc::now(),
            outcome,
            note: None,
            attachment: None,
        };

        // First claim, under one identity, with a short lease that will expire.
        let first = store
            .claim_next(&kinds, FLOOR_ALL, Duration::from_millis(40), "worker-a")
            .await
            .expect("claim")
            .expect("a job");
        assert_eq!(first.run_count, 1);

        // The lease expires and the SAME worker identity recovers and reclaims it,
        // exactly as a single worker's own `recover_stale` would.
        tokio::time::sleep(Duration::from_millis(70)).await;
        let recovered = store
            .recover(
                id,
                Utc::now(),
                1,
                first.run_count,
                make_journal(1, JournalOutcome::StaleRecovered),
            )
            .await
            .expect("recover");
        assert!(recovered);
        let second = store
            .claim_next(&kinds, FLOOR_ALL, Duration::from_secs(60), "worker-a")
            .await
            .expect("claim")
            .expect("a job");
        assert_eq!(second.run_count, 2);

        // The first run's late completion must be rejected: the identity matches,
        // but its claim epoch is stale. Without the epoch in the guard this would
        // overwrite the live second claim with the first run's outcome.
        let stale = store
            .settle(
                id,
                "worker-a",
                first.run_count,
                Settlement::Complete {
                    finished_at: Utc::now(),
                },
                make_journal(1, JournalOutcome::Completed),
            )
            .await
            .expect("settle stale");
        assert!(
            !stale,
            "a stale run must not settle under a matching identity"
        );
        assert_eq!(store.job(id).expect("job").status, Status::Claimed);

        // The live second claim settles normally.
        let current = store
            .settle(
                id,
                "worker-a",
                second.run_count,
                Settlement::Complete {
                    finished_at: Utc::now(),
                },
                make_journal(2, JournalOutcome::Completed),
            )
            .await
            .expect("settle current");
        assert!(current, "the live claim settles");
        assert_eq!(store.count(Status::Completed), 1);
    }

    #[tokio::test]
    async fn recover_rejects_a_stale_snapshot_after_the_claim_advanced() {
        use crate::store::{JournalAppend, JournalOutcome, Status, Store};

        let store = FakeStore::new();
        let queue = Queue::new(Arc::new(store.clone()));
        let id = queue.enqueue(SlowJob).await.expect("enqueue");
        let kinds = vec!["slow".to_owned()];

        let make_journal = |run_no: u32| JournalAppend {
            kind: "slow".to_owned(),
            run_no,
            recorded_at: Utc::now(),
            outcome: JournalOutcome::StaleRecovered,
            note: None,
            attachment: None,
        };

        // Claim under a short lease and take the stale snapshot (epoch 1).
        let snapshot = store
            .claim_next(&kinds, FLOOR_ALL, Duration::from_millis(40), "worker-a")
            .await
            .expect("claim")
            .expect("a job");
        assert_eq!(snapshot.run_count, 1);
        tokio::time::sleep(Duration::from_millis(70)).await;

        // Another recovery path re-pends the job (epoch still 1), then it is
        // reclaimed (epoch 2) and that lease also expires.
        assert!(
            store
                .recover(id, Utc::now(), 1, snapshot.run_count, make_journal(1))
                .await
                .expect("first recover")
        );
        let reclaim = store
            .claim_next(&kinds, FLOOR_ALL, Duration::from_millis(40), "worker-a")
            .await
            .expect("reclaim")
            .expect("a job");
        assert_eq!(reclaim.run_count, 2);
        tokio::time::sleep(Duration::from_millis(70)).await;

        // The original snapshot's recover (epoch 1) arrives late. It must be
        // rejected: the claim has advanced to epoch 2, and applying the stale
        // recover would regress the failure count and journal a stale run.
        let stale_applied = store
            .recover(id, Utc::now(), 1, snapshot.run_count, make_journal(1))
            .await
            .expect("stale recover");
        assert!(
            !stale_applied,
            "a recovery from a superseded snapshot epoch must not apply"
        );

        // The live epoch-2 claim still recovers normally.
        let live_applied = store
            .recover(id, Utc::now(), 2, reclaim.run_count, make_journal(2))
            .await
            .expect("live recover");
        assert!(live_applied, "the current claim epoch recovers");
        assert_eq!(store.job(id).expect("job").status, Status::Pending);
    }

    /// A handler that completes successfully but leaves a carry that cannot be
    /// serialized to JSON: a map with non-string (tuple) keys, which
    /// `serde_json::to_value` rejects at runtime.
    #[derive(Serialize, Deserialize)]
    struct Unencodable;

    impl crate::task::Task for Unencodable {
        const KIND: &'static str = "unencodable";
        type Carry = std::collections::BTreeMap<(i32, i32), i32>;
    }

    impl Handler<()> for Unencodable {
        async fn handle(
            &self,
            ctx: &mut Context<std::collections::BTreeMap<(i32, i32), i32>>,
            _state: &(),
        ) -> Result<Outcome, TaskError> {
            ctx.carry_mut().insert((1, 2), 3);
            Ok(Outcome::completed())
        }
    }

    #[tokio::test]
    async fn completed_run_with_unencodable_carry_is_dead_with_an_accurate_note() {
        let store = FakeStore::new();
        let queue = Queue::new(Arc::new(store.clone()));
        let id = queue.enqueue(Unencodable).await.expect("enqueue");

        let worker = Worker::builder((), Arc::new(store.clone()))
            .register::<Unencodable>()
            .build();
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(worker.run(shutdown.clone()));
        wait_until(|| store.count(crate::store::Status::Dead) == 1).await;
        shutdown.cancel();
        handle.await.expect("worker joins");

        // The run completed, so the job is dead (the carry could not be persisted
        // and re-running would only fail to serialize again) — but the journal
        // note must describe that, not misreport the run as undispatchable.
        let job = store.job(id).expect("job exists");
        assert_eq!(job.status, crate::store::Status::Dead);
        let note = store
            .journal(id)
            .await
            .expect("journal")
            .last()
            .expect("a journal entry")
            .note
            .clone()
            .unwrap_or_default();
        assert!(
            note.contains("carry could not be serialized"),
            "the note must describe the post-run carry serialization failure, got: {note:?}"
        );
    }

    #[tokio::test]
    async fn worker_with_no_kinds_returns_immediately() {
        let store = FakeStore::new();
        let worker: Worker<Counters> = Worker::builder(Counters::new(), Arc::new(store)).build();
        // Should return without needing a shutdown signal.
        worker.run(CancellationToken::new()).await;
    }

    proptest::proptest! {
        /// Over any window of `r²` consecutive claims, the rotation reserves at
        /// least one slot for Low and still admits the unconstrained (high-first)
        /// claim, so no tier is starved.
        #[test]
        fn rotation_serves_every_tier_in_a_window(r in 2u32..8, start in 0u64..10_000) {
            let window = u64::from(r) * u64::from(r);
            let floors: Vec<i16> = (start..start + window)
                .map(|c| floor_for(c, Some(r)))
                .collect();
            proptest::prop_assert!(floors.contains(&FLOOR_LOW), "Low never reserved");
            proptest::prop_assert!(floors.contains(&FLOOR_ALL), "high-first never admitted");
        }
    }

    #[test]
    fn floor_rotation_reserves_lower_tiers() {
        // Ratio 2: every 2nd claim reserves Normal/Low, every 4th reserves Low.
        assert_eq!(floor_for(1, Some(2)), FLOOR_ALL);
        assert_eq!(floor_for(2, Some(2)), FLOOR_NORMAL);
        assert_eq!(floor_for(3, Some(2)), FLOOR_ALL);
        assert_eq!(floor_for(4, Some(2)), FLOOR_LOW);
        // Strict priority never reserves.
        assert_eq!(floor_for(2, None), FLOOR_ALL);
        assert_eq!(floor_for(4, None), FLOOR_ALL);
        assert_eq!(floor_for(4, Some(1)), FLOOR_ALL);
    }

    // --- Per-kind concurrency caps (P6) ---

    #[derive(Clone)]
    struct CapState {
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        ran: Arc<AtomicUsize>,
    }

    #[derive(Serialize, Deserialize)]
    struct Capped;

    impl crate::task::Task for Capped {
        const KIND: &'static str = "capped";
        type Carry = ();
    }

    impl Handler<CapState> for Capped {
        async fn handle(
            &self,
            _ctx: &mut Context<()>,
            state: &CapState,
        ) -> Result<Outcome, TaskError> {
            let now = state.active.fetch_add(1, Ordering::SeqCst) + 1;
            state.peak.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(40)).await;
            state.active.fetch_sub(1, Ordering::SeqCst);
            state.ran.fetch_add(1, Ordering::SeqCst);
            Ok(Outcome::completed())
        }
    }

    #[tokio::test]
    async fn per_kind_cap_bounds_in_flight_below_concurrency() {
        let store = FakeStore::new();
        let queue = Queue::new(Arc::new(store.clone()));
        for _ in 0..8 {
            queue.enqueue(Capped).await.expect("enqueue");
        }

        let state = CapState {
            active: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
            ran: Arc::new(AtomicUsize::new(0)),
        };
        // Concurrency 8, but the kind is capped at 2.
        let worker = Worker::builder(state.clone(), Arc::new(store.clone()))
            .concurrency(8)
            .register_capped::<Capped>(2)
            .build();

        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(worker.run(shutdown.clone()));
        wait_until(|| state.ran.load(Ordering::SeqCst) == 8).await;
        shutdown.cancel();
        handle.await.expect("worker joins");

        assert!(
            state.peak.load(Ordering::SeqCst) <= 2,
            "peak in-flight {} exceeded the cap of 2",
            state.peak.load(Ordering::SeqCst)
        );
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

    // --- Handler panics (immediate failed-execution settlement) ---

    /// A handler that panics on its first run and completes on the next, driven by
    /// a shared run counter.
    #[derive(Serialize, Deserialize)]
    struct Panicky;

    impl crate::task::Task for Panicky {
        const KIND: &'static str = "panicky";
        type Carry = ();
    }

    impl Handler<Arc<AtomicUsize>> for Panicky {
        async fn handle(
            &self,
            _ctx: &mut Context<()>,
            state: &Arc<AtomicUsize>,
        ) -> Result<Outcome, TaskError> {
            if state.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("boom in handler");
            }
            Ok(Outcome::completed())
        }
    }

    #[tokio::test]
    async fn panicking_handler_settles_as_a_failed_execution_then_recovers() {
        let store = FakeStore::new();
        let queue = Queue::new(Arc::new(store.clone()));
        let id = queue.enqueue(Panicky).await.expect("enqueue");

        let runs = Arc::new(AtomicUsize::new(0));
        let worker = Worker::builder(runs.clone(), Arc::new(store.clone()))
            .register::<Panicky>()
            .build();
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(worker.run(shutdown.clone()));

        // The panicking run must settle promptly as a retry (not stay claimed for
        // the lease); the retry then completes. If the panic were left to lease
        // recovery, this would never reach Completed within the deadline.
        wait_until(|| store.count(crate::store::Status::Completed) == 1).await;
        shutdown.cancel();
        handle.await.expect("worker joins");

        let job = store.job(id).expect("job exists");
        assert_eq!(job.status, crate::store::Status::Completed);
        // The panic counts as exactly one failed execution.
        assert_eq!(job.failure_count, 1);
        // The journal records the panic as a retry, then the completion.
        let outcomes = store
            .journal(id)
            .await
            .expect("journal")
            .into_iter()
            .map(|e| e.outcome)
            .collect::<Vec<_>>();
        assert_eq!(
            outcomes,
            vec![
                crate::store::JournalOutcome::Retried,
                crate::store::JournalOutcome::Completed,
            ]
        );
    }

    #[tokio::test]
    async fn panic_policy_dead_sends_a_panicking_job_straight_to_dead() {
        let store = FakeStore::new();
        let queue = Queue::new(Arc::new(store.clone()));
        let id = queue.enqueue(Panicky).await.expect("enqueue");

        let runs = Arc::new(AtomicUsize::new(0));
        let worker = Worker::builder(runs.clone(), Arc::new(store.clone()))
            .register::<Panicky>()
            .panic_policy(PanicPolicy::Dead)
            .build();
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(worker.run(shutdown.clone()));

        wait_until(|| store.count(crate::store::Status::Dead) == 1).await;
        shutdown.cancel();
        handle.await.expect("worker joins");

        let job = store.job(id).expect("job exists");
        assert_eq!(job.status, crate::store::Status::Dead);
        assert_eq!(job.failure_count, 1);
        let outcomes = store
            .journal(id)
            .await
            .expect("journal")
            .into_iter()
            .map(|e| e.outcome)
            .collect::<Vec<_>>();
        assert_eq!(outcomes, vec![crate::store::JournalOutcome::Dead]);
    }

    // --- Jitter fraction knob (P-worker-defaults) ---

    #[tokio::test]
    async fn jitter_fraction_zero_schedules_the_exact_backoff_delay() {
        let store = FakeStore::new();
        // A fixed id makes the deterministic jitter reproducible.
        let id = Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("valid ULID");
        let now = Utc::now();
        let job = crate::store::NewJob {
            id,
            kind: "routed".to_owned(),
            payload: serde_json::Value::Null,
            priority: 1,
            created_at: now,
            visible_at: now,
            carry: serde_json::json!(0),
            dedup_key: None,
        };
        store.enqueue(&job).await.expect("enqueue");

        // A 100s base makes the third attempt's delay (base * (fib(3) - 1) = base)
        // large and non-zero, so any jitter would be plainly visible.
        let backoff = Backoff::new(Duration::from_secs(100), Duration::from_secs(300));
        let worker = Worker::builder(Mode::AlwaysRetryable, Arc::new(store.clone()))
            .register::<Routed>()
            .backoff(backoff)
            .jitter_fraction(0.0)
            .backstop(Some(50))
            .build();
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(worker.run(shutdown.clone()));

        // Attempts 1 and 2 are immediate; the third schedules the 100s delay and
        // then the job sits ineligible, so the failure count settles at 3.
        wait_until(|| store.job(id).is_some_and(|j| j.failure_count >= 3)).await;
        shutdown.cancel();
        handle.await.expect("worker joins");

        let job = store.job(id).expect("job exists");
        let entries = store.journal(id).await.expect("journal");
        let third = entries.last().expect("a third journal entry");
        // settle stamps the journal `recorded_at` and the retry `visible_at` from
        // one clock read, so their difference is exactly the scheduled delay.
        let scheduled = (job.visible_at - third.recorded_at)
            .to_std()
            .expect("non-negative delay");
        let expected = crate::backoff::retry_delay(&backoff, 0.0, 3, id);
        assert_eq!(scheduled, expected);
        // Jitter disabled means the full base curve, not a reduced delay.
        assert_eq!(expected, Duration::from_secs(100));
    }
}
