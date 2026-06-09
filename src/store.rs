//! The storage backend contract (ADR 8) and its type-erased value types.
//!
//! Everything above this layer (the queue handle, the worker loop, the registry)
//! depends only on [`Store`], never on a concrete driver. The default adapter
//! lives in [`crate::postgres`]. Payloads cross a JSON boundary here: the store
//! only ever sees a `kind` string and opaque [`serde_json::Value`]s for the
//! payload and carry. Type safety is recovered above this layer at enqueue and at
//! dispatch.
//!
//! ## Growing surface
//!
//! The trait grows phase by phase as the queue gains capabilities; each operation
//! is introduced alongside its adapter implementation and tests. The value types
//! below are the stable vocabulary those operations speak.

use crate::error::Error;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use std::time::Duration;
use ulid::Ulid;

// =============================================================================
// Lifecycle status
// =============================================================================

/// The lifecycle state of a job (ADR 5).
///
/// A job moves `Pending -> Claimed -> (Completed | Dead)`; a claimed job that
/// settles as a retry or a pause returns to `Pending`. The four states are
/// pinned by a `CHECK` constraint in the schema, and this enum is their in-memory
/// mirror.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Status {
    /// Eligible to be claimed once `visible_at` has passed.
    Pending,
    /// Held by a worker under a lease until it settles or the lease expires.
    Claimed,
    /// Terminal success.
    Completed,
    /// Terminal give-up.
    Dead,
}

impl Status {
    /// The textual form stored in the `status` column.
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Pending => "pending",
            Status::Claimed => "claimed",
            Status::Completed => "completed",
            Status::Dead => "dead",
        }
    }

    /// Parse the textual form read from the `status` column.
    ///
    /// Returns `None` for any value the schema's `CHECK` constraint would have
    /// rejected, so a `Some` result is a state this code understands.
    pub fn from_db(value: &str) -> Option<Status> {
        match value {
            "pending" => Some(Status::Pending),
            "claimed" => Some(Status::Claimed),
            "completed" => Some(Status::Completed),
            "dead" => Some(Status::Dead),
            _ => None,
        }
    }
}

// =============================================================================
// Journal outcomes
// =============================================================================

/// The outcome recorded by one append-only journal entry (ADR 16).
///
/// Every execution and every applied merge appends exactly one entry. The set is
/// pinned by a `CHECK` constraint in the schema. A task's failure history is the
/// subset of its journal whose outcome is a failure (`Retried`, `Dead`, or
/// `StaleRecovered`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JournalOutcome {
    /// The run completed the job.
    Completed,
    /// The run paused the job cooperatively; not a failure.
    Paused,
    /// The run failed and the job was rescheduled with backoff.
    Retried,
    /// The job gave up and moved to dead.
    Dead,
    /// A lease expired and the claim was reclaimed; counts as a failure.
    StaleRecovered,
    /// A graceful shutdown released the job; not a failure.
    Released,
    /// An enqueue merged into this pending job (ADR 10).
    Merged,
}

impl JournalOutcome {
    /// The textual form stored in the `outcome` column.
    pub fn as_str(self) -> &'static str {
        match self {
            JournalOutcome::Completed => "completed",
            JournalOutcome::Paused => "paused",
            JournalOutcome::Retried => "retried",
            JournalOutcome::Dead => "dead",
            JournalOutcome::StaleRecovered => "stale-recovered",
            JournalOutcome::Released => "released",
            JournalOutcome::Merged => "merged",
        }
    }

    /// Parse the textual form read from the `outcome` column.
    pub fn from_db(value: &str) -> Option<JournalOutcome> {
        match value {
            "completed" => Some(JournalOutcome::Completed),
            "paused" => Some(JournalOutcome::Paused),
            "retried" => Some(JournalOutcome::Retried),
            "dead" => Some(JournalOutcome::Dead),
            "stale-recovered" => Some(JournalOutcome::StaleRecovered),
            "released" => Some(JournalOutcome::Released),
            "merged" => Some(JournalOutcome::Merged),
            _ => None,
        }
    }

    /// Whether this outcome counts as a failed execution toward the backstop.
    ///
    /// A cooperative pause and a clean release are not failures; a retry, a death,
    /// and a stale recovery are. A merge is a bookkeeping event, not an execution.
    pub fn is_failure(self) -> bool {
        matches!(
            self,
            JournalOutcome::Retried | JournalOutcome::Dead | JournalOutcome::StaleRecovered
        )
    }
}

// =============================================================================
// Records crossing the storage boundary
// =============================================================================

/// A type-erased job row as the storage layer sees it.
///
/// The `payload` and `carry` are opaque JSON; the layer above turns them back
/// into concrete task types using `kind` and the registry.
#[derive(Debug, Clone)]
pub struct JobRecord {
    /// The job's ULID identity.
    pub id: Ulid,
    /// The task discriminator that routes the job to its handler.
    pub kind: String,
    /// The serialized task payload.
    pub payload: serde_json::Value,
    /// Numeric priority tier: 0 high, 1 normal, 2 low.
    pub priority: i16,
    /// Current lifecycle state.
    pub status: Status,
    /// When the job was first enqueued.
    pub created_at: DateTime<Utc>,
    /// When the job next becomes eligible to claim.
    pub visible_at: DateTime<Utc>,
    /// Lease expiry while claimed; `None` otherwise.
    pub claim_expires_at: Option<DateTime<Utc>>,
    /// Claiming worker identity (`host:pid`) while claimed; `None` otherwise.
    pub claimed_by: Option<String>,
    /// When the job reached a terminal state; `None` until then.
    pub finished_at: Option<DateTime<Utc>>,
    /// Number of times the job has been claimed/executed.
    pub run_count: u32,
    /// Number of failed executions counted toward the backstop.
    pub failure_count: i32,
    /// The typed carried state, serialized; JSON `null` when empty.
    pub carry: serde_json::Value,
    /// Deduplication candidacy key; `None` means never coalesced.
    pub dedup_key: Option<String>,
}

/// One append-only journal entry for a job (ADR 16).
#[derive(Debug, Clone)]
pub struct JournalRecord {
    /// Surrogate identity of the entry.
    pub id: i64,
    /// The job this entry belongs to.
    pub job_id: Ulid,
    /// The job's kind, denormalized so the journal is queryable without a join.
    pub kind: String,
    /// The run number this entry records.
    pub run_no: u32,
    /// When the entry was written.
    pub recorded_at: DateTime<Utc>,
    /// The recorded outcome.
    pub outcome: JournalOutcome,
    /// The run's conclusion, or on failure the error message.
    pub note: Option<String>,
    /// Structured evidence set during the run.
    pub attachment: Option<serde_json::Value>,
}

/// Parameters for inserting a brand-new pending job.
///
/// This is the producer's enqueue request in type-erased form; the queue handle
/// builds it from a typed task.
#[derive(Debug, Clone)]
pub struct NewJob {
    /// The job's ULID identity (generated by the producer).
    pub id: Ulid,
    /// The task discriminator.
    pub kind: String,
    /// The serialized task payload.
    pub payload: serde_json::Value,
    /// Numeric priority tier.
    pub priority: i16,
    /// Enqueue time.
    pub created_at: DateTime<Utc>,
    /// Eligibility time; equal to `created_at` for immediate work.
    pub visible_at: DateTime<Utc>,
    /// Initial carried state, serialized.
    pub carry: serde_json::Value,
    /// Deduplication candidacy key; `None` to never coalesce.
    pub dedup_key: Option<String>,
}

impl NewJob {
    /// Reject a job whose priority is outside the supported tier range before it
    /// reaches storage. Adapters call this at the start of `enqueue` so a direct
    /// `Store` user who hand-builds a `NewJob` gets a typed [`Error`] rather than
    /// a backend-specific constraint violation (the PostgreSQL schema's `CHECK`).
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if crate::task::Priority::from_smallint(self.priority).is_none() {
            return Err(Error::InvalidPriority {
                priority: self.priority,
            });
        }
        Ok(())
    }
}

/// A journal entry to append as part of settling a job.
///
/// Exactly one entry is written per execution (and, later, per applied merge), in
/// the same transaction as the job-row transition, so the journal is a hole-free
/// per-job event log.
#[derive(Debug, Clone)]
pub struct JournalAppend {
    /// The job's kind, denormalized so the journal is queryable without a join.
    pub kind: String,
    /// The run number this entry records.
    pub run_no: u32,
    /// When the entry is written.
    pub recorded_at: DateTime<Utc>,
    /// The recorded outcome.
    pub outcome: JournalOutcome,
    /// The run's conclusion, or on failure the error message.
    pub note: Option<String>,
    /// Structured evidence gathered during the run.
    pub attachment: Option<serde_json::Value>,
}

/// A guarded settlement of a claimed job: the lifecycle transition to apply.
///
/// Every settlement is applied only if the worker still holds the claim (see
/// [`Store::settle`]), so a slow or aborted handler cannot settle a job another
/// worker has reclaimed. Journaling is layered on in a later phase; this enum is
/// the job-row transition itself.
#[derive(Debug, Clone)]
pub enum Settlement {
    /// The run completed the job: move to `completed` and stamp `finished_at`.
    Complete {
        /// Terminal timestamp.
        finished_at: DateTime<Utc>,
    },
    /// A retryable failure: back to `pending`, eligible again at `visible_at`,
    /// with `failure_count` bumped and the carry persisted for the next run.
    Retry {
        /// When the job becomes eligible again (now + backoff).
        visible_at: DateTime<Utc>,
        /// The new failure count to store.
        failure_count: i32,
        /// The carried state to persist for the next run.
        carry: serde_json::Value,
    },
    /// A cooperative pause: back to `pending`, eligible at `visible_at`, carry
    /// persisted, no failure recorded.
    Pause {
        /// When the job becomes eligible again (now + resume_in).
        visible_at: DateTime<Utc>,
        /// The carried state to persist for the next run.
        carry: serde_json::Value,
    },
    /// A permanent give-up: move to `dead` and stamp `finished_at`.
    Dead {
        /// Terminal timestamp.
        finished_at: DateTime<Utc>,
        /// The new failure count to store.
        failure_count: i32,
    },
    /// A clean shutdown release: back to `pending`, eligible immediately, not a
    /// failure.
    Release {
        /// When the job becomes eligible again (typically now).
        visible_at: DateTime<Utc>,
    },
}

// =============================================================================
// The backend trait
// =============================================================================

/// The storage backend contract.
///
/// Implementors provide durable, concurrency-safe storage for the queue. The
/// default implementation is [`crate::postgres::PostgresStore`]; tests use an
/// in-memory fake. The trait is `async` (via `async_trait` for object safety) so
/// a worker can hold an `Arc<dyn Store>`.
///
/// Operations are added to this trait as the queue gains capabilities across the
/// build phases. For now it covers schema setup; claiming, settlement, dedup,
/// recovery, history, cleanup, and stats arrive with their respective phases.
#[async_trait]
pub trait Store: Send + Sync {
    /// Apply the schema migrations for this backend's configured table prefix.
    ///
    /// Idempotent: applying twice is a no-op, and two backends with different
    /// prefixes migrate independently.
    async fn migrate(&self) -> Result<(), Error>;

    /// Insert a brand-new pending job.
    ///
    /// This is the plain, non-deduplicating enqueue path; the dedup-aware path is
    /// layered on in a later phase.
    async fn enqueue(&self, job: &NewJob) -> Result<(), Error>;

    /// Atomically claim the next eligible job among `kinds`, or `None` if there
    /// is nothing claimable right now.
    ///
    /// Eligibility is `status = 'pending' AND visible_at <= now()`, ordered by
    /// priority then age, restricted to `kinds` and to `priority >= priority_floor`
    /// (the anti-starvation floor). Concurrent claimers skip each other's locked
    /// rows. The claimed row's status becomes `claimed`, its `claimed_by` is set,
    /// its `run_count` is incremented, and its lease expires after `lease`.
    async fn claim_next(
        &self,
        kinds: &[String],
        priority_floor: i16,
        lease: Duration,
        claimed_by: &str,
    ) -> Result<Option<JobRecord>, Error>;

    /// Apply a settlement to a claimed job, guarded by claim ownership, appending
    /// `journal` in the same transaction.
    ///
    /// The write applies only if the row is still `claimed` by `claimed_by` at the
    /// claim epoch `run_no` (the `run_count` stamped when this run claimed the job).
    /// Matching on the epoch, not just the identity, means a slow or aborted handler
    /// cannot settle a claim that was reclaimed and re-run — even when the reclaiming
    /// worker shares its `claimed_by` identity (the common self-recovery case).
    /// Returns `true` if it applied and `false` if the guard did not match (the
    /// job was reclaimed or already settled), so the caller can skip rather than
    /// retry. When the guard does not match, the journal entry is not written.
    async fn settle(
        &self,
        id: Ulid,
        claimed_by: &str,
        run_no: u32,
        settlement: Settlement,
        journal: JournalAppend,
    ) -> Result<bool, Error>;

    /// Load a job's journal in chronological order.
    async fn journal(&self, id: Ulid) -> Result<Vec<JournalRecord>, Error>;

    /// The soonest future eligibility time among pending jobs of the worker's
    /// `kinds`, or `None` if none are scheduled for the future.
    ///
    /// The worker uses this to wake exactly when a delayed job (a backoff retry, a
    /// paused job's resume, a scheduled enqueue) becomes claimable, rather than
    /// only at the poll interval.
    async fn next_visible_at(&self, kinds: &[String]) -> Result<Option<DateTime<Utc>>, Error>;

    /// A notifier that wakes the worker when newly enqueued work may be available.
    ///
    /// The default is a notifier that never fires, leaving the worker to rely on
    /// its bounded poll. The PostgreSQL adapter overrides this with a `LISTEN`
    /// connection when configured for it.
    async fn notifier(&self) -> Result<Box<dyn Notifier>, Error> {
        Ok(Box::new(NeverNotifier))
    }

    /// Find the deduplication candidate for `(kind, dedup_key)`: the oldest
    /// pending job sharing that key, or `None` if there is none.
    ///
    /// The dedup index is non-unique, so several pending siblings may exist; this
    /// returns the oldest, which the enqueue path passes to the task's `merge`.
    async fn dedup_candidate(
        &self,
        kind: &str,
        dedup_key: &str,
    ) -> Result<Option<JobRecord>, Error>;

    /// Find claimed jobs whose lease has expired, for stale-claim recovery.
    ///
    /// Detection is timeout-only (`status = 'claimed' AND claim_expires_at <
    /// now()`), with no process-liveness check, so it behaves identically on one
    /// host or many. Implementations may bound the batch size.
    async fn find_stale(&self) -> Result<Vec<JobRecord>, Error>;

    /// Recover a stale claim, re-pending it as a failed execution and appending
    /// `journal`.
    ///
    /// Guarded by the row still being claimed with an expired lease *at the claim
    /// epoch `run_no`* (the `run_count` of the stale claim this recovery observed).
    /// The epoch keeps a recovery computed from an older `find_stale` snapshot from
    /// re-pending a claim that has since been reclaimed and re-run, which would
    /// otherwise regress `failure_count` and journal a stale `run_no`. Returns
    /// whether this call recovered it.
    ///
    /// `failure_count` is the value to write, not an increment; the caller
    /// pre-computes `stored_failure_count + 1`.
    async fn recover(
        &self,
        id: Ulid,
        visible_at: DateTime<Utc>,
        failure_count: i32,
        run_no: u32,
        journal: JournalAppend,
    ) -> Result<bool, Error>;

    /// Extend a claimed job's lease, guarded by claim ownership at the claim epoch
    /// `run_no`.
    ///
    /// Used to apply a per-task `Task::lease` override after the claim, which sets
    /// only the worker default. Guarding on `run_no` as well as `claimed_by` keeps
    /// a stale run from extending the lease of a claim that was reclaimed under the
    /// same identity. Returns whether the lease was extended.
    async fn extend_lease(
        &self,
        id: Ulid,
        claimed_by: &str,
        run_no: u32,
        lease: Duration,
    ) -> Result<bool, Error>;

    /// Query jobs by the history filter, newest first.
    async fn query_jobs(&self, filter: &HistoryFilter) -> Result<Vec<JobRecord>, Error>;

    /// Fetch a single job by id, or `None` if no such job exists.
    ///
    /// This reads one full job row, including its `payload` and `carry`, for
    /// detail inspection — a point lookup on the primary key rather than the
    /// filtered, paginated scan [`query_jobs`](Store::query_jobs) performs.
    async fn job(&self, id: Ulid) -> Result<Option<JobRecord>, Error>;

    /// Bulk-delete terminal jobs matching the criteria, cascading to the journal.
    /// Returns the number of jobs deleted.
    ///
    /// Invoked only when a caller asks: nothing in venturi calls this on its own
    /// (see [`Queue::cleanup`](crate::Queue::cleanup) for the retention contract).
    /// The predicate is anchored on `finished_at`, which the default adapter
    /// indexes, so an empty sweep is an indexed probe rather than a table scan.
    async fn cleanup(&self, criteria: &CleanupCriteria) -> Result<u64, Error>;

    /// Compute a live state snapshot from on-demand aggregate queries.
    async fn stats(&self) -> Result<Snapshot, Error>;

    /// Apply a merge decision to a pending candidate, appending `journal`.
    ///
    /// `update` carries the new `(payload, carry)` for a Replace/With decision, or
    /// is `None` for Keep (the existing row is left untouched but the merge is
    /// still journaled). The write is guarded by the candidate still being
    /// `pending`; if it has since been claimed or removed, nothing is written and
    /// this returns `false` so the caller can fall back to a fresh enqueue.
    async fn merge_into(
        &self,
        id: Ulid,
        update: Option<MergePayload>,
        journal: JournalAppend,
    ) -> Result<bool, Error>;
}

/// The new payload, carry, and priority to write when applying a Replace or With
/// merge.
#[derive(Debug, Clone)]
pub struct MergePayload {
    /// The serialized payload the surviving job should carry.
    pub payload: serde_json::Value,
    /// The serialized carry the surviving job should continue from.
    pub carry: serde_json::Value,
    /// The priority tier of the superseding task, so claim ordering follows the
    /// merged-in work rather than the row it replaced.
    pub priority: i16,
}

/// A filter for the history query: every set field narrows the result, and the
/// unset fields are unconstrained.
#[derive(Debug, Clone, Default)]
pub struct HistoryFilter {
    /// Restrict to one task kind.
    pub kind: Option<String>,
    /// Restrict to one lifecycle status.
    pub status: Option<Status>,
    /// Only jobs that reached a terminal state at or after this time.
    pub finished_since: Option<DateTime<Utc>>,
    /// Only jobs that reached a terminal state strictly before this time.
    pub finished_until: Option<DateTime<Utc>>,
    /// Keyset cursor for stable pagination. Returns only jobs ordered strictly
    /// after the given `(created_at, id)` under the query's `created_at DESC,
    /// id DESC` order — that is, older than the cursor. `None` starts from the
    /// most recent job. Paired with `limit`, this walks history in pages that
    /// stay correct as rows are inserted or removed between requests, unlike an
    /// offset.
    pub created_before: Option<(DateTime<Utc>, Ulid)>,
    /// Cap the number of rows returned (most recently created first).
    pub limit: Option<i64>,
}

/// Criteria for bulk cleanup: which terminal jobs to delete.
///
/// Only terminal jobs (those with a `finished_at`) are eligible, and removing a
/// job cascades to its journal.
#[derive(Debug, Clone)]
pub struct CleanupCriteria {
    /// Delete jobs whose `finished_at` is strictly before this time.
    pub finished_before: DateTime<Utc>,
    /// Restrict deletion to one kind.
    pub kind: Option<String>,
    /// Restrict deletion to one terminal status (`Completed` or `Dead`); `None`
    /// deletes both.
    pub status: Option<Status>,
}

/// A point-in-time snapshot of live queue state, from on-demand aggregates.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    /// Backlog depth per kind: the number of pending jobs.
    pub pending_by_kind: BTreeMap<String, u64>,
    /// The age of the oldest pending job per kind, measured from its enqueue time.
    pub oldest_pending_age: BTreeMap<String, Duration>,
    /// The number of jobs currently claimed (in flight across the system).
    pub claimed: u64,
    /// The number of dead jobs per kind.
    pub dead_by_kind: BTreeMap<String, u64>,
}

/// A wakeup source the worker awaits to learn that new work may be available.
///
/// A backend that supports push notification (the PostgreSQL adapter's `LISTEN`)
/// returns one whose [`recv`](Notifier::recv) resolves on each notification; a
/// backend without one uses a notifier that never fires.
#[async_trait]
pub trait Notifier: Send {
    /// Resolve when newly enqueued work may be available. A spurious wakeup is
    /// harmless: the worker simply re-checks the queue.
    async fn recv(&mut self);
}

/// A [`Notifier`] that never fires, for backends without push notification.
pub(crate) struct NeverNotifier;

#[async_trait]
impl Notifier for NeverNotifier {
    async fn recv(&mut self) {
        std::future::pending::<()>().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job_with_priority(priority: i16) -> NewJob {
        let now = Utc::now();
        NewJob {
            id: Ulid::new(),
            kind: "demo".to_owned(),
            payload: serde_json::Value::Null,
            priority,
            created_at: now,
            visible_at: now,
            carry: serde_json::Value::Null,
            dedup_key: None,
        }
    }

    #[test]
    fn validate_accepts_every_supported_tier() {
        for tier in 0..=2 {
            assert!(job_with_priority(tier).validate().is_ok());
        }
    }

    #[test]
    fn validate_rejects_out_of_range_priority() {
        for tier in [-1, 3, i16::MAX] {
            match job_with_priority(tier).validate() {
                Err(Error::InvalidPriority { priority }) => assert_eq!(priority, tier),
                other => panic!("expected InvalidPriority for {tier}, got {other:?}"),
            }
        }
    }
}
