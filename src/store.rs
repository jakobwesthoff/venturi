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
    pub run_count: i32,
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
    pub run_no: i32,
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
    pub run_no: i32,
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
    /// The write applies only if the row is still `claimed` by `claimed_by`.
    /// Returns `true` if it applied and `false` if the guard did not match (the
    /// job was reclaimed or already settled), so the caller can skip rather than
    /// retry. When the guard does not match, the journal entry is not written.
    async fn settle(
        &self,
        id: Ulid,
        claimed_by: &str,
        settlement: Settlement,
        journal: JournalAppend,
    ) -> Result<bool, Error>;

    /// Load a job's journal in chronological order.
    async fn journal(&self, id: Ulid) -> Result<Vec<JournalRecord>, Error>;
}
