//! The task-authoring surface: the [`Task`] and [`Handler`] traits a consuming
//! project implements, plus the small value types they use.
//!
//! A unit of work is one struct that serves as both the payload and the identity
//! of a job. It is touched at two sites: a **producer** enqueues it (needs only
//! [`Task`]: identity and enqueue-time policy) and a **worker** runs it (needs
//! [`Handler<S>`]: the execution logic against the worker's shared state `S`).
//! Splitting the two lets a producer binary depend on neither the handler nor its
//! runtime dependencies.

use crate::backoff::Backoff;
use crate::context::{Context, JournalEntry};
use crate::outcome::{Outcome, TaskError};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::time::Duration;

// =============================================================================
// Priority
// =============================================================================

/// A job's scheduling priority tier.
///
/// Three fixed tiers, defaulting to [`Priority::Normal`]. The claim orders by
/// priority then age, so `High` is served before `Normal` before `Low`, subject
/// to the worker's anti-starvation rotation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Priority {
    /// Served first.
    High,
    /// The default tier.
    #[default]
    Normal,
    /// Served last.
    Low,
}

impl Priority {
    /// The numeric tier stored in the `priority` column: 0 high, 1 normal, 2 low.
    ///
    /// The ordering is deliberate: a plain `ORDER BY priority ASC` puts `High`
    /// first.
    pub fn as_smallint(self) -> i16 {
        match self {
            Priority::High => 0,
            Priority::Normal => 1,
            Priority::Low => 2,
        }
    }

    /// Parse the numeric tier read from storage.
    pub fn from_smallint(value: i16) -> Option<Priority> {
        match value {
            0 => Some(Priority::High),
            1 => Some(Priority::Normal),
            2 => Some(Priority::Low),
            _ => None,
        }
    }
}

// =============================================================================
// Deduplication key
// =============================================================================

/// A candidacy key for deduplication.
///
/// Two pending jobs with the same `(KIND, DedupKey)` are collision candidates,
/// found through an index. Returning `None` from [`Task::dedup_key`] opts a task
/// out of coalescing entirely. Build one from any string-like or identifier value
/// via [`From`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DedupKey(String);

impl DedupKey {
    /// Build a key from any value convertible into a string.
    pub fn new(key: impl Into<String>) -> DedupKey {
        DedupKey(key.into())
    }

    /// The key's string form, as stored in the `dedup_key` column.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume the key, yielding its owned string.
    pub(crate) fn into_string(self) -> String {
        self.0
    }
}

impl From<String> for DedupKey {
    fn from(value: String) -> DedupKey {
        DedupKey(value)
    }
}

impl From<&str> for DedupKey {
    fn from(value: &str) -> DedupKey {
        DedupKey(value.to_owned())
    }
}

impl From<ulid::Ulid> for DedupKey {
    fn from(value: ulid::Ulid) -> DedupKey {
        DedupKey(value.to_string())
    }
}

impl From<u64> for DedupKey {
    fn from(value: u64) -> DedupKey {
        DedupKey(value.to_string())
    }
}

impl From<i64> for DedupKey {
    fn from(value: i64) -> DedupKey {
        DedupKey(value.to_string())
    }
}

// =============================================================================
// Deduplication merge
// =============================================================================

/// The existing pending job that a [`Task::merge`] decision sees.
///
/// Carries the full state of the colliding pending job, so the decision is
/// informed by content and history and can continue in-progress work.
pub struct Pending<T: Task> {
    payload: T,
    carry: T::Carry,
    run_count: u32,
    journal: Vec<JournalEntry>,
}

impl<T: Task> Pending<T> {
    /// Build a view of an existing pending job. Used by the enqueue dedup flow.
    pub(crate) fn new(
        payload: T,
        carry: T::Carry,
        run_count: u32,
        journal: Vec<JournalEntry>,
    ) -> Pending<T> {
        Pending {
            payload,
            carry,
            run_count,
            journal,
        }
    }

    /// The existing job's deserialized payload.
    pub fn payload(&self) -> &T {
        &self.payload
    }

    /// The existing job's carried state.
    pub fn carry(&self) -> &T::Carry {
        &self.carry
    }

    /// How many times the existing job has already run.
    pub fn run_count(&self) -> u32 {
        self.run_count
    }

    /// The existing job's journal so far.
    pub fn journal(&self) -> &[JournalEntry] {
        &self.journal
    }
}

/// The decision returned by [`Task::merge`] when an enqueue collides with a
/// pending job sharing its `(KIND, dedup_key)`.
pub enum Merge<T: Task> {
    /// The incoming task is redundant; leave the existing job untouched.
    Keep,
    /// Replace the existing payload with the incoming one; reset carry to default.
    Replace,
    /// Replace the existing job with a computed payload and carry, continuing its
    /// work.
    With {
        /// The payload the surviving job should carry.
        task: T,
        /// The carry the surviving job should continue from.
        carry: T::Carry,
    },
    /// Not a duplicate after all; enqueue as a new, independent row.
    Independent,
}

// =============================================================================
// Task and Handler
// =============================================================================

/// Identity and enqueue-time policy for a unit of work.
///
/// State-free, so a producer that never runs the work can implement and use it
/// without the worker's dependencies. Implemented directly on the payload struct.
pub trait Task: Serialize + DeserializeOwned + Send + Sync + 'static + Sized {
    /// A stable discriminator stored alongside the payload and used to route the
    /// job back to its handler. Must be unique per task type and stable across
    /// releases.
    const KIND: &'static str;

    /// State carried between runs of the same job, also visible to [`merge`]. Use
    /// `()` for tasks that keep nothing.
    ///
    /// [`merge`]: Task::merge
    type Carry: Serialize + DeserializeOwned + Default + Send;

    /// The scheduling priority for this task. Defaults to [`Priority::Normal`].
    fn priority(&self) -> Priority {
        Priority::Normal
    }

    /// The deduplication candidacy key. `None` (the default) never coalesces.
    fn dedup_key(&self) -> Option<DedupKey> {
        None
    }

    /// Decide what happens when a pending job with the same `(KIND, dedup_key)`
    /// already exists. Called only on a collision; the default replaces the
    /// existing payload.
    fn merge(&self, existing: &Pending<Self>) -> Merge<Self> {
        let _ = existing;
        Merge::Replace
    }

    /// A per-task override of the retry backoff. `None` (the default) uses the
    /// worker default.
    fn backoff(&self) -> Option<Backoff> {
        None
    }

    /// A per-task override of the claim lease. `None` (the default) uses the
    /// worker default. A task known to run long can request a longer lease so a
    /// healthy worker is not reclaimed mid-run.
    fn lease(&self) -> Option<Duration> {
        None
    }
}

/// The execution side of a task, parameterized over the worker's shared state
/// `S`. A producer crate never implements this.
///
/// [`Task`] is a supertrait because running a job first requires identifying and
/// deserializing it. The returned future is required to be `Send` so the worker
/// can run handlers concurrently across threads; writing the method as
/// `async fn handle` in an impl satisfies this whenever the body is `Send`.
pub trait Handler<S>: Task {
    /// Run the job. `&self` is the deserialized payload, `state` is the worker's
    /// shared dependencies, and `ctx` is this run's execution context.
    ///
    /// A panic here is caught at the task boundary and settled as a failed
    /// execution (retried with backoff by default, or sent to dead per the
    /// worker's panic policy); the run's context is discarded and a retry resumes
    /// from the pre-run carry. Any invariant of the shared `state` (for example a
    /// lock that a panic could poison) is the handler's responsibility. Under a
    /// build that aborts on panic the process ends instead and the job is recovered
    /// by lease expiry.
    fn handle(
        &self,
        ctx: &mut Context<Self::Carry>,
        state: &S,
    ) -> impl Future<Output = Result<Outcome, TaskError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_smallint_round_trips_and_orders() {
        for p in [Priority::High, Priority::Normal, Priority::Low] {
            assert_eq!(Priority::from_smallint(p.as_smallint()), Some(p));
        }
        assert!(Priority::High.as_smallint() < Priority::Normal.as_smallint());
        assert!(Priority::Normal.as_smallint() < Priority::Low.as_smallint());
        assert_eq!(Priority::default(), Priority::Normal);
        assert_eq!(Priority::from_smallint(3), None);
    }

    #[test]
    fn dedup_key_from_various_sources() {
        assert_eq!(DedupKey::from("abc").as_str(), "abc");
        assert_eq!(DedupKey::from(String::from("x")).as_str(), "x");
        assert_eq!(DedupKey::from(42u64).as_str(), "42");
        let id = ulid::Ulid::nil();
        assert_eq!(DedupKey::from(id).as_str(), id.to_string());
    }
}
