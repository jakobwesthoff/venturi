//! The producer-side queue handle.
//!
//! A [`Queue`] turns a typed [`Task`] into a stored job and enqueues it, applying
//! the deduplication flow when the task opts in. It needs only the [`Task`] trait,
//! never a handler or the worker's shared state, so a producer binary can depend
//! on it without pulling in execution logic. It is a thin handle over a [`Store`];
//! clone it freely.

use crate::context::JournalEntry;
use crate::error::Error;
use crate::store::{
    CleanupCriteria, HistoryFilter, JobRecord, JournalAppend, JournalOutcome, JournalRecord,
    MergePayload, NewJob, Snapshot, Store,
};
use crate::task::{Merge, Pending, Task};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use ulid::Ulid;

/// A handle for enqueuing tasks onto a backing [`Store`].
#[derive(Clone)]
pub struct Queue {
    store: Arc<dyn Store>,
}

impl Queue {
    /// Build a queue handle over a store.
    pub fn new(store: Arc<dyn Store>) -> Queue {
        Queue { store }
    }

    /// The underlying store, for callers that need direct access.
    pub fn store(&self) -> &Arc<dyn Store> {
        &self.store
    }

    /// Enqueue a task for immediate processing.
    ///
    /// The job becomes eligible to claim at once. If the task deduplicates and a
    /// pending sibling exists, the task's `merge` decision is applied instead.
    /// Returns the id of the surviving job (the existing one on a merge, a new one
    /// otherwise).
    pub async fn enqueue<T: Task>(&self, task: T) -> Result<Ulid, Error> {
        let now = Utc::now();
        self.submit(task, now, now).await
    }

    /// Enqueue a task to first become eligible at `when`.
    ///
    /// Until `when`, the job is invisible to claims. The deduplication flow still
    /// applies. Returns the id of the surviving job.
    pub async fn enqueue_at<T: Task>(&self, task: T, when: DateTime<Utc>) -> Result<Ulid, Error> {
        let now = Utc::now();
        self.submit(task, now, when).await
    }

    /// Query jobs by the history filter, most recently created first.
    ///
    /// This reads individual job rows for inspection and history; it is distinct
    /// from [`stats`](Queue::stats), which reports live aggregate state.
    pub async fn jobs(&self, filter: &HistoryFilter) -> Result<Vec<JobRecord>, Error> {
        self.store.query_jobs(filter).await
    }

    /// Fetch a single job by id, including its `payload` and `carry`, or `None`
    /// if no such job exists.
    ///
    /// A primary-key point lookup for detail inspection, distinct from
    /// [`jobs`](Queue::jobs), which scans a filtered, paginated history page.
    pub async fn job(&self, id: Ulid) -> Result<Option<JobRecord>, Error> {
        self.store.job(id).await
    }

    /// Fetch a single job's journal timeline in chronological order.
    pub async fn job_journal(&self, id: Ulid) -> Result<Vec<JournalRecord>, Error> {
        self.store.journal(id).await
    }

    /// Bulk-delete terminal jobs matching the criteria, cascading to their
    /// journal entries. Returns the number of jobs deleted.
    pub async fn cleanup(&self, criteria: &CleanupCriteria) -> Result<u64, Error> {
        self.store.cleanup(criteria).await
    }

    /// A live snapshot of queue state: backlog depth and oldest-pending age per
    /// kind, the in-flight (claimed) count, and dead counts per kind.
    pub async fn stats(&self) -> Result<Snapshot, Error> {
        self.store.stats().await
    }

    /// The deduplication-aware enqueue: a plain insert when the task does not
    /// deduplicate or has no pending sibling, otherwise the task's `merge`
    /// decision applied to that sibling.
    async fn submit<T: Task>(
        &self,
        task: T,
        created_at: DateTime<Utc>,
        visible_at: DateTime<Utc>,
    ) -> Result<Ulid, Error> {
        let Some(dedup_key) = task.dedup_key() else {
            return self.insert(&task, created_at, visible_at, None).await;
        };
        let key = dedup_key.into_string();

        // Dedup is a read-modify-write across three store calls (read the
        // candidate, compute the merge below, write it via `apply_merge`), not one
        // transaction. Concurrent enqueues of the same key therefore resolve
        // last-writer-wins; see `Task::merge` for the contract.
        let Some(candidate) = self.store.dedup_candidate(T::KIND, &key).await? else {
            return self.insert(&task, created_at, visible_at, Some(key)).await;
        };

        // Reconstruct the existing job's typed state so the decision is informed
        // by its content and history.
        let existing_payload: T = serde_json::from_value(candidate.payload.clone())?;
        let existing_carry: T::Carry = if candidate.carry.is_null() {
            T::Carry::default()
        } else {
            serde_json::from_value(candidate.carry.clone())?
        };
        let journal = self
            .store
            .journal(candidate.id)
            .await?
            .into_iter()
            .map(JournalEntry::from_record)
            .collect();
        let pending = Pending::new(
            existing_payload,
            existing_carry,
            candidate.run_count.max(0) as u32,
            journal,
        );

        match task.merge(&pending) {
            Merge::Independent => self.insert(&task, created_at, visible_at, Some(key)).await,
            Merge::Keep => {
                self.apply_merge(&candidate, None, &task, created_at, visible_at, &key)
                    .await
            }
            Merge::Replace => {
                let update = MergePayload {
                    payload: serde_json::to_value(&task)?,
                    carry: serde_json::to_value(T::Carry::default())?,
                    priority: task.priority().as_smallint(),
                };
                self.apply_merge(
                    &candidate,
                    Some(update),
                    &task,
                    created_at,
                    visible_at,
                    &key,
                )
                .await
            }
            Merge::With {
                task: merged,
                carry,
            } => {
                let update = MergePayload {
                    payload: serde_json::to_value(&merged)?,
                    carry: serde_json::to_value(carry)?,
                    priority: merged.priority().as_smallint(),
                };
                self.apply_merge(
                    &candidate,
                    Some(update),
                    &task,
                    created_at,
                    visible_at,
                    &key,
                )
                .await
            }
        }
    }

    /// Apply a Keep/Replace/With merge to the candidate, recording a `merged`
    /// journal entry. If the candidate is no longer pending (it was claimed in the
    /// meantime), fall back to a fresh enqueue of the incoming task so no work is
    /// lost.
    async fn apply_merge<T: Task>(
        &self,
        candidate: &crate::store::JobRecord,
        update: Option<MergePayload>,
        incoming: &T,
        created_at: DateTime<Utc>,
        visible_at: DateTime<Utc>,
        key: &str,
    ) -> Result<Ulid, Error> {
        let journal = JournalAppend {
            kind: candidate.kind.clone(),
            run_no: candidate.run_count.max(0),
            recorded_at: Utc::now(),
            outcome: JournalOutcome::Merged,
            note: Some("enqueue merged into pending job".to_owned()),
            attachment: None,
        };

        if self.store.merge_into(candidate.id, update, journal).await? {
            crate::observability::merged(&candidate.kind);
            Ok(candidate.id)
        } else {
            self.insert(incoming, created_at, visible_at, Some(key.to_owned()))
                .await
        }
    }

    /// Serialize a task into a fresh [`NewJob`] and insert it.
    async fn insert<T: Task>(
        &self,
        task: &T,
        created_at: DateTime<Utc>,
        visible_at: DateTime<Utc>,
        dedup_key: Option<String>,
    ) -> Result<Ulid, Error> {
        let id = Ulid::new();
        let job = NewJob {
            id,
            kind: T::KIND.to_owned(),
            payload: serde_json::to_value(task)?,
            priority: task.priority().as_smallint(),
            created_at,
            visible_at,
            carry: serde_json::to_value(T::Carry::default())?,
            dedup_key,
        };
        self.store.enqueue(&job).await?;
        crate::observability::enqueued(T::KIND);
        Ok(id)
    }
}
