//! An in-memory [`Store`] implementation for testing the worker loop without a
//! database.
//!
//! It mirrors the semantics the PostgreSQL adapter implements in SQL: claim picks
//! the highest-priority oldest eligible row for a registered kind above the
//! priority floor, settlement is guarded by claim ownership and appends a journal
//! entry in the same step, and `visible_at` gates eligibility. It is deliberately
//! simple (one mutex over the whole state) since tests value clarity over
//! throughput.

use crate::error::Error;
use crate::store::{JobRecord, JournalAppend, JournalRecord, NewJob, Settlement, Status, Store};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use ulid::Ulid;

/// The mutable state behind a [`FakeStore`].
#[derive(Default)]
struct Inner {
    jobs: HashMap<Ulid, JobRecord>,
    journal: Vec<JournalRecord>,
    next_journal_id: i64,
}

/// A shareable in-memory store. Cloning shares the same underlying state.
#[derive(Clone, Default)]
pub(crate) struct FakeStore {
    inner: Arc<Mutex<Inner>>,
}

impl FakeStore {
    /// An empty store.
    pub(crate) fn new() -> FakeStore {
        FakeStore::default()
    }

    /// Snapshot a job by id.
    pub(crate) fn job(&self, id: Ulid) -> Option<JobRecord> {
        self.inner
            .lock()
            .expect("lock not poisoned")
            .jobs
            .get(&id)
            .cloned()
    }

    /// Count jobs currently in a given lifecycle state.
    pub(crate) fn count(&self, status: Status) -> usize {
        self.inner
            .lock()
            .expect("lock not poisoned")
            .jobs
            .values()
            .filter(|job| job.status == status)
            .count()
    }
}

#[async_trait]
impl Store for FakeStore {
    async fn migrate(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn enqueue(&self, job: &NewJob) -> Result<(), Error> {
        let record = JobRecord {
            id: job.id,
            kind: job.kind.clone(),
            payload: job.payload.clone(),
            priority: job.priority,
            status: Status::Pending,
            created_at: job.created_at,
            visible_at: job.visible_at,
            claim_expires_at: None,
            claimed_by: None,
            finished_at: None,
            run_count: 0,
            failure_count: 0,
            carry: job.carry.clone(),
            dedup_key: job.dedup_key.clone(),
        };
        self.inner
            .lock()
            .expect("lock not poisoned")
            .jobs
            .insert(job.id, record);
        Ok(())
    }

    async fn claim_next(
        &self,
        kinds: &[String],
        priority_floor: i16,
        lease: Duration,
        claimed_by: &str,
    ) -> Result<Option<JobRecord>, Error> {
        let now = Utc::now();
        let mut guard = self.inner.lock().expect("lock not poisoned");

        // Find the highest-priority oldest eligible row, tie-broken by id for
        // determinism (the real claim leaves equal-key ties to the planner).
        let mut best: Option<Ulid> = None;
        let mut best_key: Option<(i16, DateTime<Utc>, Ulid)> = None;
        for job in guard.jobs.values() {
            let eligible = job.status == Status::Pending
                && job.visible_at <= now
                && job.priority >= priority_floor
                && kinds.iter().any(|k| k == &job.kind);
            if !eligible {
                continue;
            }
            let key = (job.priority, job.created_at, job.id);
            if best_key.as_ref().is_none_or(|b| key < *b) {
                best_key = Some(key);
                best = Some(job.id);
            }
        }

        let Some(id) = best else {
            return Ok(None);
        };

        let job = guard.jobs.get_mut(&id).expect("selected job exists");
        job.status = Status::Claimed;
        job.claimed_by = Some(claimed_by.to_owned());
        job.claim_expires_at = Some(add_duration(now, lease));
        job.run_count += 1;
        Ok(Some(job.clone()))
    }

    async fn settle(
        &self,
        id: Ulid,
        claimed_by: &str,
        settlement: Settlement,
        journal: JournalAppend,
    ) -> Result<bool, Error> {
        let mut guard = self.inner.lock().expect("lock not poisoned");
        let Some(job) = guard.jobs.get_mut(&id) else {
            return Ok(false);
        };

        // The ownership guard: only the current claimant settles a claimed row.
        if job.status != Status::Claimed || job.claimed_by.as_deref() != Some(claimed_by) {
            return Ok(false);
        }

        match settlement {
            Settlement::Complete { finished_at } => {
                job.status = Status::Completed;
                job.finished_at = Some(finished_at);
            }
            Settlement::Retry {
                visible_at,
                failure_count,
                carry,
            } => {
                job.status = Status::Pending;
                job.visible_at = visible_at;
                job.failure_count = failure_count;
                job.carry = carry;
            }
            Settlement::Pause { visible_at, carry } => {
                job.status = Status::Pending;
                job.visible_at = visible_at;
                job.carry = carry;
            }
            Settlement::Dead {
                finished_at,
                failure_count,
            } => {
                job.status = Status::Dead;
                job.finished_at = Some(finished_at);
                job.failure_count = failure_count;
            }
            Settlement::Release { visible_at } => {
                job.status = Status::Pending;
                job.visible_at = visible_at;
            }
        }
        job.claimed_by = None;
        job.claim_expires_at = None;

        let entry_id = guard.next_journal_id;
        guard.next_journal_id += 1;
        guard.journal.push(JournalRecord {
            id: entry_id,
            job_id: id,
            kind: journal.kind,
            run_no: journal.run_no,
            recorded_at: journal.recorded_at,
            outcome: journal.outcome,
            note: journal.note,
            attachment: journal.attachment,
        });
        Ok(true)
    }

    async fn journal(&self, id: Ulid) -> Result<Vec<JournalRecord>, Error> {
        let guard = self.inner.lock().expect("lock not poisoned");
        let mut entries: Vec<JournalRecord> = guard
            .journal
            .iter()
            .filter(|entry| entry.job_id == id)
            .cloned()
            .collect();
        entries.sort_by_key(|entry| entry.id);
        Ok(entries)
    }
}

/// Add a `std::time::Duration` to a UTC instant, saturating on overflow.
fn add_duration(now: DateTime<Utc>, delta: Duration) -> DateTime<Utc> {
    match chrono::Duration::from_std(delta) {
        Ok(delta) => now.checked_add_signed(delta).unwrap_or(now),
        Err(_) => now,
    }
}
