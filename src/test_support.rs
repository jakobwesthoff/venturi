//! An in-memory [`Store`] implementation for testing the worker loop without a
//! database.
//!
//! It mirrors the semantics the PostgreSQL adapter implements in SQL: claim picks
//! the highest-priority oldest eligible row for a registered kind above the
//! priority floor, settlement is guarded by claim ownership, and `visible_at`
//! gates eligibility. It is deliberately simple (one mutex over a row map) since
//! tests value clarity over throughput.

use crate::error::Error;
use crate::store::{JobRecord, NewJob, Settlement, Status, Store};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use ulid::Ulid;

/// A shareable in-memory store. Cloning shares the same underlying state.
#[derive(Clone, Default)]
pub(crate) struct FakeStore {
    inner: Arc<Mutex<HashMap<Ulid, JobRecord>>>,
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
            .get(&id)
            .cloned()
    }

    /// Count jobs currently in a given lifecycle state.
    pub(crate) fn count(&self, status: Status) -> usize {
        self.inner
            .lock()
            .expect("lock not poisoned")
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
        for job in guard.values() {
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

        let job = guard.get_mut(&id).expect("selected job exists");
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
    ) -> Result<bool, Error> {
        let mut guard = self.inner.lock().expect("lock not poisoned");
        let Some(job) = guard.get_mut(&id) else {
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
        Ok(true)
    }
}

/// Add a `std::time::Duration` to a UTC instant, saturating on overflow.
fn add_duration(now: DateTime<Utc>, delta: Duration) -> DateTime<Utc> {
    match chrono::Duration::from_std(delta) {
        Ok(delta) => now.checked_add_signed(delta).unwrap_or(now),
        Err(_) => now,
    }
}
