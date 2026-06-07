//! The producer-side queue handle.
//!
//! A [`Queue`] turns a typed [`Task`] into a stored job and enqueues it. It needs
//! only the [`Task`] trait, never a handler or the worker's shared state, so a
//! producer binary can depend on it without pulling in execution logic. It is a
//! thin handle over a [`Store`]; clone it freely.

use crate::error::Error;
use crate::store::{NewJob, Store};
use crate::task::Task;
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
    /// The job becomes eligible to claim at once. Returns the assigned job id.
    pub async fn enqueue<T: Task>(&self, task: T) -> Result<Ulid, Error> {
        let now = Utc::now();
        self.insert(task, now, now).await
    }

    /// Enqueue a task to first become eligible at `when`.
    ///
    /// Until `when`, the job is invisible to claims. Returns the assigned job id.
    pub async fn enqueue_at<T: Task>(&self, task: T, when: DateTime<Utc>) -> Result<Ulid, Error> {
        let now = Utc::now();
        self.insert(task, now, when).await
    }

    /// Serialize a task into a [`NewJob`] and insert it.
    async fn insert<T: Task>(
        &self,
        task: T,
        created_at: DateTime<Utc>,
        visible_at: DateTime<Utc>,
    ) -> Result<Ulid, Error> {
        let id = Ulid::new();
        let payload = serde_json::to_value(&task)?;
        let carry = serde_json::to_value(T::Carry::default())?;
        let dedup_key = task.dedup_key().map(|key| key.into_string());

        let job = NewJob {
            id,
            kind: T::KIND.to_owned(),
            payload,
            priority: task.priority().as_smallint(),
            created_at,
            visible_at,
            carry,
            dedup_key,
        };

        self.store.enqueue(&job).await?;
        Ok(id)
    }
}
