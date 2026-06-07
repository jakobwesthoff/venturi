//! venturi is a durable, PostgreSQL-backed job queue for Rust.
//!
//! Work is modelled as a [`task::Task`]: a plain serializable struct that is both
//! the payload and the identity of a job. Producers enqueue tasks through a
//! [`Queue`](queue::Queue) handle; workers claim, run, and settle them through a
//! [`Worker`](worker::Worker) that dispatches each job back to its
//! [`task::Handler`].
//!
//! The crate is layered so each layer is usable on its own: storage sits behind
//! the [`store::Store`] backend trait, the worker drives the claim/dispatch loop,
//! and the task layer defines the authoring surface. The default storage adapter
//! (behind the `postgres` feature) is built on `tokio-postgres`, `deadpool`, and
//! `refinery`.

pub mod backoff;
pub mod context;
pub mod error;
pub mod outcome;
pub mod queue;
pub mod store;
pub mod task;
pub mod worker;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(test)]
mod test_support;

pub use backoff::Backoff;
pub use context::{Context, JournalEntry};
pub use error::Error;
pub use outcome::{Outcome, TaskError};
pub use queue::Queue;
pub use task::{DedupKey, Handler, Merge, Pending, Priority, Task};
pub use worker::{Worker, WorkerBuilder};
