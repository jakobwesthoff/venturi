//! venturi is a durable, PostgreSQL-backed job queue for Rust.
//!
//! Work is modelled as a [`task::Task`]: a plain serializable struct that is both
//! the payload and the identity of a job. Producers enqueue tasks through a
//! [`Queue`](postgres::Queue) handle; workers claim, run, and settle them through
//! a [`Worker`](worker::Worker) that dispatches each job back to its
//! [`task::Handler`].
//!
//! The crate is layered so each layer is usable on its own: storage sits behind
//! the [`store::Store`] backend trait, the worker drives the claim/dispatch loop,
//! and the task layer defines the authoring surface. The default storage adapter
//! (behind the `postgres` feature) is built on `tokio-postgres`, `deadpool`, and
//! `refinery`.

pub mod error;
pub mod store;

#[cfg(feature = "postgres")]
pub mod postgres;

pub use error::Error;
