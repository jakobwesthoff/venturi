//! Public error types for venturi's queue and storage operations.
//!
//! Everything a producer or worker call can return surfaces as [`enum@Error`], a
//! `thiserror` enum. The crate keeps `anyhow`/`eyre` out of its public API so
//! consumers can match on concrete failure modes. Handler results use a separate
//! [`crate::outcome::TaskError`] (the execution side); this type is the
//! infrastructure side: pools, the driver, migrations, and serialization.

use thiserror::Error;

/// The error type returned by queue, worker, and storage operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// A database statement failed at the driver level.
    #[error("while talking to PostgreSQL: {0}")]
    Database(#[from] tokio_postgres::Error),

    /// A connection could not be obtained from the pool.
    #[error("while acquiring a pooled connection: {0}")]
    Pool(#[from] deadpool_postgres::PoolError),

    /// The connection pool could not be built from the given configuration.
    #[error("while building the connection pool: {0}")]
    PoolBuild(#[from] deadpool_postgres::BuildError),

    /// Applying the schema migrations failed.
    #[error("while applying migrations: {0}")]
    Migration(#[from] refinery_core::Error),

    /// A task payload or carried state could not be (de)serialized to JSON.
    #[error("while (de)serializing a job payload or carry: {0}")]
    Serialization(#[from] serde_json::Error),

    /// A stored job referenced a `kind` that is not registered with this worker.
    #[error("no handler registered for job kind {kind:?}")]
    UnknownKind {
        /// The unregistered discriminator read from storage.
        kind: String,
    },

    /// A configuration value was invalid (for example an empty table prefix).
    #[error("invalid configuration: {0}")]
    Config(String),

    /// A job was enqueued with a priority tier outside the supported `0..=2`
    /// range. The typed [`crate::Queue`] path cannot produce this; it guards
    /// direct [`crate::Store`] users who build a `NewJob` by hand.
    #[error("invalid priority tier {priority}: expected 0 (high), 1 (normal), or 2 (low)")]
    InvalidPriority {
        /// The out-of-range tier that was supplied.
        priority: i16,
    },

    /// A run number exceeded the signed `integer` range the storage column holds.
    /// Run numbers originate from the database and only increment, so this cannot
    /// arise on the normal path; it guards the storage boundary against a
    /// hand-built run number above `i32::MAX` rather than letting it wrap to a
    /// negative on the way to the database.
    #[error("run number {run_no} exceeds the storable range")]
    RunNumberOutOfRange {
        /// The run number that did not fit the storage column.
        run_no: u32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_includes_the_underlying_source() {
        // The driver/serde variants carry their detail in `source()`; the worker
        // logs and the dead-job journal note render `Display`, so `Display` must
        // surface the underlying message rather than the bare context.
        let serde_err = serde_json::from_str::<i32>("not a number").unwrap_err();
        let source_text = serde_err.to_string();
        let shown = Error::from(serde_err).to_string();

        assert!(
            shown.contains("(de)serializing"),
            "keeps the context: {shown}"
        );
        assert!(
            shown.contains(&source_text),
            "includes the source detail {source_text:?}: {shown}"
        );
    }
}
