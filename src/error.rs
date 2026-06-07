//! Public error types for venturi's queue and storage operations.
//!
//! Everything a producer or worker call can return surfaces as [`Error`], a
//! `thiserror` enum. The crate keeps `anyhow`/`eyre` out of its public API so
//! consumers can match on concrete failure modes. Handler results use a separate
//! [`crate::task::TaskError`] (the execution side); this type is the
//! infrastructure side: pools, the driver, migrations, and serialization.

use thiserror::Error;

/// The error type returned by queue, worker, and storage operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// A database statement failed at the driver level.
    #[error("while talking to PostgreSQL")]
    Database(#[from] tokio_postgres::Error),

    /// A connection could not be obtained from the pool.
    #[error("while acquiring a pooled connection")]
    Pool(#[from] deadpool_postgres::PoolError),

    /// The connection pool could not be built from the given configuration.
    #[error("while building the connection pool")]
    PoolBuild(#[from] deadpool_postgres::BuildError),

    /// Applying the schema migrations failed.
    #[error("while applying migrations")]
    Migration(#[from] refinery_core::Error),

    /// A task payload or carried state could not be (de)serialized to JSON.
    #[error("while (de)serializing a job payload or carry")]
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
}
