//! The default PostgreSQL storage adapter.
//!
//! [`PostgresStore`] implements [`crate::store::Store`] over a
//! `deadpool_postgres::Pool`. The pool is built by the caller with whatever TLS
//! connector it wants (`NoTls` or a rustls `MakeRustlsConnect`), so the adapter
//! itself is TLS-agnostic. All of the adapter's tables and indexes are named from
//! a configurable prefix, letting several independent queues share one database.

mod migrations;

use crate::error::Error;
use crate::store::Store;
use async_trait::async_trait;
use deadpool_postgres::Pool;
use tokio_postgres::Client;

/// The PostgreSQL-backed storage adapter.
///
/// Construct it from a ready connection pool and a table-name prefix with
/// [`PostgresStore::new`], then call [`Store::migrate`] once at startup to apply
/// the schema.
#[derive(Clone)]
pub struct PostgresStore {
    pool: Pool,
    prefix: String,
}

impl PostgresStore {
    /// Build an adapter over `pool`, naming all tables and indexes from `prefix`.
    ///
    /// `prefix` must be a safe, short SQL identifier fragment: it starts with a
    /// lowercase letter, contains only `[a-z0-9_]`, and is at most 39 characters
    /// so every generated identifier (the longest being
    /// `{prefix}_refinery_schema_history`) stays within PostgreSQL's 63-character
    /// limit.
    pub fn new(pool: Pool, prefix: impl Into<String>) -> Result<Self, Error> {
        let prefix = prefix.into();
        validate_prefix(&prefix)?;
        Ok(PostgresStore { pool, prefix })
    }

    /// The configured table-name prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The underlying connection pool, for callers that share it.
    pub fn pool(&self) -> &Pool {
        &self.pool
    }
}

#[async_trait]
impl Store for PostgresStore {
    async fn migrate(&self) -> Result<(), Error> {
        // A dedicated pooled connection drives the migration. A session-level
        // advisory lock keyed on the prefix serializes concurrent startups of
        // workers sharing this queue, so they cannot race to create the schema;
        // a different prefix hashes to a different key and does not contend.
        let mut conn = self.pool.get().await?;
        let lock_id = advisory_lock_id(&self.prefix);

        let client: &mut Client = &mut conn;
        client
            .execute("SELECT pg_advisory_lock($1)", &[&lock_id])
            .await?;

        let result = migrations::apply(client, &self.prefix).await;

        // Release the lock regardless of the migration outcome before surfacing
        // it, so a failed migration does not leave the lock held for the session.
        client
            .execute("SELECT pg_advisory_unlock($1)", &[&lock_id])
            .await?;

        result
    }
}

// =============================================================================
// Prefix validation and the advisory-lock key
// =============================================================================

/// The longest suffix venturi appends to a prefix, `_refinery_schema_history`.
/// A prefix plus this suffix must fit PostgreSQL's 63-character identifier limit.
const LONGEST_SUFFIX_LEN: usize = "_refinery_schema_history".len();

/// Validate a table-name prefix, rejecting anything that could break identifier
/// generation or smuggle SQL into a name.
fn validate_prefix(prefix: &str) -> Result<(), Error> {
    if prefix.is_empty() {
        return Err(Error::Config("table prefix must not be empty".into()));
    }

    let max_prefix_len = 63 - LONGEST_SUFFIX_LEN;
    if prefix.len() > max_prefix_len {
        return Err(Error::Config(format!(
            "table prefix {prefix:?} is too long ({} chars); at most {max_prefix_len} are allowed \
             so every generated identifier stays within PostgreSQL's 63-character limit",
            prefix.len(),
        )));
    }

    let mut chars = prefix.chars();
    let first = chars.next().expect("prefix is non-empty");
    if !first.is_ascii_lowercase() {
        return Err(Error::Config(format!(
            "table prefix {prefix:?} must start with a lowercase ASCII letter"
        )));
    }
    if let Some(bad) = prefix
        .chars()
        .find(|c| !(c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '_'))
    {
        return Err(Error::Config(format!(
            "table prefix {prefix:?} contains the disallowed character {bad:?}; \
             only [a-z0-9_] are permitted"
        )));
    }

    Ok(())
}

/// Derive a stable advisory-lock key from the prefix via FNV-1a.
///
/// Different prefixes must map to different keys so independent queues do not
/// serialize against each other; the same prefix must map to the same key across
/// processes, which a content hash guarantees where a process-seeded hasher would
/// not.
fn advisory_lock_id(prefix: &str) -> i64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET;
    for byte in prefix.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    // Postgres advisory-lock keys are signed 64-bit; reinterpret the bits.
    hash as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_a_reasonable_prefix() {
        assert!(validate_prefix("venturi").is_ok());
        assert!(validate_prefix("jobs_v2").is_ok());
        assert!(validate_prefix("a").is_ok());
    }

    #[test]
    fn rejects_empty_prefix() {
        assert!(validate_prefix("").is_err());
    }

    #[test]
    fn rejects_prefix_with_uppercase_or_symbols() {
        assert!(validate_prefix("Venturi").is_err());
        assert!(validate_prefix("jobs-v2").is_err());
        assert!(validate_prefix("jobs;drop").is_err());
        assert!(validate_prefix("1jobs").is_err());
    }

    #[test]
    fn rejects_overlong_prefix() {
        let max = 63 - LONGEST_SUFFIX_LEN;
        let ok = "a".repeat(max);
        let too_long = "a".repeat(max + 1);
        assert!(validate_prefix(&ok).is_ok());
        assert!(validate_prefix(&too_long).is_err());
    }

    #[test]
    fn advisory_lock_id_is_stable_and_distinct() {
        assert_eq!(advisory_lock_id("venturi"), advisory_lock_id("venturi"));
        assert_ne!(advisory_lock_id("venturi"), advisory_lock_id("other"));
    }
}
