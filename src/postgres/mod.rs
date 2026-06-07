//! The default PostgreSQL storage adapter.
//!
//! [`PostgresStore`] implements [`crate::store::Store`] over a
//! `deadpool_postgres::Pool`. The pool is built by the caller with whatever TLS
//! connector it wants (`NoTls` or a rustls `MakeRustlsConnect`), so the adapter
//! itself is TLS-agnostic. All of the adapter's tables and indexes are named from
//! a configurable prefix, letting several independent queues share one database.

mod migrations;
mod rows;

use crate::error::Error;
use crate::store::{JobRecord, JournalAppend, JournalRecord, NewJob, Settlement, Store};
use async_trait::async_trait;
use deadpool_postgres::Pool;
use rows::{JOB_COLUMNS, JOURNAL_COLUMNS, job_from_row, journal_from_row};
use std::time::Duration;
use tokio_postgres::Client;
use ulid::Ulid;

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

    async fn enqueue(&self, job: &NewJob) -> Result<(), Error> {
        let sql = format!(
            "INSERT INTO {prefix}_jobs \
             (id, kind, payload, priority, status, created_at, visible_at, \
              run_count, failure_count, carry, dedup_key) \
             VALUES ($1, $2, $3, $4, 'pending', $5, $6, 0, 0, $7, $8)",
            prefix = self.prefix,
        );

        let conn = self.pool.get().await?;
        conn.execute(
            &sql,
            &[
                &job.id.to_string(),
                &job.kind,
                &job.payload,
                &job.priority,
                &job.created_at,
                &job.visible_at,
                &job.carry,
                &job.dedup_key,
            ],
        )
        .await?;
        Ok(())
    }

    async fn claim_next(
        &self,
        kinds: &[String],
        priority_floor: i16,
        lease: Duration,
        claimed_by: &str,
    ) -> Result<Option<JobRecord>, Error> {
        // One atomic statement: lock and mark the highest-priority oldest
        // eligible row whose kind we handle, skipping rows another claimer holds.
        // `visible_at <= now()` is the eligibility gate; the lease is stamped in
        // database time so recovery is clock-consistent across hosts.
        let sql = format!(
            "UPDATE {prefix}_jobs SET \
                 status = 'claimed', \
                 claimed_by = $1, \
                 claim_expires_at = now() + interval '1 second' * $2, \
                 run_count = run_count + 1 \
             WHERE id = ( \
                 SELECT id FROM {prefix}_jobs \
                 WHERE status = 'pending' \
                   AND visible_at <= now() \
                   AND kind = ANY($3) \
                   AND priority >= $4 \
                 ORDER BY priority, created_at \
                 LIMIT 1 \
                 FOR UPDATE SKIP LOCKED \
             ) \
             RETURNING {columns}",
            prefix = self.prefix,
            columns = JOB_COLUMNS,
        );

        let conn = self.pool.get().await?;
        let lease_secs = lease.as_secs_f64();
        let row = conn
            .query_opt(&sql, &[&claimed_by, &lease_secs, &kinds, &priority_floor])
            .await?;

        row.as_ref().map(job_from_row).transpose()
    }

    async fn settle(
        &self,
        id: Ulid,
        claimed_by: &str,
        settlement: Settlement,
        journal: JournalAppend,
    ) -> Result<bool, Error> {
        // Every settlement clears the claim columns and is guarded by claim
        // ownership, so a handler cannot settle a job another worker reclaimed.
        // The transition and its journal entry share one transaction, so the
        // journal never records a settlement that did not apply.
        let guard = "WHERE id = $1 AND claimed_by = $2 AND status = 'claimed'";
        let id = id.to_string();

        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let affected = match settlement {
            Settlement::Complete { finished_at } => {
                let sql = format!(
                    "UPDATE {prefix}_jobs SET status = 'completed', finished_at = $3, \
                         claimed_by = NULL, claim_expires_at = NULL {guard}",
                    prefix = self.prefix,
                );
                tx.execute(&sql, &[&id, &claimed_by, &finished_at]).await?
            }
            Settlement::Retry {
                visible_at,
                failure_count,
                carry,
            } => {
                let sql = format!(
                    "UPDATE {prefix}_jobs SET status = 'pending', visible_at = $3, \
                         failure_count = $4, carry = $5, claimed_by = NULL, \
                         claim_expires_at = NULL {guard}",
                    prefix = self.prefix,
                );
                tx.execute(
                    &sql,
                    &[&id, &claimed_by, &visible_at, &failure_count, &carry],
                )
                .await?
            }
            Settlement::Pause { visible_at, carry } => {
                let sql = format!(
                    "UPDATE {prefix}_jobs SET status = 'pending', visible_at = $3, \
                         carry = $4, claimed_by = NULL, claim_expires_at = NULL {guard}",
                    prefix = self.prefix,
                );
                tx.execute(&sql, &[&id, &claimed_by, &visible_at, &carry])
                    .await?
            }
            Settlement::Dead {
                finished_at,
                failure_count,
            } => {
                let sql = format!(
                    "UPDATE {prefix}_jobs SET status = 'dead', finished_at = $3, \
                         failure_count = $4, claimed_by = NULL, claim_expires_at = NULL {guard}",
                    prefix = self.prefix,
                );
                tx.execute(&sql, &[&id, &claimed_by, &finished_at, &failure_count])
                    .await?
            }
            Settlement::Release { visible_at } => {
                let sql = format!(
                    "UPDATE {prefix}_jobs SET status = 'pending', visible_at = $3, \
                         claimed_by = NULL, claim_expires_at = NULL {guard}",
                    prefix = self.prefix,
                );
                tx.execute(&sql, &[&id, &claimed_by, &visible_at]).await?
            }
        };

        // Record the journal entry only when the transition actually applied, so
        // a guard miss leaves no orphan entry.
        if affected > 0 {
            let sql = format!(
                "INSERT INTO {prefix}_journal \
                 (job_id, kind, run_no, recorded_at, outcome, note, attachment) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
                prefix = self.prefix,
            );
            tx.execute(
                &sql,
                &[
                    &id,
                    &journal.kind,
                    &journal.run_no,
                    &journal.recorded_at,
                    &journal.outcome.as_str(),
                    &journal.note,
                    &journal.attachment,
                ],
            )
            .await?;
        }

        tx.commit().await?;
        Ok(affected > 0)
    }

    async fn journal(&self, id: Ulid) -> Result<Vec<JournalRecord>, Error> {
        let sql = format!(
            "SELECT {columns} FROM {prefix}_journal WHERE job_id = $1 ORDER BY id",
            columns = JOURNAL_COLUMNS,
            prefix = self.prefix,
        );
        let conn = self.pool.get().await?;
        let rows = conn.query(&sql, &[&id.to_string()]).await?;
        rows.iter().map(journal_from_row).collect()
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
