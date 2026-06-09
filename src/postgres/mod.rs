//! The default PostgreSQL storage adapter.
//!
//! [`PostgresStore`] implements [`crate::store::Store`] over a
//! `deadpool_postgres::Pool`. The adapter owns its connection parameters (a
//! `tokio_postgres::Config` and a TLS connector) and builds two things from them:
//! the work pool that serves every query, and the dedicated `LISTEN` connection
//! that delivers push wakeups. Owning both means the listener always uses the same
//! TLS path as the pool, so push wakeups work for every deployment, plaintext or
//! TLS, with no separate endpoint and no opt-in. All of the adapter's tables and
//! indexes are named from a configurable prefix, letting several independent
//! queues share one database.

mod migrations;
mod notify;
mod rows;

use crate::error::Error;
use crate::store::{
    CleanupCriteria, HistoryFilter, JobRecord, JournalAppend, JournalRecord, MergePayload, NewJob,
    Notifier, Settlement, Snapshot, Store,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use notify::{ListenFactory, PgNotifier, make_listen_factory};
use rows::{JOB_COLUMNS, JOURNAL_COLUMNS, job_from_row, journal_from_row};
use std::time::Duration;
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls, Socket};
use ulid::Ulid;

/// The PostgreSQL-backed storage adapter.
///
/// Construct it from connection parameters and a table-name prefix with
/// [`PostgresStore::connect`] (plaintext), [`PostgresStore::connect_rustls`] (TLS),
/// or [`PostgresStore::from_config`] (any connector), then call [`Store::migrate`]
/// once at startup to apply the schema. The dedicated `LISTEN` connection that
/// delivers push wakeups is built from the same parameters, so listening is always
/// on and always matches the pool's TLS.
#[derive(Clone)]
pub struct PostgresStore {
    pool: Pool,
    prefix: String,
    // Builds the dedicated LISTEN connection on demand, carrying the same
    // connection config and TLS connector the pool was built with.
    listen_factory: ListenFactory,
}

impl PostgresStore {
    /// Build an adapter from a `tokio_postgres::Config` and a TLS connector.
    ///
    /// This is the general constructor the convenience ones delegate to. The store
    /// builds its own work pool and its dedicated `LISTEN` connection from the same
    /// `config` and `tls`, so push wakeups use the same TLS path as every query.
    /// Neither connection is opened here; the pool connects lazily and the listener
    /// connects when a worker first asks for notifications.
    ///
    /// `prefix` must be a safe, short SQL identifier fragment: it starts with a
    /// lowercase letter, contains only `[a-z0-9_]`, and is at most 39 characters so
    /// every generated identifier (the longest being
    /// `{prefix}_refinery_schema_history`) stays within PostgreSQL's 63-character
    /// limit.
    pub fn from_config<T>(
        config: tokio_postgres::Config,
        tls: T,
        prefix: impl Into<String>,
    ) -> Result<Self, Error>
    where
        T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
        T::Stream: Send + Sync,
        T::TlsConnect: Send + Sync,
        <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        let prefix = prefix.into();
        validate_prefix(&prefix)?;
        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = Manager::from_config(config.clone(), tls.clone(), manager_config);
        let pool = Pool::builder(manager).build()?;
        let listen_factory = make_listen_factory(config, tls);
        Ok(PostgresStore {
            pool,
            prefix,
            listen_factory,
        })
    }

    /// Build an adapter by connecting to `dsn` without TLS.
    ///
    /// The common local case: it builds the `deadpool` pool and the listener
    /// internally with `NoTls`, so a consumer needs no direct
    /// `deadpool`/`tokio_postgres` dependency. `dsn` is a standard
    /// libpq/`tokio_postgres` connection string. For TLS, use
    /// [`PostgresStore::connect_rustls`] or [`PostgresStore::from_config`].
    pub fn connect(dsn: &str, prefix: impl Into<String>) -> Result<Self, Error> {
        let config: tokio_postgres::Config = dsn.parse()?;
        PostgresStore::from_config(config, NoTls, prefix)
    }

    /// Build an adapter by connecting to `dsn` over TLS with a rustls connector.
    ///
    /// The TLS counterpart to [`PostgresStore::connect`]: it wraps `client_config`
    /// in a `tokio_postgres_rustls` connector and builds the pool and the listener
    /// from it, so a TLS deployment gets push wakeups over the same encrypted path
    /// with no plaintext endpoint. Requires the `rustls` feature.
    #[cfg(feature = "rustls")]
    pub fn connect_rustls(
        dsn: &str,
        prefix: impl Into<String>,
        client_config: rustls::ClientConfig,
    ) -> Result<Self, Error> {
        let config: tokio_postgres::Config = dsn.parse()?;
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(client_config);
        PostgresStore::from_config(config, tls, prefix)
    }

    /// Bound the work pool to at most `max_size` connections.
    ///
    /// Construction leaves the pool at `deadpool`'s default size. Chain this to
    /// cap it — for example to divide a database's connection budget across
    /// several stores sharing it. Only the work pool is affected; the dedicated
    /// `LISTEN` connection is separate. The pool connects lazily, so this sets
    /// the ceiling the pool grows to under load rather than opening anything now.
    #[must_use]
    pub fn with_max_pool_size(self, max_size: usize) -> Self {
        self.pool.resize(max_size);
        self
    }

    /// The configured table-name prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The underlying connection pool, for callers that share it.
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// The `NOTIFY`/`LISTEN` channel for this prefix.
    fn channel(&self) -> String {
        format!("{}_jobs", self.prefix)
    }

    /// Queue a wakeup on this queue's channel within `tx`.
    ///
    /// Issued inside the caller's transaction so the NOTIFY is delivered on commit
    /// and discarded on rollback, keeping the wakeup atomic with the row change
    /// that produced it. The payload is empty: a woken worker re-queries its
    /// claimable set rather than acting on the message contents.
    async fn notify(&self, tx: &tokio_postgres::Transaction<'_>) -> Result<(), Error> {
        tx.execute("SELECT pg_notify($1, '')", &[&self.channel()])
            .await?;
        Ok(())
    }
}

/// Whether a settlement makes its job claimable again and so must wake workers.
///
/// Re-pending transitions (a retry, a pause's resume, a release back to the pool)
/// return a job to the claimable set and need a wakeup; terminal ones (completed,
/// dead) produce no claimable work and must not.
fn notifies_on_repend(settlement: &Settlement) -> bool {
    match settlement {
        Settlement::Retry { .. } | Settlement::Pause { .. } | Settlement::Release { .. } => true,
        Settlement::Complete { .. } | Settlement::Dead { .. } => false,
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

        // Release the lock regardless of the migration outcome, so a failed
        // migration does not leave it held for the session.
        let unlock = client
            .execute("SELECT pg_advisory_unlock($1)", &[&lock_id])
            .await;

        // The migration outcome is what callers care about. The advisory lock is
        // session-scoped and released when the connection closes, so an unlock
        // that fails after a successful migration is harmless: log it rather than
        // turning a completed migration into a reported failure.
        result?;
        if let Err(error) = unlock {
            tracing::warn!(
                %error,
                "advisory unlock after migration failed; the lock releases on connection close",
            );
        }
        Ok(())
    }

    async fn enqueue(&self, job: &NewJob) -> Result<(), Error> {
        let sql = format!(
            "INSERT INTO {prefix}_jobs \
             (id, kind, payload, priority, status, created_at, visible_at, \
              run_count, failure_count, carry, dedup_key) \
             VALUES ($1, $2, $3, $4, 'pending', $5, $6, 0, 0, $7, $8)",
            prefix = self.prefix,
        );

        // Insert and wake in one transaction so the NOTIFY is delivered exactly
        // when the row commits: a rollback discards both, and no committed row is
        // ever left without its wakeup.
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        tx.execute(
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
        self.notify(&tx).await?;
        tx.commit().await?;
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
        run_no: i32,
        settlement: Settlement,
        journal: JournalAppend,
    ) -> Result<bool, Error> {
        // Every settlement clears the claim columns and is guarded by claim
        // ownership at the claim epoch (`run_count`), so a handler cannot settle a
        // job that was reclaimed and re-run — even under the same worker identity.
        // The transition and its journal entry share one transaction, so the
        // journal never records a settlement that did not apply.
        let guard = "WHERE id = $1 AND claimed_by = $2 AND run_count = $3 AND status = 'claimed'";
        let id = id.to_string();
        // Classify before the match below consumes `settlement`.
        let repends = notifies_on_repend(&settlement);

        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let affected = match settlement {
            Settlement::Complete { finished_at } => {
                let sql = format!(
                    "UPDATE {prefix}_jobs SET status = 'completed', finished_at = $4, \
                         claimed_by = NULL, claim_expires_at = NULL {guard}",
                    prefix = self.prefix,
                );
                tx.execute(&sql, &[&id, &claimed_by, &run_no, &finished_at])
                    .await?
            }
            Settlement::Retry {
                visible_at,
                failure_count,
                carry,
            } => {
                let sql = format!(
                    "UPDATE {prefix}_jobs SET status = 'pending', visible_at = $4, \
                         failure_count = $5, carry = $6, claimed_by = NULL, \
                         claim_expires_at = NULL {guard}",
                    prefix = self.prefix,
                );
                tx.execute(
                    &sql,
                    &[&id, &claimed_by, &run_no, &visible_at, &failure_count, &carry],
                )
                .await?
            }
            Settlement::Pause { visible_at, carry } => {
                let sql = format!(
                    "UPDATE {prefix}_jobs SET status = 'pending', visible_at = $4, \
                         carry = $5, claimed_by = NULL, claim_expires_at = NULL {guard}",
                    prefix = self.prefix,
                );
                tx.execute(&sql, &[&id, &claimed_by, &run_no, &visible_at, &carry])
                    .await?
            }
            Settlement::Dead {
                finished_at,
                failure_count,
            } => {
                let sql = format!(
                    "UPDATE {prefix}_jobs SET status = 'dead', finished_at = $4, \
                         failure_count = $5, claimed_by = NULL, claim_expires_at = NULL {guard}",
                    prefix = self.prefix,
                );
                tx.execute(
                    &sql,
                    &[&id, &claimed_by, &run_no, &finished_at, &failure_count],
                )
                .await?
            }
            Settlement::Release { visible_at } => {
                let sql = format!(
                    "UPDATE {prefix}_jobs SET status = 'pending', visible_at = $4, \
                         claimed_by = NULL, claim_expires_at = NULL {guard}",
                    prefix = self.prefix,
                );
                tx.execute(&sql, &[&id, &claimed_by, &run_no, &visible_at])
                    .await?
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

        // A re-pending settlement returns the job to the claimable set; wake
        // workers within the same transaction so the notify commits with it.
        if affected > 0 && repends {
            self.notify(&tx).await?;
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

    async fn next_visible_at(&self, kinds: &[String]) -> Result<Option<DateTime<Utc>>, Error> {
        let sql = format!(
            "SELECT min(visible_at) FROM {prefix}_jobs \
             WHERE status = 'pending' AND kind = ANY($1) AND visible_at > now()",
            prefix = self.prefix,
        );
        let conn = self.pool.get().await?;
        let row = conn.query_one(&sql, &[&kinds]).await?;
        Ok(row.get(0))
    }

    async fn notifier(&self) -> Result<Box<dyn Notifier>, Error> {
        let notifier = PgNotifier::connect(self.listen_factory.clone(), self.channel()).await?;
        Ok(Box::new(notifier))
    }

    async fn find_stale(&self) -> Result<Vec<JobRecord>, Error> {
        // Bound the batch so a large backlog of expired claims is recovered over
        // several ticks rather than in one oversized statement.
        let sql = format!(
            "SELECT {columns} FROM {prefix}_jobs \
             WHERE status = 'claimed' AND claim_expires_at < now() \
             ORDER BY claim_expires_at \
             LIMIT 100",
            columns = JOB_COLUMNS,
            prefix = self.prefix,
        );
        let conn = self.pool.get().await?;
        let rows = conn.query(&sql, &[]).await?;
        rows.iter().map(job_from_row).collect()
    }

    async fn recover(
        &self,
        id: Ulid,
        visible_at: DateTime<Utc>,
        failure_count: i32,
        journal: JournalAppend,
    ) -> Result<bool, Error> {
        let id = id.to_string();
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;

        let update = format!(
            "UPDATE {prefix}_jobs SET status = 'pending', visible_at = $2, \
                 failure_count = $3, claimed_by = NULL, claim_expires_at = NULL \
             WHERE id = $1 AND status = 'claimed' AND claim_expires_at < now()",
            prefix = self.prefix,
        );
        let affected = tx
            .execute(&update, &[&id, &visible_at, &failure_count])
            .await?;

        if affected > 0 {
            let insert = format!(
                "INSERT INTO {prefix}_journal \
                 (job_id, kind, run_no, recorded_at, outcome, note, attachment) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
                prefix = self.prefix,
            );
            tx.execute(
                &insert,
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

        // Recovery re-pends the abandoned job, so wake workers within the same
        // transaction.
        if affected > 0 {
            self.notify(&tx).await?;
        }

        tx.commit().await?;
        Ok(affected > 0)
    }

    async fn extend_lease(
        &self,
        id: Ulid,
        claimed_by: &str,
        run_no: i32,
        lease: Duration,
    ) -> Result<bool, Error> {
        let sql = format!(
            "UPDATE {prefix}_jobs SET claim_expires_at = now() + interval '1 second' * $4 \
             WHERE id = $1 AND claimed_by = $2 AND run_count = $3 AND status = 'claimed'",
            prefix = self.prefix,
        );
        let conn = self.pool.get().await?;
        let affected = conn
            .execute(
                &sql,
                &[&id.to_string(), &claimed_by, &run_no, &lease.as_secs_f64()],
            )
            .await?;
        Ok(affected > 0)
    }

    async fn query_jobs(&self, filter: &HistoryFilter) -> Result<Vec<JobRecord>, Error> {
        // Build the WHERE clause from whichever fields are set, binding each as a
        // positional parameter so nothing is interpolated into the SQL.
        let status = filter.status.map(|s| s.as_str());
        // The cursor id is compared against the text `id` column, so render it to
        // its ULID string here and keep it alive for the duration of the query.
        let cursor_id = filter.created_before.as_ref().map(|(_, id)| id.to_string());
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        let mut clauses: Vec<String> = Vec::new();

        if let Some(kind) = &filter.kind {
            params.push(kind);
            clauses.push(format!("kind = ${}", params.len()));
        }
        if let Some(status) = &status {
            params.push(status);
            clauses.push(format!("status = ${}", params.len()));
        }
        if let Some(since) = &filter.finished_since {
            params.push(since);
            clauses.push(format!("finished_at >= ${}", params.len()));
        }
        if let Some(until) = &filter.finished_until {
            params.push(until);
            clauses.push(format!("finished_at < ${}", params.len()));
        }
        // Keyset bound: a row-value comparison against the same `(created_at, id)`
        // the result is ordered by, so a page is every row strictly older than the
        // cursor. `(a, b) < ($1, $2)` is true when `a < $1`, or `a = $1 AND b < $2`
        // — exactly the descending-order successor condition, with `id` breaking
        // ties on equal `created_at`.
        if let Some((created_at, _)) = &filter.created_before {
            params.push(created_at);
            let ts_index = params.len();
            let id = cursor_id
                .as_ref()
                .expect("cursor_id is Some whenever created_before is Some");
            params.push(id);
            let id_index = params.len();
            clauses.push(format!("(created_at, id) < (${ts_index}, ${id_index})"));
        }

        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };
        let limit_clause = match &filter.limit {
            Some(limit) => {
                params.push(limit);
                format!("LIMIT ${}", params.len())
            }
            None => String::new(),
        };

        let sql = format!(
            "SELECT {columns} FROM {prefix}_jobs {where_clause} \
             ORDER BY created_at DESC, id DESC {limit_clause}",
            columns = JOB_COLUMNS,
            prefix = self.prefix,
        );

        let conn = self.pool.get().await?;
        let rows = conn.query(&sql, &params).await?;
        rows.iter().map(job_from_row).collect()
    }

    async fn cleanup(&self, criteria: &CleanupCriteria) -> Result<u64, Error> {
        // `finished_at < $1` already restricts to terminal jobs, since only
        // completed/dead rows have it set.
        let status = criteria.status.map(|s| s.as_str());
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![&criteria.finished_before];
        let mut clauses = vec![
            "finished_at IS NOT NULL".to_owned(),
            "finished_at < $1".to_owned(),
        ];

        if let Some(kind) = &criteria.kind {
            params.push(kind);
            clauses.push(format!("kind = ${}", params.len()));
        }
        if let Some(status) = &status {
            params.push(status);
            clauses.push(format!("status = ${}", params.len()));
        }

        let sql = format!(
            "DELETE FROM {prefix}_jobs WHERE {}",
            clauses.join(" AND "),
            prefix = self.prefix,
        );
        let conn = self.pool.get().await?;
        Ok(conn.execute(&sql, &params).await?)
    }

    async fn stats(&self) -> Result<Snapshot, Error> {
        let conn = self.pool.get().await?;
        let mut snapshot = Snapshot::default();

        // Pending backlog and oldest age per kind in one grouped pass.
        let pending_sql = format!(
            "SELECT kind, count(*), min(created_at) FROM {prefix}_jobs \
             WHERE status = 'pending' GROUP BY kind",
            prefix = self.prefix,
        );
        let now = Utc::now();
        for row in conn.query(&pending_sql, &[]).await? {
            let kind: String = row.get(0);
            let count: i64 = row.get(1);
            let oldest: DateTime<Utc> = row.get(2);
            snapshot
                .pending_by_kind
                .insert(kind.clone(), count.max(0) as u64);
            let age = (now - oldest).to_std().unwrap_or(Duration::ZERO);
            snapshot.oldest_pending_age.insert(kind, age);
        }

        let claimed_sql = format!(
            "SELECT count(*) FROM {prefix}_jobs WHERE status = 'claimed'",
            prefix = self.prefix,
        );
        let claimed: i64 = conn.query_one(&claimed_sql, &[]).await?.get(0);
        snapshot.claimed = claimed.max(0) as u64;

        let dead_sql = format!(
            "SELECT kind, count(*) FROM {prefix}_jobs WHERE status = 'dead' GROUP BY kind",
            prefix = self.prefix,
        );
        for row in conn.query(&dead_sql, &[]).await? {
            let kind: String = row.get(0);
            let count: i64 = row.get(1);
            snapshot.dead_by_kind.insert(kind, count.max(0) as u64);
        }

        Ok(snapshot)
    }

    async fn dedup_candidate(
        &self,
        kind: &str,
        dedup_key: &str,
    ) -> Result<Option<JobRecord>, Error> {
        let sql = format!(
            "SELECT {columns} FROM {prefix}_jobs \
             WHERE kind = $1 AND dedup_key = $2 AND status = 'pending' \
             ORDER BY created_at \
             LIMIT 1",
            columns = JOB_COLUMNS,
            prefix = self.prefix,
        );
        let conn = self.pool.get().await?;
        let row = conn.query_opt(&sql, &[&kind, &dedup_key]).await?;
        row.as_ref().map(job_from_row).transpose()
    }

    async fn merge_into(
        &self,
        id: Ulid,
        update: Option<MergePayload>,
        journal: JournalAppend,
    ) -> Result<bool, Error> {
        let id = id.to_string();
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;

        // Guard on the candidate still being pending. A Replace/With writes the
        // new payload and carry; a Keep self-assigns `dedup_key` so the same
        // guarded statement reports whether the candidate is still mergeable
        // without changing it.
        let affected = match update {
            Some(MergePayload {
                payload,
                carry,
                priority,
            }) => {
                let sql = format!(
                    "UPDATE {prefix}_jobs SET payload = $2, carry = $3, priority = $4 \
                     WHERE id = $1 AND status = 'pending'",
                    prefix = self.prefix,
                );
                tx.execute(&sql, &[&id, &payload, &carry, &priority])
                    .await?
            }
            None => {
                let sql = format!(
                    "UPDATE {prefix}_jobs SET dedup_key = dedup_key \
                     WHERE id = $1 AND status = 'pending'",
                    prefix = self.prefix,
                );
                tx.execute(&sql, &[&id]).await?
            }
        };

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

    #[test]
    fn repending_settlements_notify_and_terminal_ones_do_not() {
        let now = Utc::now();
        let carry = serde_json::Value::Null;

        // Re-pending: the job returns to the claimable set and workers must wake.
        assert!(notifies_on_repend(&Settlement::Retry {
            visible_at: now,
            failure_count: 1,
            carry: carry.clone(),
        }));
        assert!(notifies_on_repend(&Settlement::Pause {
            visible_at: now,
            carry,
        }));
        assert!(notifies_on_repend(&Settlement::Release { visible_at: now }));

        // Terminal: no claimable work is produced, so no wakeup.
        assert!(!notifies_on_repend(&Settlement::Complete {
            finished_at: now
        }));
        assert!(!notifies_on_repend(&Settlement::Dead {
            finished_at: now,
            failure_count: 3,
        }));
    }

    // The DSN never connects in these tests: `connect` only parses it and builds
    // the lazy pool, so the pool's configured `max_size` is observable offline.
    const TEST_DSN: &str = "host=localhost user=postgres dbname=postgres";

    #[test]
    fn with_max_pool_size_sets_exactly_the_requested_cap() {
        let store = PostgresStore::connect(TEST_DSN, "venturi")
            .expect("construct store")
            .with_max_pool_size(3);
        assert_eq!(store.pool().status().max_size, 3);

        // A second value confirms the cap tracks the argument, not a constant.
        let store = PostgresStore::connect(TEST_DSN, "venturi")
            .expect("construct store")
            .with_max_pool_size(7);
        assert_eq!(store.pool().status().max_size, 7);
    }

    #[test]
    fn default_pool_size_is_unchanged_without_the_knob() {
        let default = PostgresStore::connect(TEST_DSN, "venturi")
            .expect("construct store")
            .pool()
            .status()
            .max_size;
        assert!(default > 0, "deadpool provides a positive default max size");

        // The knob moves the cap; omitting it leaves the library default intact.
        let capped = PostgresStore::connect(TEST_DSN, "venturi")
            .expect("construct store")
            .with_max_pool_size(default + 1);
        assert_eq!(capped.pool().status().max_size, default + 1);

        let untouched = PostgresStore::connect(TEST_DSN, "venturi").expect("construct store");
        assert_eq!(untouched.pool().status().max_size, default);
    }
}
