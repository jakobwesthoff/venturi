//! Prefix-substituted schema migrations driven through refinery.
//!
//! venturi depends on `refinery` with default features off on purpose: refinery's
//! `tokio-postgres` feature would force `native-tls`/`postgres-native-tls` into
//! the dependency tree. Instead we implement refinery's async migration traits
//! ([`AsyncTransaction`], [`AsyncQuery`], [`AsyncMigrate`]) over a borrowed
//! `tokio_postgres::Client`, so the runner works against the same rustls/NoTls
//! connection the rest of the adapter uses.
//!
//! Migrations are authored with the literal `{{prefix}}` token (see
//! `migrations/V1__initial.sql`). At apply time the token is replaced with the
//! configured prefix and the result is run through refinery, with refinery's own
//! migration-history table also named per prefix. Two prefixes in one database
//! therefore track and apply migrations independently.

use crate::error::Error;
use async_trait::async_trait;
use refinery_core::Migration;
use refinery_core::traits::r#async::{AsyncMigrate, AsyncQuery, AsyncTransaction};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio_postgres::Client;
use tokio_postgres::error::Error as PgError;

// =============================================================================
// The migration source
// =============================================================================

/// The ordered set of migration files, embedded at compile time.
///
/// Each entry is `(refinery name, SQL with `{{prefix}}` tokens)`. The name must
/// follow refinery's `V{version}__{description}` convention; the version drives
/// ordering and the applied-history comparison. New versions are appended here.
const MIGRATIONS: &[(&str, &str)] = &[(
    "V1__initial",
    include_str!("../../migrations/V1__initial.sql"),
)];

/// Apply all migrations for `prefix` against a borrowed client.
///
/// The history table is `{prefix}_refinery_schema_history`, isolating this
/// prefix's migration state from any other queue sharing the database.
pub(crate) async fn apply(client: &mut Client, prefix: &str) -> Result<(), Error> {
    // Substitute the prefix into each migration's name and body. The name's
    // prefix keeps refinery's recorded migration identities distinct per queue,
    // matching the per-prefix history table.
    let migrations = MIGRATIONS
        .iter()
        .map(|(name, sql)| {
            let body = sql.replace("{{prefix}}", prefix);
            Migration::unapplied(name, &body)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let history_table = format!("{prefix}_refinery_schema_history");

    let mut runner = refinery_core::Runner::new(&migrations);
    runner.set_migration_table_name(&history_table);

    let mut conn = PgMigrationClient(client);
    runner.run_async(&mut conn).await?;
    Ok(())
}

// =============================================================================
// refinery async-trait bridge over a borrowed tokio_postgres::Client
// =============================================================================

/// Adapts a borrowed `tokio_postgres::Client` to refinery's async migration
/// traits without pulling in refinery's native-tls-bound postgres feature.
struct PgMigrationClient<'a>(&'a mut Client);

#[async_trait]
impl AsyncTransaction for PgMigrationClient<'_> {
    type Error = PgError;

    /// Run a batch of statements in one transaction, committing atomically.
    ///
    /// refinery hands us each migration's DDL plus the bookkeeping insert that
    /// records it as applied; grouping them in a single transaction is what makes
    /// an interrupted migration leave no half-applied version behind.
    async fn execute<'a, T: Iterator<Item = &'a str> + Send>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let transaction = self.0.transaction().await?;
        let mut count = 0;
        for query in queries {
            transaction.batch_execute(query).await?;
            count += 1;
        }
        transaction.commit().await?;
        Ok(count)
    }
}

#[async_trait]
impl AsyncQuery<Vec<Migration>> for PgMigrationClient<'_> {
    /// Read the applied-migration history refinery uses to decide what is pending.
    async fn query(
        &mut self,
        query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncTransaction>::Error> {
        let transaction = self.0.transaction().await?;
        let rows = transaction.query(query, &[]).await?;
        transaction.commit().await?;

        let mut applied = Vec::with_capacity(rows.len());
        for row in rows {
            // Column layout of refinery's history table: version, name,
            // applied_on (RFC 3339 text), checksum (decimal u64 text).
            let version: i32 = row.get(0);
            let name: String = row.get(1);
            let applied_on: String = row.get(2);
            let applied_on = OffsetDateTime::parse(&applied_on, &Rfc3339)
                .expect("refinery records applied_on in RFC 3339");
            let checksum: String = row.get(3);
            let checksum = checksum
                .parse::<u64>()
                .expect("refinery records checksum as a decimal u64");

            applied.push(Migration::applied(version, name, applied_on, checksum));
        }
        Ok(applied)
    }
}

impl AsyncMigrate for PgMigrationClient<'_> {}
