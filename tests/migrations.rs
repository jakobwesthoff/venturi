//! Integration tests for schema migration under a prefix.
//!
//! These require Docker and are marked `#[ignore]`; run with
//! `just integration-test`.

mod common;

use common::TestDb;
use venturi::postgres::PostgresStore;
use venturi::store::Store;

/// The harness can stand up a database and round-trip a trivial query.
#[tokio::test]
#[ignore = "requires Docker"]
async fn harness_connects() {
    let db = TestDb::start().await;
    let client = db.pool().get().await.expect("acquire connection");
    let row = client.query_one("SELECT 1", &[]).await.expect("select 1");
    let one: i32 = row.get(0);
    assert_eq!(one, 1);
}

/// Migrating under a prefix creates that prefix's tables, and migrating again is
/// an idempotent no-op.
#[tokio::test]
#[ignore = "requires Docker"]
async fn migrate_creates_prefixed_tables_idempotently() {
    let db = TestDb::start().await;
    let store = PostgresStore::connect(&db.dsn(), "venturi").expect("construct store");

    store.migrate().await.expect("first migration");

    assert!(db.table_exists("venturi_jobs").await);
    assert!(db.table_exists("venturi_journal").await);
    assert!(db.table_exists("venturi_refinery_schema_history").await);

    // Re-applying must be a clean no-op rather than an error.
    store
        .migrate()
        .await
        .expect("second migration is idempotent");
}

/// Two queues with different prefixes coexist in one database, each tracking and
/// creating its own tables independently.
#[tokio::test]
#[ignore = "requires Docker"]
async fn two_prefixes_coexist() {
    let db = TestDb::start().await;

    let alpha = PostgresStore::connect(&db.dsn(), "alpha").expect("construct alpha");
    let beta = PostgresStore::connect(&db.dsn(), "beta").expect("construct beta");

    alpha.migrate().await.expect("migrate alpha");
    beta.migrate().await.expect("migrate beta");

    for table in [
        "alpha_jobs",
        "alpha_journal",
        "alpha_refinery_schema_history",
        "beta_jobs",
        "beta_journal",
        "beta_refinery_schema_history",
    ] {
        assert!(db.table_exists(table).await, "expected {table} to exist");
    }
}
