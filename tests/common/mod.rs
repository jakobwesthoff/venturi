//! Shared integration-test harness: an ephemeral PostgreSQL container plus a
//! `deadpool` pool wired to it.
//!
//! Every database-backed test spins up its own container through
//! [`TestDb::start`]. Tests isolate from each other by table prefix, so a single
//! container can host many independent queues; callers pass a distinct prefix per
//! [`crate::common`]-built [`PostgresStore`]. All such tests are marked
//! `#[ignore]` so the fast `cargo test` run skips them; `just integration-test`
//! runs them with `--ignored` (Docker required).
//!
//! Some helpers here are used by only a subset of the integration-test binaries,
//! so individual items may read as dead code when compiling one binary in
//! isolation; the harness is shared, hence the module-level allow.
#![allow(dead_code)]

use deadpool_postgres::{Config, Pool, Runtime};
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;
use venturi::postgres::PostgresStore;
use venturi::store::Store;

/// A running PostgreSQL container and a pool connected to it.
///
/// The container is torn down when this value is dropped, so a test must keep it
/// alive for as long as it uses the pool.
pub struct TestDb {
    // Held only to keep the container running for the lifetime of the pool.
    _container: ContainerAsync<Postgres>,
    pool: Pool,
    host: String,
    port: u16,
}

impl TestDb {
    /// Start a fresh PostgreSQL container and build a connection pool for it.
    pub async fn start() -> TestDb {
        let container = Postgres::default()
            .start()
            .await
            .expect("start postgres test container");

        let host = container
            .get_host()
            .await
            .expect("resolve container host")
            .to_string();
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("resolve container port");

        // The default `postgres` image uses these well-known credentials.
        let mut config = Config::new();
        config.host = Some(host.clone());
        config.port = Some(port);
        config.user = Some("postgres".to_owned());
        config.password = Some("postgres".to_owned());
        config.dbname = Some("postgres".to_owned());

        let pool = config
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .expect("build connection pool");

        TestDb {
            _container: container,
            pool,
            host,
            port,
        }
    }

    /// The connection pool for this container.
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// A `tokio_postgres` connection string for this container, for the
    /// `LISTEN`-based notifier.
    pub fn dsn(&self) -> String {
        format!(
            "host={} port={} user=postgres password=postgres dbname=postgres",
            self.host, self.port
        )
    }

    /// Build a migrated [`PostgresStore`] over this container under `prefix`.
    pub async fn store(&self, prefix: &str) -> PostgresStore {
        let store =
            PostgresStore::connect(&self.dsn(), prefix).expect("construct store with prefix");
        store.migrate().await.expect("apply migrations");
        store
    }

    /// Whether a table exists in the connected database.
    pub async fn table_exists(&self, table: &str) -> bool {
        let client = self.pool.get().await.expect("acquire connection");
        let row = client
            .query_one("SELECT to_regclass($1) IS NOT NULL", &[&table])
            .await
            .expect("query table existence");
        row.get(0)
    }
}
