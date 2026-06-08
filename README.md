# venturi

> Controlled flow from backlog to worker.

**venturi** is a durable, PostgreSQL-backed job queue for Rust, built to be
shared across projects rather than reimplemented per codebase.

A venturi is the narrowed section of a pipe that turns built-up pressure into
controlled, measurable flow. This library does the same for work: jobs
accumulate safely in your database and are released to workers at a rate you
control, with the durability and transactional guarantees Postgres already
gives you.

## Features

- **Durable, at-least-once delivery** on PostgreSQL, claimed with
  `FOR UPDATE SKIP LOCKED` so many workers contend without blocking.
- **Typed tasks.** A job is one serializable struct; the same struct is the
  payload, the dedup identity, and the unit your handler receives.
- **Four outcomes** from a run: complete, cooperative pause (checkpoint and
  resume), retryable failure, or permanent failure.
- **Fibonacci backoff** with deterministic, RNG-free jitter; a per-task or
  worker-level give-up policy.
- **Deduplication** with a candidacy key and a full `merge` decision over the
  existing job's payload, carry, run count, and journal.
- **Reliability:** per-claim leases with automatic stale-claim recovery, and
  cooperative graceful shutdown that drains then releases.
- **Scheduling:** three priority tiers with weighted-slot anti-starvation,
  per-kind concurrency caps, delayed/scheduled jobs, and `LISTEN`/`NOTIFY`
  wakeups with a polling fallback.
- **Operations:** an append-only per-execution journal, a history query, bulk
  cleanup, a live stats snapshot, `tracing` events, and optional `metrics`.

## Installation

```toml
[dependencies]
venturi = "0.1"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

The default `postgres` feature enables the PostgreSQL adapter. The optional
`metrics` feature emits through the vendor-neutral `metrics` facade. The optional
`rustls` feature adds the `connect_rustls` TLS constructor.

## Quick start

Define a task, implement its handler, then enqueue from a producer and process
with a worker.

```rust
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use venturi::postgres::PostgresStore;
use venturi::store::Store;
use venturi::{Context, Handler, Outcome, Queue, Task, TaskError, Worker};

#[derive(Serialize, Deserialize)]
struct SendEmail { to: String, subject: String }

// The producer side: identity and enqueue-time policy.
impl Task for SendEmail {
    const KIND: &'static str = "send_email";
    type Carry = ();
}

// The worker side: the execution logic against shared state `App`.
#[derive(Clone)]
struct App;

impl Handler<App> for SendEmail {
    async fn handle(&self, _ctx: &mut Context<()>, _app: &App) -> Result<Outcome, TaskError> {
        // `?` on any error retries with backoff; `TaskError::permanent(..)` gives up.
        println!("sending {:?} to {}", self.subject, self.to);
        Ok(Outcome::completed())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = "host=localhost user=postgres password=postgres dbname=postgres";
    let store = Arc::new(PostgresStore::connect(dsn, "venturi")?);
    store.migrate().await?;

    // Produce.
    let queue = Queue::new(store.clone());
    queue.enqueue(SendEmail { to: "a@example.com".into(), subject: "Hi".into() }).await?;

    // Consume.
    let worker = Worker::builder(App, store).register::<SendEmail>().build();
    let shutdown = CancellationToken::new();
    worker.run(shutdown).await; // returns when `shutdown` is cancelled
    Ok(())
}
```

Runnable versions live in [`examples/`](examples): `cargo run --example producer`
and `cargo run --example worker` (set `DATABASE_URL`).

## Documentation

- A full walkthrough from first steps to advanced usage: [`docs/guide.md`](docs/guide.md).
- API docs: `cargo doc --open`.

## Development

The project uses [`just`](https://github.com/casey/just):

```text
just ci                # fmt-check, clippy, type-check, fast tests
just integration-test  # database-backed tests (requires Docker)
```

Database-backed tests run against an ephemeral PostgreSQL container and are
marked `#[ignore]`, so the fast `just test` stays quick.

## License

Licensed under the [MIT License](LICENSE).
