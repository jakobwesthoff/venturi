//! A minimal worker: connect, migrate, and process jobs until Ctrl-C.
//!
//! Run a PostgreSQL instance and point `DATABASE_URL` at it, for example:
//!
//! ```text
//! DATABASE_URL='host=localhost user=postgres password=postgres dbname=postgres' \
//!     cargo run --example worker
//! ```
//!
//! Enqueue work for it with the `producer` example.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use venturi::postgres::PostgresStore;
use venturi::store::Store;
use venturi::{Context, Handler, Outcome, Task, TaskError, Worker};

/// The worker's shared dependencies. A real application would hold an HTTP
/// client, a mailer, database handles, and so on. Here it is empty.
#[derive(Clone)]
struct App;

/// The same task the producer enqueues, identified by the same `KIND`.
#[derive(Serialize, Deserialize)]
struct SendEmail {
    to: String,
    subject: String,
}

impl Task for SendEmail {
    const KIND: &'static str = "send_email";
    type Carry = ();
}

impl Handler<App> for SendEmail {
    async fn handle(&self, _ctx: &mut Context<()>, _app: &App) -> Result<Outcome, TaskError> {
        // Pretend to send the email. Returning an `Err` here would retry with
        // backoff; `TaskError::permanent(..)` would give up immediately.
        println!("sending {:?} to {}", self.subject, self.to);
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(Outcome::completed_with("sent"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "host=localhost user=postgres password=postgres dbname=postgres".to_owned()
    });

    let store = Arc::new(PostgresStore::connect(&dsn, "venturi")?);
    store.migrate().await?;

    let worker = Worker::builder(App, store)
        .register::<SendEmail>()
        .concurrency(8)
        .build();

    // The application owns signal handling and hands venturi a cancellation token.
    let shutdown = CancellationToken::new();
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            shutdown.cancel();
        });
    }

    println!("worker running; press Ctrl-C to stop");
    worker.run(shutdown).await;
    println!("worker stopped");
    Ok(())
}
