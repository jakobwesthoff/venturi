//! A minimal producer: connect, migrate, and enqueue a few jobs.
//!
//! Run a PostgreSQL instance and point `DATABASE_URL` at it, for example:
//!
//! ```text
//! DATABASE_URL='host=localhost user=postgres password=postgres dbname=postgres' \
//!     cargo run --example producer
//! ```
//!
//! Then process the jobs with the `worker` example.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use venturi::postgres::PostgresStore;
use venturi::store::Store;
use venturi::{Queue, Task};

/// The unit of work. Only the producer side (`Task`) is needed to enqueue; the
/// worker example adds the matching `Handler`. The `KIND` string is the contract
/// between the two.
#[derive(Serialize, Deserialize)]
struct SendEmail {
    to: String,
    subject: String,
}

impl Task for SendEmail {
    const KIND: &'static str = "send_email";
    type Carry = ();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "host=localhost user=postgres password=postgres dbname=postgres".to_owned()
    });

    let store = Arc::new(PostgresStore::connect(&dsn, "venturi")?);
    store.migrate().await?;

    let queue = Queue::new(store);
    for i in 0..5 {
        let id = queue
            .enqueue(SendEmail {
                to: format!("user{i}@example.com"),
                subject: "Welcome".to_owned(),
            })
            .await?;
        println!("enqueued send_email {id}");
    }

    Ok(())
}
