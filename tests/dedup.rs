//! Integration tests for deduplication: the four merge decisions, Independent
//! siblings under the non-unique index, and merging into an already-run (paused)
//! job.
//!
//! Requires Docker; marked `#[ignore]`. Run with `just integration-test`.

mod common;

use common::TestDb;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use venturi::store::{JournalOutcome, Store};
use venturi::{
    Context, DedupKey, Handler, Merge, Outcome, Pending, Queue, Task, TaskError, Worker,
};

// Each task type fixes one merge behaviour and shares a constant dedup key so it
// collides with itself.

#[derive(Serialize, Deserialize)]
struct Replacing {
    v: i32,
}
impl Task for Replacing {
    const KIND: &'static str = "replacing";
    type Carry = ();
    fn dedup_key(&self) -> Option<DedupKey> {
        Some(DedupKey::new("k"))
    }
    // Default merge is Replace.
}

#[derive(Serialize, Deserialize)]
struct Keeping {
    v: i32,
}
impl Task for Keeping {
    const KIND: &'static str = "keeping";
    type Carry = ();
    fn dedup_key(&self) -> Option<DedupKey> {
        Some(DedupKey::new("k"))
    }
    fn merge(&self, _existing: &Pending<Self>) -> Merge<Self> {
        Merge::Keep
    }
}

#[derive(Serialize, Deserialize)]
struct Merging {
    v: i32,
}
impl Task for Merging {
    const KIND: &'static str = "merging";
    type Carry = u32;
    fn dedup_key(&self) -> Option<DedupKey> {
        Some(DedupKey::new("k"))
    }
    fn merge(&self, existing: &Pending<Self>) -> Merge<Self> {
        // Union the payloads and advance the carry, continuing the existing work.
        Merge::With {
            task: Merging {
                v: self.v + existing.payload().v,
            },
            carry: existing.carry() + 1,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Indep {
    v: i32,
}
impl Task for Indep {
    const KIND: &'static str = "indep";
    type Carry = ();
    fn dedup_key(&self) -> Option<DedupKey> {
        Some(DedupKey::new("k"))
    }
    fn merge(&self, _existing: &Pending<Self>) -> Merge<Self> {
        Merge::Independent
    }
}

#[derive(Serialize, Deserialize)]
struct Pausing;
impl Task for Pausing {
    const KIND: &'static str = "pausing";
    type Carry = u32;
    fn dedup_key(&self) -> Option<DedupKey> {
        Some(DedupKey::new("k"))
    }
    // Default merge is Replace.
}
impl Handler<()> for Pausing {
    async fn handle(&self, ctx: &mut Context<u32>, _state: &()) -> Result<Outcome, TaskError> {
        // Park the job far in the future so it stays pending (run once) and can be
        // used as a merge candidate.
        *ctx.carry_mut() = 1;
        Ok(Outcome::pause_in(Duration::from_secs(3600)))
    }
}

async fn payload(db: &TestDb, prefix: &str, id: &str) -> serde_json::Value {
    let client = db.pool().get().await.expect("connection");
    let sql = format!("SELECT payload FROM {prefix}_jobs WHERE id = $1");
    client
        .query_one(&sql, &[&id])
        .await
        .expect("payload")
        .get(0)
}

async fn pending_count(db: &TestDb, prefix: &str, kind: &str) -> i64 {
    let client = db.pool().get().await.expect("connection");
    let sql = format!("SELECT count(*) FROM {prefix}_jobs WHERE kind = $1 AND status = 'pending'");
    client
        .query_one(&sql, &[&kind])
        .await
        .expect("count")
        .get(0)
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn replace_supersedes_the_pending_job() {
    let db = TestDb::start().await;
    let queue = Queue::new(Arc::new(db.store("venturi").await));

    let first = queue.enqueue(Replacing { v: 1 }).await.expect("first");
    let second = queue.enqueue(Replacing { v: 2 }).await.expect("second");

    assert_eq!(first, second, "replace reuses the existing row");
    assert_eq!(pending_count(&db, "venturi", "replacing").await, 1);
    assert_eq!(
        payload(&db, "venturi", &first.to_string()).await,
        serde_json::json!({ "v": 2 })
    );

    let store = db.store("venturi").await;
    let journal = store.journal(first).await.expect("journal");
    assert_eq!(journal.len(), 1);
    assert_eq!(journal[0].outcome, JournalOutcome::Merged);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn keep_leaves_the_existing_job_untouched() {
    let db = TestDb::start().await;
    let queue = Queue::new(Arc::new(db.store("venturi").await));

    let first = queue.enqueue(Keeping { v: 1 }).await.expect("first");
    let second = queue.enqueue(Keeping { v: 2 }).await.expect("second");

    assert_eq!(first, second);
    assert_eq!(pending_count(&db, "venturi", "keeping").await, 1);
    assert_eq!(
        payload(&db, "venturi", &first.to_string()).await,
        serde_json::json!({ "v": 1 }),
        "keep retains the original payload"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn with_computes_payload_and_carry() {
    let db = TestDb::start().await;
    let queue = Queue::new(Arc::new(db.store("venturi").await));

    let first = queue.enqueue(Merging { v: 10 }).await.expect("first");
    let second = queue.enqueue(Merging { v: 5 }).await.expect("second");

    assert_eq!(first, second);
    assert_eq!(
        payload(&db, "venturi", &first.to_string()).await,
        serde_json::json!({ "v": 15 }),
        "with unions the payloads"
    );

    let client = db.pool().get().await.expect("connection");
    let row = client
        .query_one(
            "SELECT carry FROM venturi_jobs WHERE id = $1",
            &[&first.to_string()],
        )
        .await
        .expect("carry");
    let carry: serde_json::Value = row.get(0);
    assert_eq!(carry, serde_json::json!(1), "with advances the carry");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn independent_permits_sibling_rows() {
    let db = TestDb::start().await;
    let queue = Queue::new(Arc::new(db.store("venturi").await));

    let first = queue.enqueue(Indep { v: 1 }).await.expect("first");
    let second = queue.enqueue(Indep { v: 2 }).await.expect("second");

    assert_ne!(first, second, "independent inserts a new row");
    assert_eq!(
        pending_count(&db, "venturi", "indep").await,
        2,
        "the non-unique dedup index permits siblings"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn merge_into_a_paused_job_preserves_its_journal() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);
    let queue = Queue::new(store.clone());

    let id = queue.enqueue(Pausing).await.expect("enqueue");

    // Run the job once so it pauses, leaving a run-once pending candidate.
    let worker = Worker::builder((), store.clone())
        .register::<Pausing>()
        .build();
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(worker.run(shutdown.clone()));
    for _ in 0..300 {
        if store
            .journal(id)
            .await
            .expect("journal")
            .iter()
            .any(|e| e.outcome == JournalOutcome::Paused)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    shutdown.cancel();
    handle.await.expect("worker joins");

    // Enqueue again: it merges into the paused candidate rather than inserting.
    let merged = queue.enqueue(Pausing).await.expect("re-enqueue");
    assert_eq!(merged, id, "merged into the existing paused job");

    let client = db.pool().get().await.expect("connection");
    let row = client
        .query_one(
            "SELECT run_count, carry FROM venturi_jobs WHERE id = $1",
            &[&id.to_string()],
        )
        .await
        .expect("row");
    let run_count: i32 = row.get(0);
    let carry: serde_json::Value = row.get(1);
    assert_eq!(
        run_count, 1,
        "the run count from the paused run is preserved"
    );
    assert_eq!(carry, serde_json::json!(0), "replace reset the carry");

    // The journal keeps the pause entry and gains the merge entry.
    let journal = store.journal(id).await.expect("journal");
    let outcomes: Vec<_> = journal.iter().map(|e| e.outcome).collect();
    assert_eq!(
        outcomes,
        vec![JournalOutcome::Paused, JournalOutcome::Merged]
    );
}
