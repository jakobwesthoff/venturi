//! End-to-end integration tests for the enqueue/claim/run/complete cycle.
//!
//! Requires Docker; marked `#[ignore]`. Run with `just integration-test`.

mod common;

use common::TestDb;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use venturi::{Context, Handler, Outcome, Queue, Task, TaskError, Worker};

/// Shared worker state: records which task indices ran and flags duplicates.
#[derive(Default)]
struct Recorder {
    seen: Mutex<HashSet<i32>>,
    duplicates: AtomicUsize,
    ran: AtomicUsize,
}

#[derive(Serialize, Deserialize)]
struct Mark {
    index: i32,
}

impl Task for Mark {
    const KIND: &'static str = "mark";
    type Carry = ();
}

impl Handler<Arc<Recorder>> for Mark {
    async fn handle(
        &self,
        _ctx: &mut Context<()>,
        state: &Arc<Recorder>,
    ) -> Result<Outcome, TaskError> {
        let fresh = state
            .seen
            .lock()
            .expect("lock not poisoned")
            .insert(self.index);
        if !fresh {
            state.duplicates.fetch_add(1, Ordering::SeqCst);
        }
        state.ran.fetch_add(1, Ordering::SeqCst);
        Ok(Outcome::completed())
    }
}

/// Count rows of a job kind in a given status, queried directly.
async fn status_count(db: &TestDb, prefix: &str, status: &str) -> i64 {
    let client = db.pool().get().await.expect("connection");
    let sql = format!("SELECT count(*) FROM {prefix}_jobs WHERE status = $1");
    let row = client
        .query_one(&sql, &[&status])
        .await
        .expect("count query");
    row.get(0)
}

async fn wait_until(mut cond: impl FnMut() -> bool) {
    for _ in 0..300 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("condition not met within the deadline");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn enqueue_claim_run_complete() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);
    let queue = Queue::new(store.clone());

    let id = queue.enqueue(Mark { index: 1 }).await.expect("enqueue");

    let recorder = Arc::new(Recorder::default());
    let worker = Worker::builder(recorder.clone(), store.clone())
        .register::<Mark>()
        .build();

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(worker.run(shutdown.clone()));

    wait_until(|| recorder.ran.load(Ordering::SeqCst) == 1).await;
    shutdown.cancel();
    handle.await.expect("worker joins");

    assert_eq!(status_count(&db, "venturi", "completed").await, 1);
    assert_eq!(status_count(&db, "venturi", "pending").await, 0);

    // The enqueue returned the id the job was stored under.
    let client = db.pool().get().await.expect("connection");
    let row = client
        .query_one(
            "SELECT status FROM venturi_jobs WHERE id = $1",
            &[&id.to_string()],
        )
        .await
        .expect("status query");
    let status: String = row.get(0);
    assert_eq!(status, "completed");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn concurrent_workers_never_double_claim() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);
    let queue = Queue::new(store.clone());

    const JOBS: i32 = 40;
    for index in 0..JOBS {
        queue.enqueue(Mark { index }).await.expect("enqueue");
    }

    let recorder = Arc::new(Recorder::default());
    let shutdown = CancellationToken::new();

    // Two independent worker loops over the same queue. SKIP LOCKED must keep
    // them from claiming the same row.
    let mut handles = Vec::new();
    for _ in 0..2 {
        let worker = Worker::builder(recorder.clone(), store.clone())
            .register::<Mark>()
            .concurrency(4)
            .build();
        handles.push(tokio::spawn(worker.run(shutdown.clone())));
    }

    wait_until(|| recorder.ran.load(Ordering::SeqCst) >= JOBS as usize).await;
    shutdown.cancel();
    for handle in handles {
        handle.await.expect("worker joins");
    }

    assert_eq!(
        recorder.duplicates.load(Ordering::SeqCst),
        0,
        "a row was claimed twice"
    );
    assert_eq!(recorder.ran.load(Ordering::SeqCst), JOBS as usize);
    assert_eq!(
        status_count(&db, "venturi", "completed").await,
        i64::from(JOBS)
    );
}
