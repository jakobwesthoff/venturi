//! Integration tests for outcome settlement: retry with counters, pause, and
//! death (permanent and via the backstop).
//!
//! Requires Docker; marked `#[ignore]`. Run with `just integration-test`.

mod common;

use common::TestDb;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use venturi::{Context, Handler, Outcome, Queue, Task, TaskError, Worker};

/// Per-worker behaviour, chosen by the test.
#[derive(Clone)]
enum Mode {
    /// Fail retryably `n` times (counted in the carry), then complete.
    FailThenComplete(u32),
    /// Always return a retryable failure.
    AlwaysRetryable,
    /// Always return a permanent failure.
    AlwaysPermanent,
    /// Pause once (recorded in the carry), then complete.
    PauseThenComplete,
}

#[derive(Serialize, Deserialize)]
struct Job;

impl Task for Job {
    const KIND: &'static str = "job";
    type Carry = u32;
}

impl Handler<Mode> for Job {
    async fn handle(&self, ctx: &mut Context<u32>, mode: &Mode) -> Result<Outcome, TaskError> {
        match mode {
            Mode::FailThenComplete(n) => {
                if *ctx.carry() < *n {
                    *ctx.carry_mut() += 1;
                    Err(TaskError::retryable(std::io::Error::other("transient")))
                } else {
                    Ok(Outcome::completed())
                }
            }
            Mode::AlwaysRetryable => Err(TaskError::retryable(std::io::Error::other("transient"))),
            Mode::AlwaysPermanent => Err(TaskError::permanent("gone for good")),
            Mode::PauseThenComplete => {
                if *ctx.carry() == 0 {
                    *ctx.carry_mut() = 1;
                    Ok(Outcome::pause_in(Duration::ZERO))
                } else {
                    Ok(Outcome::completed())
                }
            }
        }
    }
}

/// A job's settled state, read directly from storage.
struct JobState {
    status: String,
    run_count: i32,
    failure_count: i32,
    carry: serde_json::Value,
}

async fn read_job(db: &TestDb, prefix: &str, id: &str) -> JobState {
    let client = db.pool().get().await.expect("connection");
    let sql =
        format!("SELECT status, run_count, failure_count, carry FROM {prefix}_jobs WHERE id = $1");
    let row = client.query_one(&sql, &[&id]).await.expect("read job");
    JobState {
        status: row.get(0),
        run_count: row.get(1),
        failure_count: row.get(2),
        carry: row.get(3),
    }
}

async fn count(db: &TestDb, prefix: &str, status: &str) -> i64 {
    let client = db.pool().get().await.expect("connection");
    let sql = format!("SELECT count(*) FROM {prefix}_jobs WHERE status = $1");
    client
        .query_one(&sql, &[&status])
        .await
        .expect("count")
        .get(0)
}

/// Poll until the job reaches a terminal state, or panic at the deadline.
async fn wait_terminal(db: &TestDb, id: &str) {
    for _ in 0..300 {
        let state = read_job(db, "venturi", id).await;
        if state.status == "completed" || state.status == "dead" {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("job {id} did not reach a terminal state within the deadline");
}

/// Run a worker over one enqueued job until it terminates, then stop it.
async fn drive(db: &TestDb, mode: Mode, backstop: Option<u32>, id: String) -> JobState {
    let store = Arc::new(db.store("venturi").await);
    let worker = Worker::builder(mode, store.clone())
        .register::<Job>()
        .backstop(backstop)
        .build();

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(worker.run(shutdown.clone()));

    wait_terminal(db, &id).await;

    shutdown.cancel();
    handle.await.expect("worker joins");
    read_job(db, "venturi", &id).await
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn permanent_failure_marks_dead() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);
    let id = Queue::new(store).enqueue(Job).await.expect("enqueue");

    let state = drive(&db, Mode::AlwaysPermanent, Some(20), id.to_string()).await;
    assert_eq!(state.status, "dead");
    assert_eq!(state.failure_count, 1);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn retryable_failures_count_then_complete() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);
    let id = Queue::new(store).enqueue(Job).await.expect("enqueue");

    // Two zero-delay retries, then completion.
    let state = drive(&db, Mode::FailThenComplete(2), Some(20), id.to_string()).await;
    assert_eq!(state.status, "completed");
    assert_eq!(state.failure_count, 2);
    assert_eq!(state.run_count, 3);
    assert_eq!(state.carry, serde_json::json!(2));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn retryable_failures_reach_dead_at_backstop() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);
    let id = Queue::new(store).enqueue(Job).await.expect("enqueue");

    let state = drive(&db, Mode::AlwaysRetryable, Some(2), id.to_string()).await;
    assert_eq!(state.status, "dead");
    assert_eq!(state.failure_count, 2);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn pause_repends_without_failure() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);
    let id = Queue::new(store).enqueue(Job).await.expect("enqueue");

    let state = drive(&db, Mode::PauseThenComplete, Some(20), id.to_string()).await;
    assert_eq!(state.status, "completed");
    assert_eq!(state.run_count, 2);
    assert_eq!(state.failure_count, 0);
    assert_eq!(state.carry, serde_json::json!(1));

    assert_eq!(count(&db, "venturi", "completed").await, 1);
}
