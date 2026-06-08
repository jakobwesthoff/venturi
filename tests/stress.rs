//! Concurrency stress: many workers draining a large backlog with retries, with
//! no lost or double-run jobs.
//!
//! Requires Docker; marked `#[ignore]`. Run with `just integration-test`.

mod common;

use common::TestDb;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use venturi::store::{HistoryFilter, Status, Store};
use venturi::{Context, Handler, Outcome, Queue, Task, TaskError, Worker};

/// Records, per job index, how many times its handler ran.
#[derive(Default)]
struct Runs {
    by_index: Mutex<HashMap<i32, u32>>,
}

#[derive(Serialize, Deserialize)]
struct Job {
    index: i32,
    /// How many times to fail (retryably) before completing.
    fail_times: u32,
}

impl Task for Job {
    const KIND: &'static str = "stress";
    type Carry = u32;
}

impl Handler<Arc<Runs>> for Job {
    async fn handle(
        &self,
        ctx: &mut Context<u32>,
        state: &Arc<Runs>,
    ) -> Result<Outcome, TaskError> {
        *state
            .by_index
            .lock()
            .expect("lock")
            .entry(self.index)
            .or_insert(0) += 1;
        if *ctx.carry() < self.fail_times {
            *ctx.carry_mut() += 1;
            Err(TaskError::retryable(std::io::Error::other("transient")))
        } else {
            Ok(Outcome::completed())
        }
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn many_workers_drain_a_backlog_exactly_once() {
    const JOBS: i32 = 120;

    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);
    let queue = Queue::new(store.clone());

    for index in 0..JOBS {
        // A deterministic spread of 0..=2 retryable failures before completion.
        queue
            .enqueue(Job {
                index,
                fail_times: (index % 3) as u32,
            })
            .await
            .expect("enqueue");
    }

    let runs = Arc::new(Runs::default());
    let shutdown = CancellationToken::new();

    // Four independent worker loops contend over the same queue.
    let mut handles = Vec::new();
    for _ in 0..4 {
        let worker = Worker::builder(runs.clone(), store.clone())
            .register::<Job>()
            .concurrency(6)
            .build();
        handles.push(tokio::spawn(worker.run(shutdown.clone())));
    }

    // Wait until every job reaches completed.
    for _ in 0..600 {
        let completed = store
            .query_jobs(&HistoryFilter {
                status: Some(Status::Completed),
                ..Default::default()
            })
            .await
            .expect("query")
            .len();
        if completed == JOBS as usize {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    shutdown.cancel();
    for handle in handles {
        handle.await.expect("worker joins");
    }

    // Every job completed exactly once, and ran exactly (fail_times + 1) times.
    let completed = store
        .query_jobs(&HistoryFilter {
            status: Some(Status::Completed),
            ..Default::default()
        })
        .await
        .expect("query");
    assert_eq!(completed.len(), JOBS as usize, "not every job completed");

    let by_index = runs.by_index.lock().expect("lock");
    for index in 0..JOBS {
        let expected = (index % 3) as u32 + 1;
        assert_eq!(
            by_index.get(&index).copied(),
            Some(expected),
            "job {index} ran an unexpected number of times"
        );
    }
}
