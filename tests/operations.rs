//! Integration tests for the operations APIs: history query, cascade cleanup,
//! the stats snapshot, and feature-gated metrics.
//!
//! Requires Docker; marked `#[ignore]`. Run with `just integration-test`.

mod common;

use chrono::Utc;
use common::TestDb;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use venturi::store::{CleanupCriteria, HistoryFilter, Status, Store};
use venturi::{Context, Handler, Outcome, Queue, Task, TaskError, Worker};

#[derive(Serialize, Deserialize)]
struct Alpha;
impl Task for Alpha {
    const KIND: &'static str = "alpha";
    type Carry = ();
}
impl Handler<()> for Alpha {
    async fn handle(&self, _ctx: &mut Context<()>, _state: &()) -> Result<Outcome, TaskError> {
        Ok(Outcome::completed())
    }
}

#[derive(Serialize, Deserialize)]
struct Beta;
impl Task for Beta {
    const KIND: &'static str = "beta";
    type Carry = ();
}

// A task that carries payload data, so the by-id lookup can be checked to return
// the stored payload rather than a unit struct's `null`.
#[derive(Serialize, Deserialize)]
struct Detailed {
    label: String,
}
impl Task for Detailed {
    const KIND: &'static str = "detailed";
    type Carry = ();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn history_query_filters_by_kind_and_status() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);
    let queue = Queue::new(store.clone());

    // Two alpha jobs (one will complete), one beta job left pending.
    let alpha1 = queue.enqueue(Alpha).await.expect("enqueue");
    queue.enqueue(Alpha).await.expect("enqueue");
    queue.enqueue(Beta).await.expect("enqueue");

    // Complete the alpha jobs by running an alpha-only worker briefly.
    let worker = Worker::builder((), store.clone())
        .register::<Alpha>()
        .build();
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(worker.run(shutdown.clone()));
    for _ in 0..200 {
        let done = store
            .query_jobs(&HistoryFilter {
                kind: Some("alpha".into()),
                status: Some(Status::Completed),
                ..Default::default()
            })
            .await
            .expect("query")
            .len();
        if done == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    shutdown.cancel();
    handle.await.expect("worker joins");

    // Filter by kind.
    let alphas = queue
        .jobs(&HistoryFilter {
            kind: Some("alpha".into()),
            ..Default::default()
        })
        .await
        .expect("query");
    assert_eq!(alphas.len(), 2);

    // Filter by status.
    let pending = queue
        .jobs(&HistoryFilter {
            status: Some(Status::Pending),
            ..Default::default()
        })
        .await
        .expect("query");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].kind, "beta");

    // The completed alpha's journal timeline is reachable.
    let journal = queue.job_journal(alpha1).await.expect("journal");
    assert!(!journal.is_empty());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn job_by_id_returns_the_full_record_or_none() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);
    let queue = Queue::new(store.clone());

    let id = queue
        .enqueue(Detailed {
            label: "render".into(),
        })
        .await
        .expect("enqueue");

    let record = queue.job(id).await.expect("lookup").expect("job exists");
    assert_eq!(record.id, id);
    assert_eq!(record.kind, "detailed");
    // The point lookup carries the payload the filtered history scan also returns.
    assert_eq!(record.payload, serde_json::json!({ "label": "render" }));

    // An id that was never enqueued resolves to `None`, not an error.
    let missing = queue.job(ulid::Ulid::new()).await.expect("lookup");
    assert!(missing.is_none());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn cleanup_deletes_terminal_jobs_and_cascades_to_journal() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);
    let queue = Queue::new(store.clone());

    let id = queue.enqueue(Alpha).await.expect("enqueue");
    let worker = Worker::builder((), store.clone())
        .register::<Alpha>()
        .build();
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(worker.run(shutdown.clone()));
    for _ in 0..200 {
        if !queue.job_journal(id).await.expect("journal").is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    shutdown.cancel();
    handle.await.expect("worker joins");

    assert!(!queue.job_journal(id).await.expect("journal").is_empty());

    let deleted = queue
        .cleanup(&CleanupCriteria {
            finished_before: Utc::now() + chrono::Duration::seconds(1),
            kind: None,
            status: None,
        })
        .await
        .expect("cleanup");
    assert_eq!(deleted, 1);

    // The job and its journal are gone (FK cascade).
    assert!(queue.job_journal(id).await.expect("journal").is_empty());
    let client = db.pool().get().await.expect("connection");
    let count: i64 = client
        .query_one("SELECT count(*) FROM venturi_jobs", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 0);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn stats_reports_live_state() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);
    let queue = Queue::new(store.clone());

    queue.enqueue(Alpha).await.expect("enqueue");
    queue.enqueue(Alpha).await.expect("enqueue");
    queue.enqueue(Beta).await.expect("enqueue");

    let snapshot = queue.stats().await.expect("stats");
    assert_eq!(snapshot.pending_by_kind.get("alpha"), Some(&2));
    assert_eq!(snapshot.pending_by_kind.get("beta"), Some(&1));
    assert_eq!(snapshot.claimed, 0);
    assert!(snapshot.oldest_pending_age.contains_key("alpha"));
}

#[cfg(feature = "metrics")]
#[tokio::test]
#[ignore = "requires Docker"]
async fn metrics_are_emitted_through_the_facade() {
    use metrics_util::debugging::DebuggingRecorder;

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    // Safe to install once in this dedicated test binary path.
    let _ = recorder.install();

    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);
    let queue = Queue::new(store.clone());
    let id = queue.enqueue(Alpha).await.expect("enqueue");

    let worker = Worker::builder((), store.clone())
        .register::<Alpha>()
        .build();
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(worker.run(shutdown.clone()));
    for _ in 0..200 {
        if !queue.job_journal(id).await.expect("journal").is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    shutdown.cancel();
    handle.await.expect("worker joins");

    let names: Vec<String> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .map(|(key, _, _, _)| key.key().name().to_owned())
        .collect();
    assert!(
        names.iter().any(|n| n == "venturi_jobs_enqueued_total"),
        "enqueued counter missing; saw {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "venturi_jobs_settled_total"),
        "settled counter missing; saw {names:?}"
    );
}
