//! Integration tests for reliability: stale-claim recovery by lease expiry, the
//! claim-ownership guard, and graceful-shutdown release of a straggler.
//!
//! Requires Docker; marked `#[ignore]`. Run with `just integration-test`.

mod common;

use chrono::Utc;
use common::TestDb;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use venturi::store::{JournalAppend, JournalOutcome, Settlement, Store};
use venturi::{Context, Handler, Outcome, Queue, Task, TaskError, Worker};

#[derive(Serialize, Deserialize)]
struct Unit;

impl Task for Unit {
    const KIND: &'static str = "unit";
    type Carry = ();
}

fn stale_journal(kind: &str) -> JournalAppend {
    JournalAppend {
        kind: kind.to_owned(),
        run_no: 1,
        recorded_at: Utc::now(),
        outcome: JournalOutcome::StaleRecovered,
        note: Some("lease expired".to_owned()),
        attachment: None,
    }
}

async fn status_of(db: &TestDb, id: &str) -> String {
    let client = db.pool().get().await.expect("connection");
    let row = client
        .query_one("SELECT status FROM venturi_jobs WHERE id = $1", &[&id])
        .await
        .expect("status");
    row.get(0)
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn expired_lease_is_recovered_as_a_failed_execution() {
    let db = TestDb::start().await;
    let store = db.store("venturi").await;
    let id = Queue::new(Arc::new(store.clone()))
        .enqueue(Unit)
        .await
        .expect("enqueue");

    // Claim with a sub-second lease under a "dead" worker identity.
    let kinds = vec!["unit".to_owned()];
    let claimed = store
        .claim_next(&kinds, 0, Duration::from_millis(200), "deadworker")
        .await
        .expect("claim")
        .expect("a job to claim");
    assert_eq!(claimed.id, id);

    // Let the lease expire, then recovery should see it.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let stale = store.find_stale().await.expect("find stale");
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0].id, id);

    let now = Utc::now();
    let recovered = store
        .recover(id, now, 1, stale_journal("unit"))
        .await
        .expect("recover");
    assert!(recovered);

    let job = store.find_stale().await.expect("rescan");
    assert!(job.is_empty(), "the job is no longer a stale claim");

    let journal = store.journal(id).await.expect("journal");
    assert_eq!(journal.len(), 1);
    assert_eq!(journal[0].outcome, JournalOutcome::StaleRecovered);

    assert_eq!(status_of(&db, &id.to_string()).await, "pending");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn ownership_guard_prevents_double_settle() {
    let db = TestDb::start().await;
    let store = db.store("venturi").await;
    let id = Queue::new(Arc::new(store.clone()))
        .enqueue(Unit)
        .await
        .expect("enqueue");
    let kinds = vec!["unit".to_owned()];

    // Worker A claims with a tiny lease and then loses it to recovery.
    store
        .claim_next(&kinds, 0, Duration::from_millis(100), "worker-a")
        .await
        .expect("claim a")
        .expect("job");
    tokio::time::sleep(Duration::from_millis(250)).await;
    store
        .recover(id, Utc::now(), 1, stale_journal("unit"))
        .await
        .expect("recover");

    // Worker B reclaims the now-pending job.
    store
        .claim_next(&kinds, 0, Duration::from_secs(60), "worker-b")
        .await
        .expect("claim b")
        .expect("job");

    // Worker A's late settle must not apply; worker B's must.
    let journal = JournalAppend {
        kind: "unit".to_owned(),
        run_no: 1,
        recorded_at: Utc::now(),
        outcome: JournalOutcome::Completed,
        note: None,
        attachment: None,
    };
    let a_applied = store
        .settle(
            id,
            "worker-a",
            Settlement::Complete {
                finished_at: Utc::now(),
            },
            journal.clone(),
        )
        .await
        .expect("a settle");
    let b_applied = store
        .settle(
            id,
            "worker-b",
            Settlement::Complete {
                finished_at: Utc::now(),
            },
            journal,
        )
        .await
        .expect("b settle");

    assert!(!a_applied, "the stale owner cannot settle");
    assert!(b_applied, "the current owner settles");
    assert_eq!(status_of(&db, &id.to_string()).await, "completed");
}

/// A handler that ignores cancellation and runs longer than any test is willing
/// to wait, forcing the shutdown path to abort and release it.
#[derive(Serialize, Deserialize)]
struct Hang;

impl Task for Hang {
    const KIND: &'static str = "hang";
    type Carry = ();
}

impl Handler<()> for Hang {
    async fn handle(&self, _ctx: &mut Context<()>, _state: &()) -> Result<Outcome, TaskError> {
        tokio::time::sleep(Duration::from_secs(30)).await;
        Ok(Outcome::completed())
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn graceful_shutdown_releases_a_straggler() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);
    let id = Queue::new(store.clone())
        .enqueue(Hang)
        .await
        .expect("enqueue");

    let worker = Worker::builder((), store.clone())
        .register::<Hang>()
        .shutdown_timeout(Duration::from_millis(200))
        .build();
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(worker.run(shutdown.clone()));

    // Wait until the handler is running (claimed), then ask the worker to stop.
    for _ in 0..300 {
        if status_of(&db, &id.to_string()).await == "claimed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    shutdown.cancel();

    // The worker returns near the shutdown timeout, not the 30s handler sleep.
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("worker returns within the grace window")
        .expect("worker joins");

    assert_eq!(status_of(&db, &id.to_string()).await, "pending");
    let journal = store.journal(id).await.expect("journal");
    assert!(
        journal
            .iter()
            .any(|e| e.outcome == JournalOutcome::Released),
        "the straggler was released"
    );
    // A release is not a failure.
    let job = store.find_stale().await.expect("scan");
    assert!(job.is_empty());
}
