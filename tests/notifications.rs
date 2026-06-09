//! Integration tests for the emit side of push wakeups: every store write that
//! makes a job claimable again must queue a `NOTIFY`, and terminal ones must not.
//!
//! These drive each transition through the [`Store`] trait and observe the channel
//! directly on a dedicated `LISTEN` connection. Observing the raw notification
//! (rather than whether some worker happens to wake) is deliberate: the worker that
//! performs a re-pend re-claims it on its own next loop without needing the wakeup,
//! so a worker-timing test could not isolate the emit. The receive side — a parked
//! worker actually waking on a delivered `NOTIFY` — is covered by
//! `scheduling::notify_wakes_an_idle_worker_promptly`.
//!
//! The TLS receive path is not exercised here: the test container serves no TLS.
//! `connect_rustls` shares the same `from_config`/listener code as the `NoTls` path
//! these tests run through, and is compile-verified only.
//!
//! Requires Docker; marked `#[ignore]`. Run with `just integration-test`.

mod common;

use chrono::Utc;
use common::TestDb;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tokio_postgres::{AsyncMessage, Client, NoTls};
use ulid::Ulid;
use venturi::store::{JournalAppend, JournalOutcome, NewJob, Settlement, Store};

const KIND: &str = "quick";
const CLAIMED_BY: &str = "tester";

/// Open a dedicated connection and `LISTEN` on `channel`, forwarding each
/// notification as a unit wakeup. The returned client must be kept alive for the
/// connection (and thus the subscription) to stay open.
async fn listen(dsn: &str, channel: &str) -> (Client, UnboundedReceiver<()>) {
    let (client, mut connection) = tokio_postgres::connect(dsn, NoTls)
        .await
        .expect("connect listener");
    let (tx, rx) = unbounded_channel();
    tokio::spawn(async move {
        loop {
            match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(AsyncMessage::Notification(_))) => {
                    if tx.send(()).is_err() {
                        break;
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            }
        }
    });
    client
        .batch_execute(&format!("LISTEN {channel}"))
        .await
        .expect("LISTEN");
    (client, rx)
}

/// Whether a wakeup arrives within `ms` milliseconds.
async fn notified_within(rx: &mut UnboundedReceiver<()>, ms: u64) -> bool {
    tokio::time::timeout(Duration::from_millis(ms), rx.recv())
        .await
        .map(|received| received.is_some())
        .unwrap_or(false)
}

/// Consume any wakeups already queued (e.g. the enqueue's own notify) so the next
/// assertion sees only what the transition under test produced.
async fn drain(rx: &mut UnboundedReceiver<()>) {
    while notified_within(rx, 250).await {}
}

fn new_job(id: Ulid) -> NewJob {
    let now = Utc::now();
    NewJob {
        id,
        kind: KIND.to_owned(),
        payload: serde_json::Value::Null,
        priority: 1,
        created_at: now,
        // Eligible comfortably in the past so the immediate claim cannot lose to
        // clock skew between the host and the database container.
        visible_at: now - chrono::Duration::seconds(5),
        carry: serde_json::Value::Null,
        dedup_key: None,
    }
}

fn journal(run_no: i32, outcome: JournalOutcome) -> JournalAppend {
    JournalAppend {
        kind: KIND.to_owned(),
        run_no,
        recorded_at: Utc::now(),
        outcome,
        note: None,
        attachment: None,
    }
}

/// Enqueue a fresh job and claim it, returning its id and the run number to
/// journal the settlement under. Drains the enqueue's own notify before returning.
async fn enqueue_and_claim(
    store: &impl Store,
    rx: &mut UnboundedReceiver<()>,
    lease: Duration,
) -> (Ulid, i32) {
    let id = Ulid::new();
    store.enqueue(&new_job(id)).await.expect("enqueue");
    let kinds = vec![KIND.to_owned()];
    let job = store
        .claim_next(&kinds, 0, lease, CLAIMED_BY)
        .await
        .expect("claim")
        .expect("a claimable job");
    drain(rx).await;
    (id, job.run_count)
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn enqueue_notifies() {
    let db = TestDb::start().await;
    let store = db.store("venturi").await;
    let (_listener, mut rx) = listen(&db.dsn(), "venturi_jobs").await;

    store.enqueue(&new_job(Ulid::new())).await.expect("enqueue");

    assert!(
        notified_within(&mut rx, 1000).await,
        "enqueue did not emit a NOTIFY"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn retry_settlement_notifies() {
    let db = TestDb::start().await;
    let store = db.store("venturi").await;
    let (_listener, mut rx) = listen(&db.dsn(), "venturi_jobs").await;

    let (id, run_no) = enqueue_and_claim(&store, &mut rx, Duration::from_secs(60)).await;
    store
        .settle(
            id,
            CLAIMED_BY,
            run_no,
            Settlement::Retry {
                visible_at: Utc::now(),
                failure_count: 1,
                carry: serde_json::Value::Null,
            },
            journal(run_no, JournalOutcome::Retried),
        )
        .await
        .expect("settle retry");

    assert!(
        notified_within(&mut rx, 1000).await,
        "retry settlement did not emit a NOTIFY"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn pause_settlement_notifies() {
    let db = TestDb::start().await;
    let store = db.store("venturi").await;
    let (_listener, mut rx) = listen(&db.dsn(), "venturi_jobs").await;

    let (id, run_no) = enqueue_and_claim(&store, &mut rx, Duration::from_secs(60)).await;
    store
        .settle(
            id,
            CLAIMED_BY,
            run_no,
            Settlement::Pause {
                visible_at: Utc::now(),
                carry: serde_json::Value::Null,
            },
            journal(run_no, JournalOutcome::Paused),
        )
        .await
        .expect("settle pause");

    assert!(
        notified_within(&mut rx, 1000).await,
        "pause settlement did not emit a NOTIFY"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn release_settlement_notifies() {
    let db = TestDb::start().await;
    let store = db.store("venturi").await;
    let (_listener, mut rx) = listen(&db.dsn(), "venturi_jobs").await;

    let (id, run_no) = enqueue_and_claim(&store, &mut rx, Duration::from_secs(60)).await;
    store
        .settle(
            id,
            CLAIMED_BY,
            run_no,
            Settlement::Release {
                visible_at: Utc::now(),
            },
            journal(run_no, JournalOutcome::Released),
        )
        .await
        .expect("settle release");

    assert!(
        notified_within(&mut rx, 1000).await,
        "release settlement did not emit a NOTIFY"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn complete_settlement_does_not_notify() {
    let db = TestDb::start().await;
    let store = db.store("venturi").await;
    let (_listener, mut rx) = listen(&db.dsn(), "venturi_jobs").await;

    let (id, run_no) = enqueue_and_claim(&store, &mut rx, Duration::from_secs(60)).await;
    store
        .settle(
            id,
            CLAIMED_BY,
            run_no,
            Settlement::Complete {
                finished_at: Utc::now(),
            },
            journal(run_no, JournalOutcome::Completed),
        )
        .await
        .expect("settle complete");

    assert!(
        !notified_within(&mut rx, 500).await,
        "a terminal completion must not emit a NOTIFY"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn dead_settlement_does_not_notify() {
    let db = TestDb::start().await;
    let store = db.store("venturi").await;
    let (_listener, mut rx) = listen(&db.dsn(), "venturi_jobs").await;

    let (id, run_no) = enqueue_and_claim(&store, &mut rx, Duration::from_secs(60)).await;
    store
        .settle(
            id,
            CLAIMED_BY,
            run_no,
            Settlement::Dead {
                finished_at: Utc::now(),
                failure_count: 3,
            },
            journal(run_no, JournalOutcome::Dead),
        )
        .await
        .expect("settle dead");

    assert!(
        !notified_within(&mut rx, 500).await,
        "a terminal dead must not emit a NOTIFY"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn recover_notifies() {
    let db = TestDb::start().await;
    let store = db.store("venturi").await;
    let (_listener, mut rx) = listen(&db.dsn(), "venturi_jobs").await;

    // Claim under a tiny lease and let it expire, so the row is a stale claim.
    let (id, run_no) = enqueue_and_claim(&store, &mut rx, Duration::from_millis(100)).await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let recovered = store
        .recover(
            id,
            Utc::now(),
            run_no + 1,
            journal(run_no, JournalOutcome::StaleRecovered),
        )
        .await
        .expect("recover");
    assert!(recovered, "the stale claim should have been recovered");

    assert!(
        notified_within(&mut rx, 1000).await,
        "stale-claim recovery did not emit a NOTIFY"
    );
}
