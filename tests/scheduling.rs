//! Integration tests for scheduling: priority ordering, anti-starvation
//! rotation, prompt NOTIFY wakeup, and delayed (`visible_at`) pickup.
//!
//! Requires Docker; marked `#[ignore]`. Run with `just integration-test`.

mod common;

use chrono::Utc;
use common::TestDb;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use venturi::{Context, Handler, Outcome, Priority, Queue, Task, TaskError, Worker};

/// Records the order in which jobs ran by their label.
#[derive(Default)]
struct Order {
    ran: Mutex<Vec<String>>,
    count: AtomicUsize,
}

#[derive(Serialize, Deserialize)]
struct Prioritized {
    label: String,
    tier: u8,
}

impl Task for Prioritized {
    const KIND: &'static str = "prioritized";
    type Carry = ();
    fn priority(&self) -> Priority {
        match self.tier {
            0 => Priority::High,
            2 => Priority::Low,
            _ => Priority::Normal,
        }
    }
}

impl Handler<Arc<Order>> for Prioritized {
    async fn handle(
        &self,
        _ctx: &mut Context<()>,
        state: &Arc<Order>,
    ) -> Result<Outcome, TaskError> {
        // A little work so ordering is observable with concurrency 1.
        tokio::time::sleep(Duration::from_millis(15)).await;
        state.ran.lock().expect("lock").push(self.label.clone());
        state.count.fetch_add(1, Ordering::SeqCst);
        Ok(Outcome::completed())
    }
}

async fn wait_count(order: &Order, n: usize) {
    for _ in 0..500 {
        if order.count.load(Ordering::SeqCst) >= n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("did not reach {n} runs within the deadline");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn strict_priority_runs_high_before_low() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);
    let queue = Queue::new(store.clone());

    // Enqueue low first, then high, to prove ordering is by priority not age.
    queue
        .enqueue(Prioritized {
            label: "low".into(),
            tier: 2,
        })
        .await
        .expect("enqueue");
    queue
        .enqueue(Prioritized {
            label: "normal".into(),
            tier: 1,
        })
        .await
        .expect("enqueue");
    queue
        .enqueue(Prioritized {
            label: "high".into(),
            tier: 0,
        })
        .await
        .expect("enqueue");

    let order = Arc::new(Order::default());
    let worker = Worker::builder(order.clone(), store.clone())
        .register::<Prioritized>()
        .concurrency(1)
        .priority_ratio(None) // strict priority
        .build();
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(worker.run(shutdown.clone()));
    wait_count(&order, 3).await;
    shutdown.cancel();
    handle.await.expect("worker joins");

    let ran = order.ran.lock().expect("lock").clone();
    assert_eq!(ran, vec!["high", "normal", "low"]);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn rotation_keeps_low_priority_from_starving() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);
    let queue = Queue::new(store.clone());

    // One low job behind several high jobs. Under strict priority the low would
    // run last; the rotation must let it run sooner.
    queue
        .enqueue(Prioritized {
            label: "low".into(),
            tier: 2,
        })
        .await
        .expect("enqueue");
    for i in 0..4 {
        queue
            .enqueue(Prioritized {
                label: format!("high{i}"),
                tier: 0,
            })
            .await
            .expect("enqueue");
    }

    let order = Arc::new(Order::default());
    let worker = Worker::builder(order.clone(), store.clone())
        .register::<Prioritized>()
        .concurrency(1)
        .priority_ratio(Some(2))
        .build();
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(worker.run(shutdown.clone()));
    wait_count(&order, 5).await;
    shutdown.cancel();
    handle.await.expect("worker joins");

    let ran = order.ran.lock().expect("lock").clone();
    let low_position = ran.iter().position(|l| l == "low").expect("low ran");
    assert!(
        low_position < ran.len() - 1,
        "low should not run last under rotation; order was {ran:?}"
    );
}

#[derive(Serialize, Deserialize)]
struct Quick;

impl Task for Quick {
    const KIND: &'static str = "quick";
    type Carry = ();
}

impl Handler<Arc<AtomicUsize>> for Quick {
    async fn handle(
        &self,
        _ctx: &mut Context<()>,
        state: &Arc<AtomicUsize>,
    ) -> Result<Outcome, TaskError> {
        state.fetch_add(1, Ordering::SeqCst);
        Ok(Outcome::completed())
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn notify_wakes_an_idle_worker_promptly() {
    let db = TestDb::start().await;
    // A long poll so a prompt pickup can only come from the NOTIFY path.
    let store = Arc::new(db.store("venturi").await.with_listen(db.dsn()));
    let queue = Queue::new(store.clone());

    let ran = Arc::new(AtomicUsize::new(0));
    let worker = Worker::builder(ran.clone(), store.clone())
        .register::<Quick>()
        .poll_max(Duration::from_secs(60))
        .build();
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(worker.run(shutdown.clone()));

    // Let the worker reach its idle wait, then enqueue.
    tokio::time::sleep(Duration::from_millis(300)).await;
    queue.enqueue(Quick).await.expect("enqueue");

    // With a 60s poll, only NOTIFY can make this prompt.
    for _ in 0..100 {
        if ran.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    shutdown.cancel();
    handle.await.expect("worker joins");
    assert_eq!(
        ran.load(Ordering::SeqCst),
        1,
        "NOTIFY did not wake the worker"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn delayed_job_is_picked_up_at_its_time() {
    let db = TestDb::start().await;
    // No listener: the smart wait must time the pickup from `visible_at`.
    let store = Arc::new(db.store("venturi").await);
    let queue = Queue::new(store.clone());

    let when = Utc::now() + chrono::Duration::milliseconds(800);
    queue.enqueue_at(Quick, when).await.expect("enqueue_at");

    let ran = Arc::new(AtomicUsize::new(0));
    let worker = Worker::builder(ran.clone(), store.clone())
        .register::<Quick>()
        .poll_max(Duration::from_secs(60))
        .build();
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(worker.run(shutdown.clone()));

    // Not eligible yet.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(ran.load(Ordering::SeqCst), 0, "ran before its visible_at");

    // Picked up shortly after its time, well before the 60s poll.
    for _ in 0..100 {
        if ran.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    shutdown.cancel();
    handle.await.expect("worker joins");
    assert_eq!(
        ran.load(Ordering::SeqCst),
        1,
        "delayed job was not picked up at its time"
    );
}
