//! Integration tests for the journal and the execution context's history.
//!
//! Requires Docker; marked `#[ignore]`. Run with `just integration-test`.

mod common;

use common::TestDb;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use venturi::store::{JournalOutcome, Store};
use venturi::{Context, Handler, Outcome, Queue, Task, TaskError, Worker};

/// Records what the completing run observed about its own history.
#[derive(Default)]
struct Probe {
    history_len: AtomicUsize,
    failures_seen: AtomicUsize,
}

#[derive(Serialize, Deserialize)]
struct Work;

impl Task for Work {
    const KIND: &'static str = "work";
    type Carry = u32;
}

impl Handler<Arc<Probe>> for Work {
    async fn handle(
        &self,
        ctx: &mut Context<u32>,
        state: &Arc<Probe>,
    ) -> Result<Outcome, TaskError> {
        // Attach evidence on every run, regardless of how it concludes.
        ctx.set_attachment(serde_json::json!({ "run": ctx.run_count() }));

        if *ctx.carry() < 2 {
            *ctx.carry_mut() += 1;
            Err(TaskError::retryable(std::io::Error::other("boom")))
        } else {
            state
                .history_len
                .store(ctx.history().len(), Ordering::SeqCst);
            let failures = ctx.history().iter().filter(|e| e.is_failure()).count();
            state.failures_seen.store(failures, Ordering::SeqCst);
            Ok(Outcome::completed_with("done"))
        }
    }
}

async fn wait_terminal(store: &impl Store, id: ulid::Ulid) {
    for _ in 0..300 {
        let journal = store.journal(id).await.expect("read journal");
        if journal
            .iter()
            .any(|e| matches!(e.outcome, JournalOutcome::Completed | JournalOutcome::Dead))
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("job did not terminate within the deadline");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn journal_is_hole_free_with_history_and_attachments() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);
    let id = Queue::new(store.clone())
        .enqueue(Work)
        .await
        .expect("enqueue");

    let probe = Arc::new(Probe::default());
    let worker = Worker::builder(probe.clone(), store.clone())
        .register::<Work>()
        .build();

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(worker.run(shutdown.clone()));
    wait_terminal(store.as_ref(), id).await;
    shutdown.cancel();
    handle.await.expect("worker joins");

    // Two failures then a completion: three hole-free entries, run_no 1..=3.
    let journal = store.journal(id).await.expect("read journal");
    assert_eq!(journal.len(), 3);
    assert_eq!(
        journal.iter().map(|e| e.outcome).collect::<Vec<_>>(),
        vec![
            JournalOutcome::Retried,
            JournalOutcome::Retried,
            JournalOutcome::Completed,
        ]
    );
    assert_eq!(
        journal.iter().map(|e| e.run_no).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    // Each entry carries the run's attachment and an appropriate note.
    for (index, entry) in journal.iter().enumerate() {
        let run = index + 1;
        assert_eq!(entry.attachment, Some(serde_json::json!({ "run": run })));
    }
    assert_eq!(journal[0].note.as_deref(), Some("boom"));
    assert_eq!(journal[2].note.as_deref(), Some("done"));

    // The completing run saw both prior failures in its history.
    assert_eq!(probe.history_len.load(Ordering::SeqCst), 2);
    assert_eq!(probe.failures_seen.load(Ordering::SeqCst), 2);
}
