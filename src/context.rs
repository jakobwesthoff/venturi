//! The per-run execution [`Context`] handed to a handler, and the [`JournalEntry`]
//! view it exposes over prior runs.
//!
//! A handler reads its run count and history, reads and mutates its carried
//! state, attaches structured evidence, and observes graceful shutdown, all
//! through the context. The carried state is persisted on both retry and pause;
//! the attachment rides this run's journal entry.

use crate::store::JournalOutcome;
use chrono::{DateTime, Utc};
use std::future::Future;
use tokio_util::sync::CancellationToken;

/// A read-only view of one prior journal entry, as a handler sees its history.
///
/// This is the user-facing projection of a stored journal row: it drops storage
/// surrogates and exposes the run number, the outcome, the note, and any
/// attachment. A task's failure history is the subset whose [`outcome`] is a
/// failure.
///
/// [`outcome`]: JournalEntry::outcome
#[derive(Debug, Clone)]
pub struct JournalEntry {
    run_no: u32,
    recorded_at: DateTime<Utc>,
    outcome: JournalOutcome,
    note: Option<String>,
    attachment: Option<serde_json::Value>,
}

impl JournalEntry {
    /// Construct an entry. Used by the worker when loading history from storage.
    pub(crate) fn new(
        run_no: u32,
        recorded_at: DateTime<Utc>,
        outcome: JournalOutcome,
        note: Option<String>,
        attachment: Option<serde_json::Value>,
    ) -> JournalEntry {
        JournalEntry {
            run_no,
            recorded_at,
            outcome,
            note,
            attachment,
        }
    }

    /// The run number this entry recorded.
    pub fn run_no(&self) -> u32 {
        self.run_no
    }

    /// When the entry was written.
    pub fn recorded_at(&self) -> DateTime<Utc> {
        self.recorded_at
    }

    /// The recorded outcome.
    pub fn outcome(&self) -> JournalOutcome {
        self.outcome
    }

    /// The run's conclusion, or on failure the error message.
    pub fn note(&self) -> Option<&str> {
        self.note.as_deref()
    }

    /// Structured evidence attached during that run, if any.
    pub fn attachment(&self) -> Option<&serde_json::Value> {
        self.attachment.as_ref()
    }

    /// Whether this entry records a failed execution.
    pub fn is_failure(&self) -> bool {
        self.outcome.is_failure()
    }
}

/// The execution context for one run of a handler.
///
/// Generic over the task's `Carry` type. The worker builds it from the claimed
/// job (run count, prior journal, deserialized carry) and reads back the carry
/// and attachment after the run to settle the job.
pub struct Context<Carry> {
    run_count: u32,
    history: Vec<JournalEntry>,
    carry: Carry,
    attachment: Option<serde_json::Value>,
    cancel: CancellationToken,
}

impl<Carry> Context<Carry> {
    /// Build a context for a run. Called by the worker, not by handlers.
    pub(crate) fn new(
        run_count: u32,
        history: Vec<JournalEntry>,
        carry: Carry,
        cancel: CancellationToken,
    ) -> Context<Carry> {
        Context {
            run_count,
            history,
            carry,
            attachment: None,
            cancel,
        }
    }

    /// How many times this job has been executed, including the current run.
    pub fn run_count(&self) -> u32 {
        self.run_count
    }

    /// Prior outcomes for this job. The failure count is the number of entries
    /// for which [`JournalEntry::is_failure`] holds.
    pub fn history(&self) -> &[JournalEntry] {
        &self.history
    }

    /// Read the carried state.
    pub fn carry(&self) -> &Carry {
        &self.carry
    }

    /// Mutate the carried state. The value is persisted for the next run on both
    /// retry and pause.
    pub fn carry_mut(&mut self) -> &mut Carry {
        &mut self.carry
    }

    /// Set the structured attachment for this run's journal entry. Last write
    /// wins, and it is recorded for any outcome including a failure.
    pub fn set_attachment(&mut self, value: serde_json::Value) {
        self.attachment = Some(value);
    }

    /// Whether a graceful shutdown has been signalled. A long handler can poll
    /// this at a safe point and stop early, typically by returning `Pause`.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Resolves when a graceful shutdown is signalled, for use inside a `select!`
    /// to react in the middle of a long await.
    pub fn cancelled(&self) -> impl Future<Output = ()> + '_ {
        self.cancel.cancelled()
    }

    /// Consume the context after a run, yielding the (possibly mutated) carry and
    /// attachment for settlement.
    pub(crate) fn into_parts(self) -> (Carry, Option<serde_json::Value>) {
        (self.carry, self.attachment)
    }
}
