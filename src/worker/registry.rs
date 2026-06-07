//! The type-erased handler registry.
//!
//! A worker is generic over one shared-state type `S`, so every registered
//! handler has the same erased shape: given a stored job's JSON payload and carry
//! plus an `Arc<S>`, run the handler and report the outcome. Type safety is
//! recovered inside each entry, which is monomorphic in the concrete task type:
//! it deserializes the payload back into that type, builds the typed context,
//! runs `handle`, and serializes the (possibly mutated) carry back out.

use crate::backoff::Backoff;
use crate::context::{Context, JournalEntry};
use crate::error::Error;
use crate::outcome::{Outcome, TaskError};
use crate::task::Handler;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Everything one run needs, in type-erased form.
pub(crate) struct RunInput<S> {
    /// The stored task payload as JSON.
    pub payload: serde_json::Value,
    /// The stored carried state as JSON (`null` means "use the default").
    pub carry: serde_json::Value,
    /// Executions including this one.
    pub run_count: u32,
    /// Prior journal entries, for the handler's history.
    pub history: Vec<JournalEntry>,
    /// The worker's shared state.
    pub state: Arc<S>,
    /// Fires when a graceful shutdown is signalled.
    pub cancel: CancellationToken,
}

/// What one completed run yields back to the worker for settlement.
pub(crate) struct RunReport {
    /// The handler's result: complete/pause, or a retryable/permanent failure.
    pub result: Result<Outcome, TaskError>,
    /// The serialized carry after the run, persisted on pause and retry.
    pub carry: serde_json::Value,
    /// The task's per-task backoff override, if any, for scheduling a retry.
    pub backoff: Option<Backoff>,
    /// Structured evidence the handler attached during the run, for the journal.
    pub attachment: Option<serde_json::Value>,
}

/// A boxed, `Send` future produced by dispatching one run.
type RunFuture = Pin<Box<dyn Future<Output = Result<RunReport, Error>> + Send>>;

/// A type-erased run entry: build a run future from erased input.
type ErasedRun<S> = Box<dyn Fn(RunInput<S>) -> RunFuture + Send + Sync>;

/// A type-erased lease reader: deserialize a payload and read its `Task::lease`.
type ErasedLease = Box<dyn Fn(&serde_json::Value) -> Option<Duration> + Send + Sync>;

/// One registered kind: how to run it and how to read its per-task lease.
struct Entry<S> {
    run: ErasedRun<S>,
    lease: ErasedLease,
}

/// The set of handlers a worker can run, keyed by `KIND`.
pub(crate) struct Registry<S> {
    entries: HashMap<&'static str, Entry<S>>,
}

impl<S> Registry<S>
where
    S: Send + Sync + 'static,
{
    /// An empty registry.
    pub(crate) fn new() -> Registry<S> {
        Registry {
            entries: HashMap::new(),
        }
    }

    /// Register a handler type.
    ///
    /// Re-registering the same `KIND` overwrites the prior entry; the `KIND`
    /// contract is one type per discriminator.
    pub(crate) fn register<T>(&mut self)
    where
        T: Handler<S>,
    {
        self.entries.insert(
            T::KIND,
            Entry {
                run: erased_run::<T, S>(),
                lease: erased_lease::<T>(),
            },
        );
    }

    /// The registered kinds, which also form the claim filter.
    pub(crate) fn kinds(&self) -> Vec<String> {
        self.entries.keys().map(|k| (*k).to_owned()).collect()
    }

    /// Build the run future for a claimed job, or fail if its kind is unknown.
    pub(crate) fn dispatch(&self, kind: &str, input: RunInput<S>) -> Result<RunFuture, Error> {
        let entry = self.entries.get(kind).ok_or_else(|| Error::UnknownKind {
            kind: kind.to_owned(),
        })?;
        Ok((entry.run)(input))
    }

    /// The per-task lease override for a claimed job, if its kind is registered
    /// and the task requests one. A payload that fails to deserialize yields
    /// `None`; the run dispatch surfaces that failure.
    pub(crate) fn lease_for(&self, kind: &str, payload: &serde_json::Value) -> Option<Duration> {
        self.entries
            .get(kind)
            .and_then(|entry| (entry.lease)(payload))
    }
}

/// Build the erased lease reader for one concrete task type.
fn erased_lease<T>() -> ErasedLease
where
    T: crate::task::Task,
{
    Box::new(|payload: &serde_json::Value| {
        serde_json::from_value::<T>(payload.clone())
            .ok()
            .and_then(|task| task.lease())
    })
}

/// Build the erased run closure for one concrete handler type.
fn erased_run<T, S>() -> ErasedRun<S>
where
    T: Handler<S>,
    S: Send + Sync + 'static,
{
    Box::new(move |input: RunInput<S>| {
        Box::pin(async move {
            let payload: T = serde_json::from_value(input.payload)?;

            // A `null` carry is the initial (never-run) state; decode anything
            // else, falling back to the default only when storage holds null.
            let carry: T::Carry = if input.carry.is_null() {
                T::Carry::default()
            } else {
                serde_json::from_value(input.carry)?
            };

            let mut ctx = Context::new(input.run_count, input.history, carry, input.cancel);
            let result = payload.handle(&mut ctx, &input.state).await;

            // Capture the per-task backoff override before dropping the payload,
            // so the worker can schedule a retry without the concrete type.
            let backoff = payload.backoff();

            let (carry, attachment) = ctx.into_parts();
            let carry = serde_json::to_value(carry)?;
            Ok(RunReport {
                result,
                carry,
                backoff,
                attachment,
            })
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::Task;
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicI32, Ordering};

    /// Shared state that accumulates what handlers add.
    #[derive(Default)]
    struct Sink {
        total: AtomicI32,
    }

    #[derive(Serialize, Deserialize)]
    struct Ping {
        n: i32,
    }

    impl Task for Ping {
        const KIND: &'static str = "ping";
        type Carry = ();
    }

    impl Handler<Sink> for Ping {
        async fn handle(&self, _ctx: &mut Context<()>, state: &Sink) -> Result<Outcome, TaskError> {
            state.total.fetch_add(self.n, Ordering::SeqCst);
            Ok(Outcome::completed())
        }
    }

    fn input(payload: serde_json::Value, state: Arc<Sink>) -> RunInput<Sink> {
        RunInput {
            payload,
            carry: serde_json::Value::Null,
            run_count: 1,
            history: Vec::new(),
            state,
            cancel: CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn dispatch_runs_the_registered_handler() {
        let mut registry = Registry::new();
        registry.register::<Ping>();

        let state = Arc::new(Sink::default());
        let report = registry
            .dispatch("ping", input(serde_json::json!({ "n": 5 }), state.clone()))
            .expect("kind is registered")
            .await
            .expect("run succeeds");

        assert!(matches!(report.result, Ok(Outcome::Completed { .. })));
        assert_eq!(state.total.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn dispatch_unknown_kind_is_an_error() {
        let registry: Registry<Sink> = Registry::new();
        let state = Arc::new(Sink::default());
        let result = registry.dispatch("missing", input(serde_json::Value::Null, state));
        assert!(matches!(result, Err(Error::UnknownKind { kind }) if kind == "missing"));
    }
}
