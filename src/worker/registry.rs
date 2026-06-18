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
use futures_util::FutureExt;
use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Everything one run needs, in type-erased form.
pub(crate) struct RunInput<S> {
    /// The job's stable identifier, exposed to the handler through `Context::id`.
    pub id: ulid::Ulid,
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
    /// How a panic in this run is settled.
    pub panic_policy: super::PanicPolicy,
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
    /// How long the handler's `handle` call took, for the duration metric.
    pub duration: Duration,
}

/// A boxed, `Send` future produced by dispatching one run.
type RunFuture = Pin<Box<dyn Future<Output = Result<RunReport, Error>> + Send>>;

/// A type-erased run entry: build a run future from erased input.
type ErasedRun<S> = Box<dyn Fn(RunInput<S>) -> RunFuture + Send + Sync>;

/// A type-erased lease reader: deserialize a payload and read its `Task::lease`.
type ErasedLease = Box<dyn Fn(&serde_json::Value) -> Option<Duration> + Send + Sync>;

/// One registered kind: how to run it, how to read its per-task lease, and its
/// optional per-kind concurrency cap.
struct Entry<S> {
    run: ErasedRun<S>,
    lease: ErasedLease,
    cap: Option<usize>,
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

    /// Register a handler type with an optional per-kind concurrency cap.
    ///
    /// Re-registering the same `KIND` overwrites the prior entry; the `KIND`
    /// contract is one type per discriminator.
    pub(crate) fn register<T>(&mut self, cap: Option<usize>)
    where
        T: Handler<S>,
    {
        self.entries.insert(
            T::KIND,
            Entry {
                run: erased_run::<T, S>(),
                lease: erased_lease::<T>(),
                cap,
            },
        );
    }

    /// The registered kinds, which also form the base claim filter.
    pub(crate) fn kinds(&self) -> Vec<String> {
        self.entries.keys().map(|k| (*k).to_owned()).collect()
    }

    /// The per-kind concurrency cap for `kind`, if one was set.
    pub(crate) fn cap(&self, kind: &str) -> Option<usize> {
        self.entries.get(kind).and_then(|entry| entry.cap)
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

            // Keep the pre-run carry as JSON. If the handler panics mid-run, its
            // partially mutated context is discarded and the retry resumes from
            // this last good state rather than a torn one.
            let carry_before = input.carry.clone();

            // A `null` carry is the initial (never-run) state; decode anything
            // else, falling back to the default only when storage holds null.
            let carry: T::Carry = if input.carry.is_null() {
                T::Carry::default()
            } else {
                serde_json::from_value(input.carry)?
            };

            let mut ctx = Context::new(
                input.id,
                input.run_count,
                input.history,
                carry,
                input.cancel,
            );
            let started = std::time::Instant::now();

            // Catch a panic at the task boundary so it settles as a failed
            // execution (a retryable error) instead of abandoning the claim to
            // lease recovery. `AssertUnwindSafe` is sound here because a caught
            // panic discards `ctx` and the handler's effects and persists the
            // pre-run carry, so no torn value crosses the unwind boundary. This
            // only engages under unwind; an abort-mode panic ends the process and
            // still falls to lease recovery.
            let caught = AssertUnwindSafe(payload.handle(&mut ctx, &input.state))
                .catch_unwind()
                .await;
            let duration = started.elapsed();

            // Capture the per-task backoff override before dropping the payload,
            // so the worker can schedule a retry without the concrete type.
            let backoff = payload.backoff();

            match caught {
                Ok(result) => {
                    let (carry, attachment) = ctx.into_parts();
                    // The handler already ran to completion here, so a carry that
                    // cannot be serialized is not a dispatch failure: surface it as a
                    // permanent failure with an accurate message, settled dead through
                    // the normal outcome routing (re-running would only fail to encode
                    // again). The pre-run decode `?`s above still report genuine
                    // dispatch failures.
                    match serde_json::to_value(carry) {
                        Ok(carry) => Ok(RunReport {
                            result,
                            carry,
                            backoff,
                            attachment,
                            duration,
                        }),
                        Err(error) => Ok(RunReport {
                            result: Err(TaskError::permanent(format!(
                                "handler completed but its carry could not be serialized: {error}"
                            ))),
                            carry: serde_json::Value::Null,
                            backoff,
                            attachment,
                            duration,
                        }),
                    }
                }
                Err(panic) => {
                    // The configured policy decides whether a panic is a retryable
                    // failure (scheduled with backoff, bounded by the backstop) or
                    // a permanent one (straight to dead). Both flow through the
                    // worker's normal settlement routing.
                    let message = panic_message(panic);
                    let error = match input.panic_policy {
                        super::PanicPolicy::Retry => TaskError::retryable(message),
                        super::PanicPolicy::Dead => TaskError::permanent(message),
                    };
                    Ok(RunReport {
                        result: Err(error),
                        carry: carry_before,
                        backoff,
                        attachment: None,
                        duration,
                    })
                }
            }
        })
    })
}

/// Extract a readable message from a caught panic payload, which is the value
/// passed to `panic!` (commonly a `&str` or `String`).
fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&'static str>() {
        format!("handler panicked: {message}")
    } else if let Some(message) = panic.downcast_ref::<String>() {
        format!("handler panicked: {message}")
    } else {
        "handler panicked".to_owned()
    }
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
            id: ulid::Ulid::new(),
            payload,
            carry: serde_json::Value::Null,
            run_count: 1,
            history: Vec::new(),
            state,
            cancel: CancellationToken::new(),
            panic_policy: crate::worker::PanicPolicy::Retry,
        }
    }

    #[tokio::test]
    async fn dispatch_runs_the_registered_handler() {
        let mut registry = Registry::new();
        registry.register::<Ping>(None);

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
