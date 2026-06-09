# 17. Split the task abstraction into Task and Handler

Date: 2026-06-07

## Status

Accepted

Relates to [9. Define tasks as a trait dispatched through a type-erased registry](0009-define-tasks-as-a-trait-dispatched-through-a-type-erased-registry.md)

Relates to [10. Deduplicate with a candidacy key and a full-task merge decision](0010-deduplicate-with-a-candidacy-key-and-a-full-task-merge-decision.md)

Relates to [11. Signal task outcome through Completed/Pause results, notes, and retryable-by-default errors](0011-signal-task-outcome-through-completed-pause-results-notes-and-retryable-by-default-errors.md)

Relates to [13. Let the task decide when to give up, with a configurable backstop](0013-let-the-task-decide-when-to-give-up-with-a-configurable-backstop.md)

Relates to [15. Pass an execution context with run history, typed carried state, and a journal attachment](0015-pass-an-execution-context-with-run-history-typed-carried-state-and-a-journal-attachment.md)

Relates to [19. Recover stale claims by lease expiry](0019-recover-stale-claims-by-lease-expiry.md)

Relates to [22. Schedule claims by priority with weighted-slot anti-starvation](0022-schedule-claims-by-priority-with-weighted-slot-anti-starvation.md)

## Context

ADR 9 defined tasks as a trait implemented on the payload struct, with handlers
receiving a single shared state `S`. Where `S` attaches matters: producers that
only enqueue work must not be forced to depend on the worker's state type. A
typical deployment separates an enqueuing HTTP server from one or more worker
binaries that process the jobs, so coupling enqueue to the worker state would
force the server to depend on runtime dependencies it never uses.

## Decision

The abstraction is two traits implemented on the same payload struct:

- **`Task`** — state-free. Carries `const KIND`, `dedup_key` and the carry-aware
  `merge` (ADR 10), `priority`, the per-task backoff override (ADR 13), the
  associated `Carry` type, and the `Serialize + DeserializeOwned` bounds. Used by
  producers to enqueue and by storage. It does not mention `S`.
- **`Handler<S>: Task`** — the execution side. Carries the method
  `async fn handle(&self, ctx: &mut Context<Self::Carry>, state: &S) -> Result<Outcome, TaskError>`.

The worker is generic over a consumer-defined state `S` (`Worker<S>`) and
registers task types by the bound `T: Handler<S>`. `Handler<S>` has `Task` as a
supertrait because running a job requires identifying and deserialising it first;
a handler with no identity cannot exist in this system, and the supertrait keeps
the registration bound to a single trait.

`Carry` is on `Task`, not `Handler`, because it is used at enqueue time: `merge`
reads the existing job's carry and may produce a continued carry (ADR 10), and a
new job is stored with `Carry::default()`. `handle` reaches the same `Carry`
through `Context<Self::Carry>` by way of the supertrait. A producer crate
implements `Task` (including `Carry` and `merge`) with no knowledge of `S`; a
worker crate adds `impl Handler<S>`. In a single-binary project both impls sit
together.

## Consequences

Enqueueing depends only on `Task`, so producers can live in a state-free crate.
Execution and its dependencies are isolated in `Handler<S>`. The homogeneous
registry (ADR 9) keys on `Task::KIND` and dispatches through `Handler::handle`
with the worker's `&S`. This finalises the state-binding question left open in
ADR 9.
