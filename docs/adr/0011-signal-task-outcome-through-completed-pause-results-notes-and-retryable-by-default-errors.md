# 11. Signal task outcome through Completed/Pause results, notes, and retryable-by-default errors

Date: 2026-06-07

## Status

Accepted

Relates to [9. Define tasks as a trait dispatched through a type-erased registry](0009-define-tasks-as-a-trait-dispatched-through-a-type-erased-registry.md)

Relates to [10. Deduplicate with a candidacy key and a full-task merge decision](0010-deduplicate-with-a-candidacy-key-and-a-full-task-merge-decision.md)

Relates to [12. Retry with a visible-at gate, Fibonacci backoff, and proportional jitter](0012-retry-with-a-visible-at-gate-fibonacci-backoff-and-proportional-jitter.md)

Relates to [13. Let the task decide when to give up, with a configurable backstop](0013-let-the-task-decide-when-to-give-up-with-a-configurable-backstop.md)

Relates to [15. Pass an execution context with run history, typed carried state, and a journal attachment](0015-pass-an-execution-context-with-run-history-typed-carried-state-and-a-journal-attachment.md)

Relates to [16. Record every execution and merge in an append-only journal table](0016-record-every-execution-and-merge-in-an-append-only-journal-table.md)

Relates to [17. Split the task abstraction into Task and Handler](0017-split-the-task-abstraction-into-task-and-handler.md)

Relates to [19. Recover stale claims by lease expiry](0019-recover-stale-claims-by-lease-expiry.md)

Relates to [20. Run a bounded-concurrency claim and dispatch loop](0020-run-a-bounded-concurrency-claim-and-dispatch-loop.md)

Relates to [21. Shut down gracefully by draining cooperatively, then releasing](0021-shut-down-gracefully-by-draining-cooperatively-then-releasing.md)

## Context

A handler reports one of four things: the job finished; it failed transiently and
should be retried; it failed in a way that will never succeed and must be
abandoned; or it has not finished and did not fail, but wants to run again later
(a cooperative pause). Each terminal decision of a run also has a human-readable
conclusion that belongs with the decision; for a failure that conclusion is the
error itself. A queue that models only success, retry, and permanent failure has
no way to express the fourth case, a cooperative pause, and folds the conclusion
into the control signal rather than carrying it alongside. venturi's handler is
the `handle` trait method (ADR 9, ADR 17), which should keep `?` ergonomic for
the failure path.

## Decision

`handle` returns `Result<Outcome, TaskError>`, where

```rust
enum Outcome {
    Completed { note: Option<String> },
    Pause { resume_in: Duration, note: Option<String> },
}
```

- `Ok(Outcome::Completed { .. })` — completed.
- `Ok(Outcome::Pause { resume_in, .. })` — paused, **not a failure**. The job
  returns to `pending` with `visible_at = now + resume_in` (ADR 12; `resume_in`
  may be `Duration::ZERO`), carried state persisted (ADR 15). No failure is
  recorded and the retry backstop (ADR 13) is not consumed.
- `Err(_)` — failure, **retryable by default**; `Err(TaskError::permanent(_))`
  goes to `dead`.

The optional `note` is the run's conclusion and becomes the journal entry's note
(ADR 16); on failure the note is the `TaskError`'s message. Structured per-run
data is attached separately through the context (`ctx.set_attachment`, ADR 15),
not carried on the outcome. Constructors keep it terse: `Outcome::completed()`,
`Outcome::completed_with(msg)`, `Outcome::pause_in(d)`,
`Outcome::pause_in_with(d, msg)`.

## Consequences

Success and failure are symmetric: each is an outcome carrying a note, so they
produce journal entries of the same shape. The failure path keeps `?` ergonomics.
Pause reuses the `visible_at` mechanism, so a long poll or checkpoint loop neither
records failures nor counts against the backstop. The outcome carries only its
conclusion (and, for pause, the resume delay); evidence gathered during the run
lives on the context.
