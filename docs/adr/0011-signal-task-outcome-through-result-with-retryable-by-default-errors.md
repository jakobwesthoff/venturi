# 11. Signal task outcome through Completed/Pause results, notes, and retryable-by-default errors

Date: 2026-06-07

## Status

Accepted

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
