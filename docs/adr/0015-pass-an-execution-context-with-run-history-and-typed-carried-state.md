# 15. Pass an execution context with run history, typed carried state, and a journal attachment

Date: 2026-06-07

## Status

Accepted

## Context

A task that decides its own give-up policy (ADR 13) and that can pause to resume
later (ADR 11) needs two things the bare payload cannot give it: knowledge of its
prior runs, and a place to keep progress between runs. It also needs to attach
structured evidence to the current run's journal entry (ADR 16), independent of
which outcome it returns. The handler otherwise receives only the payload
(`&self`) and the shared worker state (`&S`, ADR 17).

## Decision

`handle` receives a mutable execution context alongside `&self` and `&S`:

```rust
async fn handle(&self, ctx: &mut Context<Self::Carry>, state: &S)
    -> Result<Outcome, TaskError>;
```

The context exposes:

- the **run count** and the **journal** of prior outcomes (ADR 16), from which the
  failure count is read; this is what a task inspects to decide whether to give
  up.
- the **carried state**: a typed value `Carry`, a `Task` associated type
  (`Serialize + DeserializeOwned + Default`, default `()`, ADR 10/17), the handler
  reads and mutates, persisted for the next run on both retry and pause, stored in
  a JSONB column on the jobs row.
- **`ctx.set_attachment(value)`**: sets the structured attachment
  (`serde_json::Value`) for the current run's journal entry, last-write-wins.
  Available for any outcome, including before returning an error.
- the **shutdown signal**: `ctx.is_cancelled()` to poll at safe points and
  `ctx.cancelled().await` to react inside a `select!`, so a handler can wind down
  cleanly during a graceful shutdown (ADR 21).

## Consequences

A task can implement an arbitrary give-up policy from its real history rather than
a fixed counter. Multi-step and polling tasks persist progress across runs without
inventing their own storage. The attachment is gathered during the run and is
orthogonal to the outcome; the note (the run's conclusion) travels with the
outcome or error instead (ADR 11), so the two never compete. The carried state is
the job's private working state; the journal is the immutable record (ADR 16).
`Carry` is declared on `Task` rather than `Handler`, because `merge` reads and
writes it at enqueue (ADR 10); `handle` reaches it through `Context<Self::Carry>`
by way of the `Handler<S>: Task` supertrait (ADR 17).
