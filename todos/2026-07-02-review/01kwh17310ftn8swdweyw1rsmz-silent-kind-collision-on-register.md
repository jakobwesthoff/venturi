# Registering two task types with the same `KIND` silently drops one

## Problem

`Registry::register` inserts into a `HashMap` keyed by `T::KIND` and
documents the behavior as "Re-registering the same `KIND` overwrites the
prior entry" (`src/worker/registry.rs:91-107`). Nothing warns or fails when
that happens.

The overwrite is only correct for the intentional case (re-registering the
same type, e.g. to change its cap). The dangerous case is two *different*
task types accidentally sharing a `KIND` string — easy to do since `KIND`
is a hand-written constant with a global-uniqueness contract that nothing
enforces (`src/task.rs:203-206`, "Must be unique per task type"). When it
happens:

- The first type's handler silently disappears from the worker.
- Jobs of that kind enqueued with the first type's payload shape are
  claimed, fail to deserialize as the surviving type, and are settled
  **Dead** ("job could not be dispatched; marking dead",
  `src/worker/mod.rs:709-719`).

So a one-line copy-paste mistake (`const KIND: &'static str = "cleanup"` in
two modules) destroys jobs at runtime with no signal pointing at the
collision.

## Suggested fix (decision needed)

`register`/`register_capped` are infallible builder methods, so pick one:

- Panic on a `KIND` collision during registration (builder-time programming
  error, caught in any test that builds the worker). Re-registration of the
  *same* type cannot be distinguished from a different type with the same
  `KIND` at runtime (the erased closures hide the type), so this would
  forbid re-registration entirely — acceptable if re-registering was never
  a supported use.
- Keep overwrite but `tracing::warn!` on any duplicate `KIND` insert.
- Make the builder methods fallible (breaking change; fold into the
  API-stability review,
  `todos/01ktj9nym9e8r5x3sqmg76a4sm-public-api-surface-review.md`).

A `TypeId` check alongside the entry would allow "same type overwrites
silently, different type panics", which preserves both behaviors exactly.

Source: review finding, `src/worker/registry.rs:91-107`,
`src/task.rs:203-206`, `src/worker/mod.rs:705-719`.
