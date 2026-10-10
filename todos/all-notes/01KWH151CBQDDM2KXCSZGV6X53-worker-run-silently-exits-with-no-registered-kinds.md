# `Worker::run` silently returns when no handler is registered

## Problem

`Worker::run` exits immediately, with no log line and no error, when the
registry is empty (`src/worker/mod.rs:313-318`):

```rust
let kinds = self.registry.kinds();
// A worker with no registered kinds can never claim anything; running its
// loop would just spin on empty waits.
if kinds.is_empty() {
    return;
}
```

Not spinning is right, but the silence is a misconfiguration trap. A binary
that builds a worker and forgets (or feature-gates away) every
`.register::<T>()` call starts up, `run()` returns instantly, and the
process either exits "cleanly" or, if `run` was spawned onto a task, keeps
running while consuming no jobs. Nothing in logs, metrics, or the return
type distinguishes this from a healthy worker on an empty queue. The
backlog just grows.

`run` returns `()`, so today there is no channel to report it as an error
without a signature change.

## Suggested fix (decision needed)

In increasing order of strength:

1. Emit `tracing::error!` (not `warn`) before returning, so the condition is
   at least visible in logs. Minimal, non-breaking.
2. Panic: an empty registry is a programming error, and a loud failure at
   startup is easier to attribute than a silent idle worker.
3. Move the check to `WorkerBuilder::build()` and make it return
   `Result<Worker<S>, Error>` (using `Error::Config`). Catches the mistake
   at construction, where the missing `.register` call is; a pre-1.0
   breaking change to the builder signature — fold into the API-stability
   review (`todos/01ktj9nym9e8r5x3sqmg76a4sm-public-api-surface-review.md`)
   if chosen.

Source: review finding, `src/worker/mod.rs:312-318`.
