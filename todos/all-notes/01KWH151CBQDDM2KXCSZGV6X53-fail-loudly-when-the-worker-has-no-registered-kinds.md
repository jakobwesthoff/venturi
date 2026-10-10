---
title: "Fail loudly when the worker has no registered kinds"
kind: bug
component: worker
status: needs-discussion
origin: review
---
# Fail loudly when the worker has no registered kinds

Found in the 2026-07-02 review. `Worker::run` returns immediately and silently when no handler is registered, so a worker that forgot its `.register::<T>()` calls exits or idles without any log line, error or metric.

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

## Fix (decision needed)

In increasing order of strength:

1. Emit `tracing::error!` (not `warn`) before returning, so the condition is
   at least visible in logs. Minimal, non-breaking.
2. Panic: an empty registry is a programming error, and a loud failure at
   startup is easier to attribute than a silent idle worker.
3. Move the check to `WorkerBuilder::build()` and make it return
   `Result<Worker<S>, Error>` (using `Error::Config`). Catches the mistake
   at construction, where the missing `.register` call is; a pre-1.0
   breaking change to the builder signature. Fold it into the API-stability
   review if chosen.

Source: review finding, `src/worker/mod.rs:312-318`.

## Relations

- Relates to: [Review the public API surface and stability before 1.0](01KTJ9NYM9E8R5X3SQMG76A4SM-review-the-public-api-surface-and-stability-before-10.md)
