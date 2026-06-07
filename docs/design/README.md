# venturi design

venturi is a durable, PostgreSQL-backed job queue for Rust, built to be shared
across projects rather than reimplemented per codebase. This directory holds the
design: the surface a project programs against, the runtime that executes work,
the storage schema, and how it is observed. Every decision is recorded as an
Architecture Decision Record in `../adr/`; these documents synthesize the accepted
ADRs into coherent narratives.

## Shape

venturi is layered, and each layer is usable on its own (ADR 7):

- **Storage** sits behind a backend trait (ADR 8). The default adapter is
  PostgreSQL via tokio-postgres, deadpool, and refinery (ADR 24).
- **The worker loop** drives the storage operations: claim, settle, recover.
- **The task registry and dispatch** sit on top, turning typed tasks into runnable
  work.

A consuming project can use the batteries-included engine or drop down a layer and
supply its own piece against the layer below.

## How it fits together

A producer enqueues a typed `Task`, needing only the state-free `Task` trait.
Storage records it as a row in the jobs table. A `Worker<S>` claims the
highest-priority, oldest, eligible job among its registered kinds with `SELECT …
FOR UPDATE SKIP LOCKED`, deserializes it, and runs the consumer's `Handler<S>` with
shared state `&S` and an execution `Context`. The handler returns an `Outcome`
(completed, a cooperative pause, a retryable error, or a permanent failure); the
worker settles it, and every execution is recorded in an append-only journal.
Deduplication with content-aware merge, backoff with deterministic jitter, a
per-job lease with stale-claim recovery, cooperative graceful shutdown, priority
with weighted-slot anti-starvation, and per-kind concurrency caps govern how work
flows.

## The documents

- **[`task-model.md`](task-model.md)** — the authoring surface: the `Task` /
  `Handler<S>` split, the type-erased registry, deduplication (`dedup_key` +
  `merge`), the four outcomes, the `Context` and typed carry, and the
  retry/give-up model.
- **[`worker-model.md`](worker-model.md)** — the runtime: the claim/dispatch loop,
  concurrency, wakeup, settlement, stale-claim recovery, graceful shutdown,
  priority and anti-starvation, and per-kind concurrency caps.
- **[`schema.md`](schema.md)** — the default adapter's PostgreSQL tables, columns,
  constraints, and indexes.
- **[`indexes.md`](indexes.md)** — a tracker mapping each access path to the index
  that serves it.
- **[`observability.md`](observability.md)** — logging, metrics, and queue-state
  introspection.

## Decided and deferred

The design is complete for its agreed scope, recorded in `../adr/0001` through
`0025`. Deliberately deferred: rate control (throttling a kind over time; see
`../../todos/`), global cross-worker concurrency and rate limits, and
implementation-time index tuning.
