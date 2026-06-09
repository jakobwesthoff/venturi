# 24. Build the default storage adapter on tokio-postgres, deadpool, and refinery

Date: 2026-06-07

## Status

Accepted

Amended by [26. Keep PostgreSQL as the only backend and remove the postgres feature](0026-keep-postgresql-as-the-only-backend-and-remove-the-postgres-feature.md)

## Context

Storage sits behind a backend trait (ADR 8), so the concrete database technology
is an implementation detail of an adapter rather than something the worker loop,
the registry, or the task layers depend on. The first and default adapter still
needs a concrete async runtime, PostgreSQL driver, connection pool, migration
tool, and TLS strategy.

The queue's hot path is a hand-tuned claim statement — an `UPDATE` over a `SELECT
… FOR UPDATE SKIP LOCKED` (ADR 3) — that also varies at runtime: the set of kinds
is a dynamic array and the priority floor is optional (ADR 20, ADR 22). The
adapter's tables also carry a configurable name prefix (ADR 6), and the project
prefers to keep C out of the dependency graph so that a static build has no native
dependency to satisfy.

## Decision

The default adapter is built on:

- **tokio** as the async runtime. The worker model already depends on it (the
  in-flight task set, select-based waiting, the cancellation token, spawning), so
  the runtime is not abstracted away.
- **tokio-postgres** as the driver: a low-level, pure-Rust client that gives full
  control over the SQL, which the dynamic claim statement needs. The main
  alternative, `sqlx`, offers compile-time-checked queries, but its checked-query
  macro does not apply to a statement whose kind list and priority floor vary at
  runtime, so that benefit would be lost while its build-time database requirement
  and heavier footprint would remain.
- **deadpool-postgres** as the connection pool. The adapter owns its connection
  parameters (a `tokio_postgres` config and a TLS connector) and builds both the
  pool and the dedicated `LISTEN` connection (ADR 4) from them, so the listener
  always uses the same transport as the pool and listening needs no separate
  configuration.
- **refinery** for migrations. Migrations are authored as ordinary SQL files,
  each using a prefix placeholder (for example `{{prefix}}`) wherever a table name
  appears. Because the tables carry a configurable prefix (ADR 6), the adapter
  reads each file, substitutes the configured prefix with a plain string replace,
  and runs the result through refinery's runner, with refinery's migration-history
  table name set per prefix. Independent prefixed queues in one database therefore
  track and apply their migrations independently. The SQL stays file-based and
  reviewable; refinery supplies the versioned, ordered, transactional application
  and the history tracking, and the only added step is the prefix substitution.
- **rustls** for TLS, with a no-TLS option for same-host deployments. A shared
  library connects to managed and remote databases that require TLS, and a
  pure-Rust TLS stack keeps C out of the graph where a native-TLS stack would not.
  The rustls stack and its convenience constructor sit behind an off-by-default
  `rustls` feature, so a plaintext-only consumer does not compile it; the general
  constructor still accepts any caller-supplied connector without the feature.
- **serde** and **serde_json** for the JSONB payload, carried state, and journal
  attachment.
- the **ulid** crate for identifiers, stored as text (ADR 2), and **chrono** for
  timestamps, mapped through tokio-postgres's chrono integration.

All of these are confined to the adapter. Nothing above storage names a driver, a
pool, or a SQL type; that boundary is the backend trait (ADR 8).

## Consequences

The adapter has full control of the claim and recovery SQL, which the queue's
correctness depends on, and the dependency graph stays pure-Rust with no C to
satisfy. Migrations stay reviewable SQL files parameterized only by a prefix placeholder,
and reuse a tested runner rather than a hand-rolled applier, while still
supporting prefixed names and per-prefix history, so multiple queues coexist and
migrate independently in one database. Because the stack lives behind the
backend trait, a different driver, pool, or even a different database could be
provided later as another adapter without touching the worker, registry, or task
layers. Taking tokio as a hard dependency rules out other async runtimes for
consumers, an accepted trade given its ubiquity and the worker model's reliance on
it.
