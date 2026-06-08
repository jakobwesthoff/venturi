# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-06-08

Initial release.

### Added

- Durable, at-least-once job delivery on PostgreSQL, with jobs claimed via
  `FOR UPDATE SKIP LOCKED` so many workers contend without blocking.
- Typed tasks: a job is a single serializable struct that doubles as the
  payload, the deduplication identity, and the unit a handler receives.
- Four run outcomes: complete, cooperative pause (checkpoint and resume),
  retryable failure, and permanent failure.
- Fibonacci backoff with deterministic, RNG-free jitter, plus a per-task or
  worker-level give-up policy.
- Deduplication via a candidacy key and a full `merge` decision over the
  existing job's payload, carry, run count, and journal.
- Per-claim leases with automatic stale-claim recovery.
- Cooperative graceful shutdown that drains in-flight work before releasing.
- Scheduling with three priority tiers, weighted-slot anti-starvation, per-kind
  concurrency caps, and delayed/scheduled jobs.
- `LISTEN`/`NOTIFY` wakeups with a polling fallback.
- Append-only per-execution journal and a history query.
- Bulk cleanup and a live stats snapshot.
- Observability through `tracing` events and an optional `metrics` feature.
- Optional `rustls` feature adding the `connect_rustls` TLS constructor.
- PostgreSQL storage adapter (`postgres` feature, enabled by default) with
  schema migrations.

[0.1.0]: https://github.com/jakobwesthoff/venturi/releases/tag/v0.1.0
