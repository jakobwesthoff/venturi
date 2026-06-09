# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.2.0 - 2026-06-09

### Added

- Keyset cursor pagination for the history query: `HistoryFilter::created_before`
  takes the last-seen `(created_at, id)` and the query applies it as a
  `(created_at, id)` row-value bound, so history pages stay correct as rows are
  inserted or removed between requests (unlike an offset). A new
  `{prefix}_jobs_created` index on `(created_at, id)` backs the scan.
- `PostgresStore::with_max_pool_size` to bound the work pool's maximum number of
  connections; the constructors keep `deadpool`'s default when it is not called.

### Changed

- The history query now orders by `created_at DESC, id DESC` (previously
  `created_at DESC` alone), so rows sharing a `created_at` have a deterministic
  order — the basis for stable keyset pagination.

## 0.1.0 - 2026-06-08

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

