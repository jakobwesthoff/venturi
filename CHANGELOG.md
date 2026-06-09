# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- `Store::enqueue` now rejects a `NewJob` whose `priority` is outside the
  supported `0..=2` tier range with a new `Error::InvalidPriority`, in both the
  PostgreSQL and in-memory adapters. Direct `Store` users who hand-build a
  `NewJob` get a typed error at the boundary instead of a backend-specific
  `CHECK`-constraint violation; the typed `Queue` path always passes a valid
  tier and is unaffected.

### Changed

- Trimmed the crate's `tokio` dependency from `features = ["full"]` to the set
  the library actually uses (`rt`, `sync`, `time`, `macros`), and dropped the
  unused direct `refinery` dependency (only `refinery-core` is used). Consumers
  that relied on venturi transitively enabling other `tokio` features should
  declare those features on their own `tokio` dependency. No API change.
- **Breaking:** run numbers are `u32` across the public surface. `JobRecord::run_count`,
  `JournalRecord::run_no`, `JournalAppend::run_no`, and the `run_no` argument of
  `Store::settle`/`Store::recover`/`Store::extend_lease` change from `i32` to
  `u32`, matching the already-`u32` handler-facing `Context::run_count` and
  `JournalEntry::run_no`. The signedness is now confined to the PostgreSQL
  adapter, which converts to the DB's signed `integer` column at the SQL
  boundary; a run number above `i32::MAX` there surfaces as the new
  `Error::RunNumberOutOfRange` rather than wrapping to a negative. Custom `Store`
  implementations and code reading these fields must adjust the type.
- **Breaking:** `TaskError::source` is renamed to `TaskError::cause`. The old
  name shadowed [`std::error::Error::source`] even though `TaskError` does not
  implement that trait, so a caller could invoke it expecting trait-based
  error-chain walking and silently get a method that does not participate in it.
- Index tuning (migration `V3__index_tuning`): the claim indexes now carry
  `visible_at` as a trailing key column so future-visible rows are filtered
  in-index without a heap fetch on the hot claim path; a partial
  `(finished_at) WHERE finished_at IS NOT NULL` index makes status-less cleanup
  an indexed range scan instead of a sequential scan; the dedup and per-job
  journal indexes gained trailing columns that eliminate a sort; and the unused
  `(kind, recorded_at)` journal index was dropped. Applied automatically on the
  next `migrate()`.
- Documented that deduplication merges are last-writer-wins under concurrent
  enqueue of the same `(KIND, dedup_key)`: the candidate read, `Task::merge`
  decision, and write are not one transaction, so racing enqueues can both merge
  (with `Merge::With`, one contribution can be lost). No behavior change; the
  contract is now stated on `Task::merge`.

### Fixed

- A corrupted migration-history row (malformed `applied_on` or `checksum`, e.g.
  from a manual edit, a partial restore, or a future refinery format change) now
  surfaces as a recoverable `Error::Migration` instead of panicking the
  migration runner, matching how the adapter handles every other malformed-DB
  case.
- A run that completes successfully but whose carry cannot be serialized to JSON
  is now journaled with an accurate note ("handler completed but its carry could
  not be serialized") and settled dead through the normal outcome path, instead
  of being misreported as an undispatchable job. (The job still goes to dead:
  the carry cannot be persisted and re-running would only fail to encode again.)

## 0.3.0 - 2026-06-09

### Added

- `Queue::job(id)` (and the underlying `Store::job`) fetches a single job by id,
  returning the full `JobRecord` including its `payload` and `carry`, or `None`
  when no such job exists. This is a primary-key point lookup for detail
  inspection, distinct from the filtered, paginated `Queue::jobs` history scan.

### Changed

- **Breaking:** `Store` gains a required `job` method. Custom `Store`
  implementations must add it; the bundled PostgreSQL adapter already does.
- **Breaking:** `Store::settle`, `Store::extend_lease`, and `Store::recover`
  take an additional `run_no` (claim epoch) argument that participates in their
  ownership guards. Custom `Store` implementations must thread it through.

### Fixed

- The settlement ownership guard now matches the claim epoch (`run_count`) in
  addition to the claiming worker's identity. A slow or aborted handler run can
  no longer settle a job that was reclaimed and re-run, even when the reclaiming
  worker shares the same `host:pid` identity (the common self-recovery case) or
  when two workers collide on identity.
- Stale-claim recovery now carries the same claim epoch in its guard, so a
  recovery computed from an older `find_stale` snapshot can no longer re-pend a
  claim that has since been reclaimed and re-run (which would have regressed the
  failure count and journaled a stale run number).
- An overflowing `pause`/retry delay now parks the job in the far future instead
  of collapsing to `now`, which had made a very long pause immediately eligible
  again and spun a tight claim/pause loop.
- `Error`'s `Display` now includes the underlying driver, pool, migration, and
  serialization messages instead of only a generic context line. Worker log
  lines and the dead-job journal note (which render `Display`) now carry the
  actual failure detail.
- The in-memory test `Store` now applies the `created_before` keyset cursor in
  its history query, matching the PostgreSQL adapter; it had ignored the cursor
  and returned the first page for every request.

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

