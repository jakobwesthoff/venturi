# Minor review findings, batch 2 (low severity)

Low-severity items from the second full-codebase review pass. None is a
correctness defect.

## 1. `wait_duration` queries even when no slot is free

`Worker::run` calls `wait_duration` every loop spin, including spins where
`running.len() == concurrency` and no claim can happen regardless of the answer
(`src/worker/mod.rs` run loop → `wait_duration` → `next_visible_at`). That is one
extra query per settled job on a busy worker. Same family as the `recover_stale`
cadence todo, but a distinct query (`next_visible_at`), so noted separately.
Consider skipping the `next_visible_at` probe when every slot is occupied.

## 2. Dedup-candidate tie-break diverges fake vs PG (low confidence)

`FakeStore::dedup_candidate` breaks an equal `created_at` deterministically by id
(`min_by_key((created_at, id))`), while the adapter's `ORDER BY created_at
LIMIT 1` leaves ties to the planner (`src/postgres/mod.rs` dedup_candidate). Only
observable with colliding timestamps; same fake-vs-PG divergence class as the
`HistoryFilter::limit` todo.

## 3. Status-less cleanup cannot use the history index

`cleanup` without a `status` filter predicates on `finished_at IS NOT NULL AND
finished_at < $1` (`src/postgres/mod.rs` cleanup), which has no leading-column
match for the `(status, kind, finished_at)` index (`migrations/V1__initial.sql`),
so it sequential-scans the jobs table. Matters only on large tables with periodic
cleanup. Consider an index that leads with `finished_at`, or documenting that a
`status` filter is preferred for large-table cleanup.

## 4. `tokio` pulls `features = ["full"]` in a library

`Cargo.toml` enables `tokio`'s `full` feature, forcing fs/net/process/signal/
io-std onto every consumer; the crate needs roughly rt, sync, time, macros. Fits
the existing `postgres`-feature dependency-hygiene todo's theme. Trim to the
features actually used.

## 5. Notifier reconnect spins at ~1 Hz during a full DB outage

`PgNotifier::reconnect` sleeps ~1s on failure and returns, so `recv` resolves
roughly once per second during an outage; each resulting loop spin fires three
failing queries (`find_stale`, claim, `next_visible_at`) plus warn logs
(`src/postgres/notify.rs:85-96`, `src/worker/mod.rs` run loop). Bounded and
self-healing, but a noisy ~1 Hz error spin rather than the intended `poll_max`
cadence. Consider backing off the reconnect, or not treating a reconnect-failure
return as a wakeup.

Source: review findings R2, R3, R4, R5, R7.
