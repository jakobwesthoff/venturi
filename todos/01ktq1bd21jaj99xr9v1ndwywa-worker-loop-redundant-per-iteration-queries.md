# Reduce redundant per-iteration queries in the worker run loop

One change request: the `'outer` loop in `Worker::run` (`src/worker/mod.rs`)
issues avoidable queries on every spin. Both are efficiency-only (correctness is
unaffected) and would be addressed together when reworking the loop's query
cadence.

## 1. `recover_stale` runs once per loop spin

The loop calls `self.recover_stale().await` at the top of every iteration. The
`select!` returns after a single event (a handler finishing, a notify, a
timeout), so `find_stale` (`SELECT ... WHERE status = 'claimed' AND
claim_expires_at < now() ... LIMIT 100`) runs once per reaped handler and per
notification. Under a busy worker draining a backlog that is roughly one
stale-scan per job settled, per worker; with M workers, stale scanning scales
with total throughput rather than elapsed time, loading the lease index.

Fix: gate `recover_stale` behind a minimum interval (scan only when some interval
relative to the lease has elapsed since the last scan).

## 2. `wait_duration` queries even when no slot is free

`wait_duration` runs every spin, including spins where `running.len() ==
concurrency` and no claim can happen regardless — one extra `next_visible_at`
query per settled job on a busy worker.

Fix: skip the `next_visible_at` probe when every slot is occupied (the wait then
only needs to resolve on a handler finishing, a notification, or shutdown).

Source: review findings, `src/worker/mod.rs` `'outer` loop / `recover_stale` /
`wait_duration`, `src/postgres/mod.rs` `find_stale` / `next_visible_at`.
