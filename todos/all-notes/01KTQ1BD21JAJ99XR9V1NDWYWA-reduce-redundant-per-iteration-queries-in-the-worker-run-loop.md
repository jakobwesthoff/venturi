---
title: "Reduce redundant per-iteration queries in the worker run loop"
kind: improvement
component: worker
impact: low
origin: review
tags: [performance]
---
# Reduce redundant per-iteration queries in the worker run loop

Found in review. The `'outer` loop in `Worker::run` (`src/worker/mod.rs`)
issues two avoidable queries on every spin. Both are efficiency-only:
correctness is unaffected. Both would be addressed together when the loop's
query cadence is reworked.

## 1. `recover_stale` runs once per loop spin

The loop calls `self.recover_stale().await` at the top of every iteration. The
`select!` returns after a single event (a handler finishing, a notify, a
timeout), so `find_stale` (`SELECT ... WHERE status = 'claimed' AND
claim_expires_at < now() ... LIMIT 100`) runs once per reaped handler and per
notification. Under a busy worker draining a backlog, that is roughly one
stale scan per job settled, per worker. With M workers, stale scanning scales
with total throughput rather than elapsed time, and it loads the lease index.

Proposed fix: gate `recover_stale` behind a minimum interval, so it scans only
when some interval relative to the lease has elapsed since the last scan.

## 2. `wait_duration` queries even when no slot is free

`wait_duration` runs every spin, including spins where `running.len() ==
concurrency` and no claim can happen regardless. On a busy worker, that adds
one `next_visible_at` query per settled job.

Proposed fix: skip the `next_visible_at` probe when every slot is occupied. The
wait then only needs to resolve on a handler finishing, a notification, or
shutdown.

## Code references

`src/worker/mod.rs`: the `'outer` loop, `recover_stale`, `wait_duration`.
`src/postgres/mod.rs`: `find_stale`, `next_visible_at`.
