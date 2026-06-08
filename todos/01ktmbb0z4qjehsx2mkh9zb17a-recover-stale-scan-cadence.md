# Decouple stale-claim scanning from the per-iteration loop cadence

## Problem

The worker loop calls `self.recover_stale().await` at the top of every `'outer`
iteration (`src/worker/mod.rs`). The loop's `select!` returns after a *single*
event: one `join_next_with_id` (a handler finished), one notify, or one timeout.
So `recover_stale` runs once per loop spin, i.e. after every reaped handler and
every notification.

`recover_stale` issues `find_stale`
(`SELECT ... WHERE status = 'claimed' AND claim_expires_at < now() ORDER BY
claim_expires_at LIMIT 100`, `src/postgres/mod.rs`). Under a busy worker with
concurrency N draining a backlog, that is roughly one stale-scan per job settled,
per worker, on top of the claim/settle queries. With M workers against one queue,
stale scanning scales with total throughput rather than with elapsed time, adding
avoidable load and contention on the lease index.

It is correctness-neutral: the scan is idempotent and guarded by
`claim_expires_at < now()`. This is purely an efficiency concern.

## Suggested fix

Gate `recover_stale` behind a minimum interval (only scan when some interval has
elapsed since the last scan), decoupling recovery cadence from per-job loop
spins. Pick the interval relative to the lease duration.

Source: review finding, `src/worker/mod.rs` `'outer` loop + `recover_stale`,
`src/postgres/mod.rs` `find_stale`.
