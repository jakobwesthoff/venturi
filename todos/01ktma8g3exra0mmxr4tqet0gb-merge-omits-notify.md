# A merge that raises priority sends no NOTIFY

## Problem

`merge_into` (`src/postgres/mod.rs`) updates the surviving row (payload, carry,
and now priority) but never calls `self.notify(&tx)`, unlike `enqueue`,
`settle`, and `recover`, which wake listening workers when they make work
claimable.

When a `Merge::Replace`/`With` raises an existing pending job's priority (e.g.
Normal upgraded to High), workers parked at `notifier.recv()` or sleeping until
their next poll do not wake to re-evaluate claim ordering. The job is not lost,
but a higher-priority upgrade is not picked up until the next poll cycle (up to
`poll_max`, default 30s).

## Notes

- The candidate is already pending, so a merge never makes a *new* row
  claimable; this is purely about re-prioritizing an already-claimable row.
  Whether that warrants a wakeup depends on how promptly priority upgrades
  should take effect.
- Fix: call `self.notify(&tx)` inside `merge_into` when `affected > 0`, matching
  `recover()`.

## Decision needed

Whether priority-raising merges should wake idle workers, or the next poll is an
acceptable latency for re-prioritization.

Source: review finding, `src/postgres/mod.rs` `merge_into` vs. `enqueue`/`recover`.
