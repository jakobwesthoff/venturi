# `failure_count` is `i32` while `run_count` was unified to `u32`

## Problem

Commit `5ddf0d7` ("Unify run numbers to u32 across the public surface") moved
run numbers to `u32`, with a dedicated boundary guard
(`Error::RunNumberOutOfRange`, `src/error.rs:55-64`) for the storage cast.
Failure counts were left as `i32` on the same public surface:

- `JobRecord.failure_count: i32` (`src/store.rs:172`) next to
  `run_count: u32` (`src/store.rs:170`)
- `Settlement::Retry { failure_count: i32, .. }` and
  `Settlement::Dead { failure_count: i32, .. }` (`src/store.rs:275-297`)
- `Store::recover(.., failure_count: i32, ..)` (`src/store.rs:423-430`)

Both counters have the same storage type (`integer NOT NULL DEFAULT 0`,
`migrations/V1__initial.sql:24-25`) and the same domain (non-negative,
increment-only). The asymmetry forces defensive code at call sites, e.g.
`recover_stale` does `job.failure_count.saturating_add(1)` and then
`next_failures.max(0) as u32` (`src/worker/mod.rs:546-547`) to feed the
backoff attempt, guarding against a negative value that cannot legitimately
occur.

## Fix (decision needed)

Unify `failure_count` to `u32` the same way run numbers were unified: `u32`
in the public types, checked cast at the storage boundary (reusing the
`RunNumberOutOfRange` pattern or a shared "count out of range" variant).
This is a pre-1.0 breaking change to public struct fields and the `Store`
trait, so it should ride along with the API-stability review
(`todos/01ktj9nym9e8r5x3sqmg76a4sm-public-api-surface-review.md`).

Source: review finding, `src/store.rs:170-172,275-297,423-430`,
`src/worker/mod.rs:546-547`, `migrations/V1__initial.sql:24-25`,
commit `5ddf0d7`.
