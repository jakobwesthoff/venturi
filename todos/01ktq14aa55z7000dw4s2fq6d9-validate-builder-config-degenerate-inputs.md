# Validate builder/config setters against degenerate inputs

One change request: every public builder/config setter that takes a numeric or
duration value should reject or clamp degenerate inputs at the boundary (with a
documented contract or `Error::Config`), instead of letting them surface as a
runtime misbehaviour far from the call. These are all the same problem with the
same solution; addressing them is one pass over the builder surface.

## Affected setters and their failure modes

- **`PostgresStore::with_max_pool_size(0)`** (`src/postgres/mod.rs:126-130`):
  passes straight to `pool.resize(0)`; a zero-size pool can never hand out a
  connection, so every later `pool.get()` errors or waits forever.
- **`WorkerBuilder::lease(huge)`** (`src/postgres/mod.rs` claim_next/extend_lease):
  `lease.as_secs_f64()` feeds `now() + interval '1 second' * $n`; an absurd value
  (e.g. `Duration::from_secs(u64::MAX)`) overflows the interval multiply, so every
  `claim_next` returns `Error::Database` and the worker spins on "claim failed".
- **`WorkerBuilder::lease(0)`** (`src/worker/mod.rs:164-167`): stamps an
  already-expired lease at claim time, so any worker's `recover_stale` re-pends
  the job mid-run — guaranteed duplicate execution on every run. (Correctness,
  not just efficiency.)
- **`WorkerBuilder::poll_max(0)`** (`src/worker/mod.rs:157-160`): makes
  `wait_duration` return zero, busy-spinning the loop with `find_stale` + claim +
  `next_visible_at` queries every iteration.
- **`WorkerBuilder::register_capped(0)`** (`src/worker/mod.rs:138-144`): already
  clamped to `1` via `max.max(1)`, but silently and undocumented — unlike
  `concurrency`, whose 0-clamp is documented (`src/worker/mod.rs:146-152`).
- **`Backoff::new` degenerate base/cap** (`src/backoff.rs:30-33`): no validation.
  `Backoff::new(Duration::ZERO, cap)` makes every delay zero (a tight retry loop
  to the backstop); `cap < base` silently clamps every delay to `cap`, inverting
  intent. Neither is guarded or documented. Same "validate-or-document a
  constructor input" change, so addressed in the same pass (at minimum a
  `debug_assert!(base <= cap)` and a note on `base == 0`).

## Suggested fix

A single consistent policy across the surface: clamp to a sane minimum/maximum
(matching the existing `concurrency(0) -> 1` convention) or return
`Error::Config`, and document the contract on each setter. `lease(0)` and
`with_max_pool_size(0)` are correctness/liveness footguns and should be the
priority; the rest are efficiency or documentation.

Source: review findings R1/R6 and the zero-duration-knob and pool-size findings.
