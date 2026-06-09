# Worker/lease builder input validation gaps

Two builder inputs accept degenerate values that surface only as runtime
failures, in the same family as the `with_max_pool_size(0)` footgun todo.

## 1. An absurd lease poisons every claim

`WorkerBuilder::lease` (and `Task::lease`) flow into the claim SQL as
`now() + interval '1 second' * $2` with `lease.as_secs_f64()`
(`src/postgres/mod.rs` claim_next, extend_lease). A
`lease(Duration::from_secs(u64::MAX))` overflows the interval multiplication, so
every `claim_next` returns `Error::Database` and the worker spins forever on
"claim failed; will retry". A sane upper bound (or `Error::Config`) at the
builder would catch it before it reaches the database.

## 2. `register_capped::<T>(0)` clamps silently

`register_capped` clamps `max` to `1` via `max.max(1)`
(`src/worker/mod.rs:138-144`) with no doc note, unlike `concurrency`, whose
0-clamp to 1 is documented (`src/worker/mod.rs:146-152`). Either document the
clamp on `register_capped` or reject 0.

Source: review findings R1, R6.
