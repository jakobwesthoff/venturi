# An absurd lease poisons every claim

## Problem

`WorkerBuilder::lease` (and `Task::lease`) flow into the claim SQL as
`now() + interval '1 second' * $2` with `lease.as_secs_f64()`
(`src/postgres/mod.rs` claim_next, extend_lease). A
`lease(Duration::from_secs(u64::MAX))` overflows the interval multiplication, so
every `claim_next` returns `Error::Database` and the worker spins forever on
"claim failed; will retry".

## Suggested fix

Apply a sane upper bound (or return `Error::Config`) at the builder, before the
value reaches the database. Same footgun family as the `with_max_pool_size(0)`
todo.

Source: review finding R1, `src/postgres/mod.rs` claim_next/extend_lease.
