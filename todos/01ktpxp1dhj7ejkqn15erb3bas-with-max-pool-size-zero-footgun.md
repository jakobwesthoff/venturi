# `with_max_pool_size(0)` is silently accepted

## Problem

`PostgresStore::with_max_pool_size` (`src/postgres/mod.rs:126-130`) passes its
argument straight to `pool.resize(max_size)` with no lower bound. A value of `0`
yields a pool that can never hand out a connection, so every subsequent
`pool.get()` either errors or waits forever (deadpool's exact behavior for a
zero-size pool was not verified). The failure surfaces far from the call that
caused it.

## Suggested fix

Either clamp with `max_size.max(1)` (mirroring how `concurrency(0)` is clamped
to `1` in the worker builder, `src/worker/mod.rs:149-152`) or reject `0` with
`Error::Config`. Clamping matches the existing builder convention; document
whichever is chosen on the method.

Source: review finding, `src/postgres/mod.rs:126-130`.
