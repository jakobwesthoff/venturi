# `stats()` snapshot is non-atomic across its three queries

## Problem

`PostgresStore::stats()` (`src/postgres/mod.rs`, ~lines 559-598) runs three
aggregate queries (pending-by-kind, claimed count, dead-by-kind) on separate
pooled connections. Each sees its own transaction snapshot, so a job moving
between states (e.g. `claimed -> completed`) in the window between two queries
can be counted in neither or both. The `Snapshot` doc implies a coherent
point-in-time view that the implementation does not guarantee.

## Suggested fix

Wrap the three queries in a single `REPEATABLE READ` transaction (or fold them
into one combined query) so the returned snapshot is internally consistent.

## Decision needed

Confirm whether callers actually need a consistent cross-state snapshot, or
whether approximate counts are acceptable for the intended monitoring use. If
approximate is fine, instead soften the `Snapshot` documentation.

Source: review finding, `src/postgres/mod.rs:559-598`.
