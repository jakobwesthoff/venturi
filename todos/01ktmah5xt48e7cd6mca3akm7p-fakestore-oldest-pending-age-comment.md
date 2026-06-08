# Clarify how `FakeStore` computes `oldest_pending_age`

## Observation

`Snapshot::oldest_pending_age` is documented as the age of the oldest pending
job per kind, measured from enqueue time. The PostgreSQL adapter derives it from
`min(created_at)` (oldest enqueue time → maximum age). `FakeStore`
(`src/test_support.rs:294`) instead accumulates `max(age)` per kind.

The two are equivalent (the oldest enqueue time yields the maximum age), so there
is no functional discrepancy. But the `FakeStore` code carries no note of this,
which could trip up a maintainer who expects the `min(created_at)` framing the
Postgres adapter uses.

## Suggested action

Add a one-line comment in `FakeStore` noting that `max(age)` per kind is the
in-memory equivalent of the adapter's `min(created_at)`. Pure documentation.

Source: review finding, `src/test_support.rs:294`, `src/store.rs:487`.
