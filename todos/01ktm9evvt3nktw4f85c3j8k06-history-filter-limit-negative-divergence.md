# `HistoryFilter::limit` negative value diverges between stores

## Problem

`HistoryFilter.limit` is `Option<i64>` (`src/store.rs:459`). The two `Store`
implementations disagree on negative values:

- `FakeStore::query_jobs` (`src/test_support.rs:253-255`) clamps via
  `limit.max(0) as usize`, silently treating a negative limit as 0.
- `PostgresStore::query_jobs` (`src/postgres/mod.rs:516-522`) binds the raw
  `i64` into `LIMIT $N`, which PostgreSQL rejects with a runtime error.

A user who tests against `FakeStore` will not see the failure that Postgres
surfaces in production.

## Suggested fix (decision needed)

1. Change the field type to `Option<u64>`, eliminating the negative domain at
   compile time (cast to `i64` for the Postgres bind, saturating). This is a
   public-API change; acceptable pre-1.0.
2. Or add a runtime clamp/guard in `PostgresStore::query_jobs` matching
   `FakeStore`.

Prefer option 1 if a breaking change is acceptable, since it removes the invalid
state rather than papering over it.

Source: review finding, `src/store.rs:459`, `src/postgres/mod.rs:516-522`,
`src/test_support.rs:253-255`.
