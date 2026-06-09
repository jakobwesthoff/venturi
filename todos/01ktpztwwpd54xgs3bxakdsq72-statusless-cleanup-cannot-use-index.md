# Status-less cleanup cannot use the history index

## Problem

`cleanup` without a `status` filter predicates on `finished_at IS NOT NULL AND
finished_at < $1` (`src/postgres/mod.rs` cleanup), which has no leading-column
match for the `(status, kind, finished_at)` index
(`migrations/V1__initial.sql`), so it sequential-scans the jobs table. Matters
only on large tables with periodic cleanup.

## Suggested fix

Add an index that leads with `finished_at`, or document that supplying a `status`
filter is preferred for large-table cleanup.

Source: review finding R4, `src/postgres/mod.rs` cleanup, `migrations/V1__initial.sql`.
