# Keep merge churns a dead row version

## Problem

The Keep branch of `merge_into` issues `SET dedup_key = dedup_key`
(`src/postgres/mod.rs:689-696`) purely to test whether the candidate is still
pending. That writes a new row version (plus WAL) on every Keep merge.

## Suggested fix

Use an `EXISTS` / `SELECT ... FOR UPDATE` inside the transaction to report
pendingness without rewriting the row. Low impact unless Keep merges are very
frequent.

Source: review finding, `src/postgres/mod.rs:689-696`.
