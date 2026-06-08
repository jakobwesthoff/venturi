# `run_no` is `i32` in the store but `u32` in the context API

## Problem

The run-number type differs across the API surface:

- `store::JournalRecord::run_no` and `JournalAppend::run_no` are `i32`
  (`src/store.rs`).
- `Context::run_count()` is `u32` (`src/context.rs`).

The conversions at `src/context.rs` (`record.run_no.max(0) as u32`) and
`src/worker/mod.rs` (`job.run_count.max(0) as u32`) guard against negative
values, so there is no incorrect behavior below `i32::MAX` (~2 billion runs,
practically unreachable). But the signed/unsigned split is undocumented and can
surprise implementors of an alternate `Store`.

## Suggested action

Either document the signed-vs-unsigned choice on `JournalRecord::run_no` /
`JournalAppend::run_no` (PostgreSQL `integer` is signed, hence `i32` at the
store boundary; the handler-facing API exposes `u32`), or unify the types if a
single representation is preferred.

Source: review finding, `src/store.rs`, `src/context.rs`, `src/worker/mod.rs`.
