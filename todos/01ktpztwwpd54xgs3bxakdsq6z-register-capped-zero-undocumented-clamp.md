# `register_capped::<T>(0)` clamps silently

## Problem

`register_capped` clamps `max` to `1` via `max.max(1)`
(`src/worker/mod.rs:138-144`) with no doc note, unlike `concurrency`, whose
0-clamp to 1 is documented (`src/worker/mod.rs:146-152`).

## Suggested fix

Either document the clamp on `register_capped` or reject `0`.

Source: review finding R6, `src/worker/mod.rs:138-144`.
