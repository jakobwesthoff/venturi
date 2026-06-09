# `enqueue` notifies even for future-visible jobs

## Problem

`enqueue` issues the NOTIFY unconditionally (`src/postgres/mod.rs:237`), so every
scheduled/delayed enqueue (a `visible_at` in the future) wakes every idle worker
on the prefix for a claim plus a `next_visible_at` round trip that finds nothing
claimable yet.

The settle path already gates its notify on re-pending transitions
(`notifies_on_repend`, `src/postgres/mod.rs:165-170`); enqueue could analogously
skip the notify when `visible_at` is in the future. Efficiency only, no
correctness impact.

## Suggested fix

Only emit the enqueue NOTIFY when `visible_at <= now()` (immediate work); for a
future `visible_at`, the worker's `next_visible_at`-driven sleep already wakes it
at the right time.

Source: review finding, `src/postgres/mod.rs:237`.
