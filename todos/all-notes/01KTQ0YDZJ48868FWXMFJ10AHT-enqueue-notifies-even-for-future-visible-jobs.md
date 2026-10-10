---
title: "`enqueue` notifies even for future-visible jobs"
kind: improvement
component: postgres
impact: low
origin: review
tags: [performance]
---
# `enqueue` notifies even for future-visible jobs

Found in review. `enqueue` sends its NOTIFY even when `visible_at` is in the future, which wakes idle workers for nothing. Gating it the way the settle path already gates its notify removes that wasted wakeup. The change is for efficiency only.

## Problem

`enqueue` issues the NOTIFY unconditionally (`src/postgres/mod.rs`, line 237). Every scheduled or delayed enqueue (a `visible_at` in the future) wakes every idle worker on the prefix for a claim plus a `next_visible_at` round trip that finds nothing claimable yet.

The settle path already gates its notify on re-pending transitions (`notifies_on_repend`, `src/postgres/mod.rs`, lines 165-170). Enqueue could analogously skip the notify when `visible_at` is in the future. This is an efficiency change with no correctness impact.

## Suggested fix

Emit the enqueue NOTIFY only when `visible_at <= now()` (immediate work). For a future `visible_at`, the worker's `next_visible_at`-driven sleep already wakes it at the right time.

Source: review finding, `src/postgres/mod.rs` line 237.
