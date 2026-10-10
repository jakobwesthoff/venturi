---
title: "Settlement and shutdown-release are single-attempt"
kind: improvement
component: worker
status: needs-discussion
origin: review
tags: [retry]
---
# Settlement and shutdown-release are single-attempt

Found in review. `Worker::settle` and `Worker::release` write their outcome once, so one failed store write leaves the job to lease expiry and can re-run completed work. A bounded retry would shrink that window, but the retry policy is a decision for the maintainers.

## Problem

When a handler finishes, `Worker::settle` writes the outcome exactly once (`src/worker/mod.rs`, `reap` → `settle`, the `settle failed` warn path). If `store.settle` errors once (a pool blip, a restarted PostgreSQL), the job stays `claimed` until its lease expires (default 15 minutes). It is then recovered as a *failed* execution (`failure_count` +1, `StaleRecovered` journaled), and the handler re-runs work that already completed.

The shutdown `release` path (`Worker::release`) has the same shape: a single failed `settle` abandons the job to lease expiry instead of making it immediately reclaimable.

At-least-once semantics make the re-run legal, but the window is far larger than necessary. The settlement value is already computed and, under the claim-epoch guard, idempotent: a retried `settle` either applies once or no-ops on a guard miss. A small bounded retry would shrink "one DB blip" from "15-minute delay, spurious failure and duplicate side effects" to nothing.

Source: review finding, `src/worker/mod.rs` `reap`, `settle` and `release`.

## Open questions

- Bounded retry (how many attempts, what backoff) around `store.settle` in `settle` and `release`. This needs a retry-policy decision: count, delay, and whether to reuse the worker `Backoff`.
- Leave it as-is and rely on lease recovery (current behavior), accepting the larger duplicate-execution window.

This is a deliberate behavior and policy choice, so it is captured here rather than implemented blind.
