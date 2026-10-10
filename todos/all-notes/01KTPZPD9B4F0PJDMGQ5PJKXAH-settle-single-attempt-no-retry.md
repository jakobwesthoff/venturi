# Settlement and shutdown-release are single-attempt

## Problem

When a handler finishes, `Worker::settle` writes the outcome exactly once
(`src/worker/mod.rs` `reap` → `settle`, the `settle failed` warn path). If
`store.settle` errors once (a pool blip, a restarted PostgreSQL), the job stays
`claimed` until its lease expires (default 15 minutes), is then recovered as a
*failed* execution (`failure_count` +1, `StaleRecovered` journaled), and the
handler re-runs work that already completed.

The shutdown `release` path (`Worker::release`) has the same shape: a single
failed `settle` abandons the job to lease expiry instead of making it
immediately reclaimable.

At-least-once semantics make the re-run legal, but the window is far larger than
necessary: the settlement value is already computed and, under the claim-epoch
guard, idempotent (a retried `settle` either applies once or no-ops on a guard
miss). A small bounded retry would shrink "one DB blip" from "15-minute delay +
spurious failure + duplicate side effects" to nothing.

## Options / decision needed

- Bounded retry (how many attempts, what backoff) around `store.settle` in
  `settle`/`release`. Needs a retry-policy decision (count, delay, whether to
  reuse the worker `Backoff`).
- Leave as-is and rely on lease recovery (current behavior), accepting the
  larger duplicate-execution window.

This is a deliberate behavior/policy choice, so it is captured here rather than
implemented blind.

Source: review finding, `src/worker/mod.rs` reap/settle/release.
