# Revisit handler-panic settlement (immediate vs lease recovery)

**Status:** decided-as-interim; a gap vs. the worker-model design.

## What the design says

A handler that ends in a panic should settle as a **failed execution** at the
task boundary (ADR 20 / worker-model.md): "panics are caught at the task boundary
and settled as a failed execution (abort-mode panics fall to lease recovery)."
So under unwind (the default), a panicking handler should be turned into a
retryable failure *immediately*, not left for lease recovery.

## What was implemented

The worker spawns each handler on a `JoinSet` and reaps with
`join_next_with_id`. On a panic, `join_next_with_id` yields `Err(JoinError)`; the
worker logs it and drops the job from its in-flight tracking, but **does not
settle it**. The job stays `claimed` until its lease expires and is then recovered
as `stale-recovered` (a failed execution with backoff). See `reap` in
`src/worker/mod.rs`.

So today: unwind-mode panics behave like abort-mode panics — they fall to lease
recovery — rather than settling immediately. Correctness is preserved (the job is
not lost), but recovery is delayed by up to the lease (default 15m) instead of
being prompt.

## Why

Catching a panic *inside* the spawned task (so it can settle promptly and still
know its job id) needs `FutureExt::catch_unwind` + `AssertUnwindSafe` (the
`futures` crate) or an equivalent. I avoided adding that in the initial build; the
`JoinError`-from-`join_next_with_id` path loses the ability to settle but keeps
the id only for tracking, not for a settle.

## Options

- Wrap the dispatched run future in `AssertUnwindSafe(fut).catch_unwind()` so the
  spawned task always returns a `FinishedRun`, mapping a caught panic to a
  retryable `TaskError` (immediate failed-execution settlement, journal note like
  "handler panicked"). Adds a `futures`/`futures-util` runtime dependency (or a
  hand-rolled catch).
- Leave as-is and document that panics are recovered by lease, recommending a
  shorter lease for panic-prone handlers.

## Decision needed

Whether prompt panic settlement is worth a small dependency + `UnwindSafe`
wrangling, or lease recovery is acceptable for the panic case.
