# 12. Retry with a visible-at gate, Fibonacci backoff, and proportional jitter

Date: 2026-06-07

## Status

Accepted

Relates to [11. Signal task outcome through Completed/Pause results, notes, and retryable-by-default errors](0011-signal-task-outcome-through-completed-pause-results-notes-and-retryable-by-default-errors.md)

Relates to [13. Let the task decide when to give up, with a configurable backstop](0013-let-the-task-decide-when-to-give-up-with-a-configurable-backstop.md)

Relates to [14. Source retry jitter deterministically from the job identifier](0014-source-retry-jitter-deterministically-from-the-job-identifier.md)

Relates to [19. Recover stale claims by lease expiry](0019-recover-stale-claims-by-lease-expiry.md)

## Context

Re-enqueueing a failed job straight back to `pending` with no delay leaves it
immediately re-claimable, and under sustained failure this becomes a tight loop
hammering whatever the job depends on. For a job that wraps an external call this
is especially wrong: the right behaviour is to back off and retry later. A delay
mechanism that gates eligibility on a timestamp solves both the retry case and,
as a side effect, delayed and scheduled enqueue.

## Decision

A `visible_at` timestamp column gates eligibility. The claim query gains
`AND visible_at <= now`. The same column lets a producer enqueue a job that
becomes eligible only at a future time.

On a retryable failure at attempt `n`, `visible_at = now + actual`, where:

```
delay  = min(base * (fib(n) - 1), cap)
actual = delay*(1 - f) + jitter_offset(0, delay*f)
```

- The `fib(n) - 1` shaping makes the first two retries immediate (multiplier 0)
  before the curve climbs: `0, 0, 1, 2, 4, 7, 12, …` times `base`.
- `cap` clamps the base delay **before** jitter, so the realized delay is always
  within `[delay*(1-f), delay]` and never exceeds `cap`. Proportional jitter is
  self-bounded; there is no separate jitter cap.
- The jitter offset is derived deterministically from the job's identifier and
  attempt (ADR 14), then added to `now` to produce the stored `visible_at`. The
  claim query stays deterministic. Jitter on a zero delay is a no-op, so the two
  immediate retries stay immediate for any `f`.

The backoff strategy is a pluggable policy mapping an attempt number to a base
delay; Fibonacci is the first and only initial implementation. Jitter is
orthogonal to the strategy: the worker applies a single fraction `f` to whatever
delay the policy returns. Parameters (`base`, `cap`, `f`) are configured per
ADR 13.

## Consequences

Failed batches are decorrelated rather than retrying in lockstep, while the
deliberate quick-first-retries shape is preserved. `cap` is a hard ceiling on any
realized retry delay. `visible_at` participates in the claim predicate, so it is
part of the claim index in the schema decision. The same column gives delayed and
scheduled enqueue without further machinery.
