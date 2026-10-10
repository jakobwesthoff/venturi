---
title: "Add per-kind rate control for task kinds"
kind: feature
component: worker
status: needs-discussion
horizon: someday
tags: [rate-limit]
---
# Add per-kind rate control for task kinds

Some task kinds must be throttled to a rate, independent of how many run at once. Rate control was deferred on 2026-06-07 and stays parked until there is a concrete need. Per-kind concurrency caps are already decided in ADR 0023; rate control is a separate concern.

## Problem

Some task kinds must be throttled to a rate (for example, a kind that calls an
external service capped at N requests per minute), independent of how many run
concurrently. This is distinct from a concurrency cap (max in-flight at once),
which ADR 0023 handles.

## Key design insight to carry over

Enforce at claim time, consistent with concurrency caps (ADR 0023): the worker
should not claim a kind whose rate budget is spent. Those jobs stay `pending`
until the budget refills, so they incur no claim, slot, or lease cost while
waiting. The loop's wait timeout must account for the next refill time, the same
way it already accounts for the next `visible_at`.

## The local-vs-global split (the crux)

- **Local per-worker token bucket:** simple, claim-time, in-memory, resets on
  restart. But it is per-worker, so the effective global rate is the per-worker
  rate times the number of workers. Correct only for a single worker per kind, or
  a budget divided manually across workers.
- **Global (cross-worker) rate limiting:** correct across all workers, but needs
  a database-coordinated shared bucket/counter with refill coordination and
  contention on the claim path, and it is not cleanly atomic with the
  `SKIP LOCKED` claim. Materially heavier.

A rate limit that models an external shared resource is usually only *correct*
when enforced globally, which is exactly the expensive case.

## Options considered

1. Local per-worker token bucket now, defer global. (Matches a simple bucket; per-worker caveat.)
2. Defer all rate control. **(Chosen for now.)**
3. Build global DB-coordinated rate limiting now. (Most capable, heaviest.)

## Interim workaround

A consumer can self-throttle inside a handler by acquiring a rate-limiter held in
the shared worker state. Caveat: a claimed job blocking on the limiter occupies a
worker slot and ticks its lease while it waits (the claimed-and-blocked problem
ADR 0023 avoids for concurrency caps), so it is only acceptable at low volume.

## Open questions for when this is picked up

- Local vs global vs both; default.
- Where configured: at registration (like the concurrency cap, ADR 0023) for a
  local throttle, or as a kind-intrinsic property for an external limit.
- Bucket algorithm (token vs leaky), refill model, and where bucket state lives
  (in-memory vs persisted).
- Interaction with the wait-timeout and the `visible_at` eligibility gate.

## Related

- ADR 0023 (per-kind concurrency caps, claim-time, local).
- `docs/design/worker-model.md` (claim/dispatch loop, wait timeout).
- `docs/design/indexes.md` (claim access path).

## Revisit when

A concrete need for a per-kind rate limit appears, for example a kind that calls
an external service capped at N requests per minute.
