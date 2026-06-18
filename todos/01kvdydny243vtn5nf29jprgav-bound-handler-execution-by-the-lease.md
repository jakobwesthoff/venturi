# Bound (or at least surface) handler execution by the lease

## Problem

The claim lease is a **hard per-job execution ceiling**, but a handler has no way
to observe it, so it can neither stop itself before the lease expires nor hand a
lease-aware deadline to a child subsystem it drives.

What the worker does today:

- The lease is set once, at claim time, from `self.config.lease` (default 15 min)
  or a per-task `Task::lease` override (`src/worker/mod.rs`: `claim_next(..,
  self.config.lease, ..)` and `apply_task_lease` → `extend_lease`, called **once**
  to stamp the override).
- There is **no renewal** while the handler runs. `extend_lease` is not called
  periodically; the only other lease touch is `recover_expired`
  (`src/worker/mod.rs`, "lease expired; worker {} presumed dead"), which re-pends
  an expired claim from *another* worker as a failed execution.
- The per-run `Context` carries only the worker's **graceful-shutdown**
  `CancellationToken` (`src/worker/registry.rs` builds `RunInput { cancel:
  shutdown.clone(), .. }`; `Context::cancelled`/`is_cancelled`/
  `cancellation_token` all observe that one token). It is **not** linked to the
  lease.

Consequences when a handler outruns its lease:

- The handler keeps running. Another worker's `recover_expired` reclaims the job
  and runs it again → **duplicate execution** (real, redundant work).
- The original run's eventual `settle` is rejected by the ownership/epoch guard
  (covered by `tests/recovery.rs` `ownership_guard_prevents_double_settle`), so
  the *state* stays consistent — but the wasted work already happened, and the
  second run may not be idempotent.

A handler today cannot implement "stop at `min(my_budget, lease_remaining)`"
because venturi exposes neither the lease deadline nor a lease-linked
cancellation. The handler only learns it lost the lease indirectly, after the
fact, via a guard-failed settle.

## Why it matters (consumer context)

A downstream consumer (latent-ink's Flows, running the `penstock` executor inside
a `flow.run` handler) documents the execution deadline as `min(now + budget,
venturi lease)` (its ADR 0055 / penstock decision 22). It **cannot** implement the
`lease` half: the handler hands `penstock::execute` a `&CancellationToken`, and
the only token available is the shutdown token from `Context::cancellation_token`.
So the execution is bounded purely by penstock's own wall-clock budget. This is
correct *only as long as* every handler's self-imposed budget is shorter than the
lease — which is true at defaults (30 s budget ≪ 15 min lease) but is an
unchecked, unobservable invariant, not something venturi enforces or surfaces.

## Design question

Decide what the lease *is*, then make it observable or enforced:

1. **A hard execution ceiling (today's de-facto behaviour).** The handler must
   finish within the lease or risk duplicate execution. If this is the contract,
   venturi should let the handler see and/or honour it.
2. **A liveness signal.** The lease only detects dead workers; a live worker
   renews it for as long as the handler runs, and a *separate* explicit per-job
   timeout bounds runaway work. This removes the duplicate-execution hazard for
   slow-but-alive handlers but is a larger semantic change.

## Options (not yet decided)

- **(a) Expose the lease deadline.** Add `Context::lease_deadline() -> Instant`
  (or `time_remaining() -> Duration`), stamped from the claim time plus the
  applied lease. Minimal and additive; the handler computes `min` itself and
  passes the result to whatever it drives. Downside: every long handler must
  remember to consult it; nothing enforces it.
- **(b) Lease-linked cancellation.** Make the run's token (and therefore
  `cancelled()` / `is_cancelled()` / `cancellation_token()`) fire on **either**
  graceful shutdown **or** the lease deadline, via a per-run timer the worker arms
  when it spawns the handler. This composes cleanly with the newly added
  `Context::cancellation_token`: a handler hands that one token to a child
  executor and the child is bounded for
  free, with no `min` arithmetic. Downside: a timer per in-flight job, and it
  commits venturi to interpretation (1) above.
- **(c) Lease renewal + explicit timeout.** A background heartbeat renews the
  lease while the handler runs (interpretation 2), plus an optional explicit
  per-job execution timeout that fires the run token. Largest change; decouples
  "worker is alive" from "job has run too long".

## Considerations

- **Clock domains.** The module header (`src/worker/mod.rs`) notes that claim
  eligibility and lease expiry are evaluated in **database** time while local
  waits use the monotonic clock, and that lease arithmetic is padded for skew. Any
  exposed deadline or per-run timer must be honest about which clock it is in: a
  `lease_deadline()` derived from a local `Instant` at claim time can drift from
  the DB-side expiry the recovery path uses. Prefer returning a conservative
  (earlier) local deadline so a handler that honours it stops *before* the DB
  considers the lease expired.
- **Interaction with `Pause`.** A handler that returns `Pause` mid-run already
  cooperates with shutdown; a lease-deadline signal should steer a long handler
  toward `Pause`/early return the same way `cancelled()` does, not toward a hard
  abort that loses the carry.
- **Existing safety net.** The ownership/epoch guard already prevents
  double-*settle*; this todo is about avoiding the wasted double-*execution* and
  giving handlers a way to self-bound, not about settle correctness.
- **`clamp_lease`.** Whatever is exposed should reflect the clamped, applied lease
  (including a `Task::lease` override), not the raw configured value.

## Suggested direction

Lean toward **(b)** — a run token that fires on shutdown *or* lease expiry —
because it composes with `Context::cancellation_token` and any child executor with
its own cancellation seam, and needs no `min` bookkeeping at call sites. Pair it
with **(a)** (`time_remaining()`) for handlers that want to self-pace rather than
be cut off. Hold (c) unless there is a concrete need for slow-but-alive handlers
to outlive a single lease. This is a deliberate semantic decision about the lease
contract and warrants an ADR before implementation.

Source: design discussion with the latent-ink consumer; `src/worker/mod.rs`
(claim/lease/recover paths), `src/worker/registry.rs` (`RunInput.cancel`),
`src/context.rs` (cancellation surface).
