# 21. Shut down gracefully by draining cooperatively, then releasing

Date: 2026-06-07

## Status

Accepted

## Context

A worker runs up to `N` in-flight handler tasks (ADR 20). On a stop request — a
deploy, a termination signal, or a programmatic stop — it must wind down without
losing work or leaving jobs wedged in `claimed`. A library must not seize the
process's signal handlers, since the surrounding application owns those.

## Decision

**Trigger.** The worker exposes a programmatic shutdown signal (a cancellation
token the loop observes, or an equivalent handle method); the consuming
application wires termination signals to it. venturi installs no operating-system
signal handlers of its own.

**Cooperative drain, then release.** On shutdown the worker:

1. stops claiming new jobs;
2. raises a cooperative cancellation signal visible to every in-flight handler and
   waits up to a configurable `shutdown_timeout` for handlers to wind down on
   their own terms. A handler that observes the signal typically returns `Pause`
   to checkpoint its carry, or `Complete` if it can finish; these settle through
   the normal path (ADR 11), losing no progress;
3. at the timeout, force-aborts any handler still running and releases its job.

**Release semantics.** A release from a clean shutdown is not a failure: the
operator chose to stop and the job did not misbehave. A released job is recorded
in the journal as a `released` event (ADR 16), does not count toward the failure
backstop (ADR 13), and returns to `pending` with `visible_at = now` so another
worker picks it up immediately. This is distinct from stale-claim recovery
(ADR 19), which represents a crash and does count as a failure with backoff.
Carried state is the last persisted value (ADR 15); mid-run mutations are
discarded.

**Cooperative signal.** The execution context exposes the shutdown signal in two
forms: `ctx.is_cancelled()` to poll at safe points in step- or loop-structured
work, and `ctx.cancelled().await` to react inside a `select!` even while blocked
in a long await. A handler that ignores the signal is simply force-released at the
timeout.

**Ownership guard.** Settlement and release are conditioned on the worker still
holding the claim (matching the claim owner and the `claimed` status). A handler
that is slow or being aborted cannot settle or release a job that another worker
has already reclaimed.

After all in-flight jobs have settled or been released, the worker tears down its
dedicated listen connection (ADR 4) and returns.

## Consequences

A cooperative handler shuts down cleanly within the grace window and keeps its
progress through `Pause`, so a forced `released` becomes the exception rather than
the rule. A job is never left wedged in `claimed` after a clean stop: it is either
settled or promptly re-pended, so a rolling restart hands work off in seconds
rather than waiting for lease expiry. The ownership guard makes the
shutdown-release path and the stale-recovery path safe against double-settlement.
Because the trigger is a signal the application supplies, venturi composes with
whatever signal or lifecycle management the surrounding service already has.
