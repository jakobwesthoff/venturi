# 22. Schedule claims by priority with weighted-slot anti-starvation

Date: 2026-06-07

## Status

Accepted

## Context

Jobs carry a priority (`Task::priority()`, ADR 9, ADR 17), and the claim orders by
priority then age (ADR 3), so the highest-priority oldest job is taken first.
Strict priority ordering has a known failure mode: a sustained stream of
higher-priority work starves lower-priority jobs indefinitely. A general queue
should not let a low-priority job wait forever behind a busy high-priority tier.

## Decision

**Three priority tiers.** Priority is a fixed enum — `High`, `Normal`, `Low` —
defaulting to `Normal`. Three tiers cover the practical range, keep the claim
ordering a clean indexed `(priority, created_at)`, and make tier-based fairness
tractable.

**Weighted-slot anti-starvation, on by default.** The worker does not claim
strict-highest-priority on every claim. It keeps a claim counter and, on a cadence
set by a `priority_ratio`, lowers the priority floor for a single claim so lower
tiers receive guaranteed slots. Higher tiers are favored by roughly the ratio per
tier while every tier keeps a nonzero long-run share, so nothing starves. A claim
that reserves a lower tier and finds it empty falls back to an unconstrained claim,
so a reserved slot is never wasted when that tier has no work. The constraint is a
`priority >= floor` filter that rides the existing claim index, so there is no new
index and no schema change.

**Disabling gives strict priority.** `priority_ratio` is configured at worker
construction with a sane default that keeps anti-starvation active. Setting it off
(a disabled / `None` value) makes every claim unconstrained, which is exactly
strict priority ordering. The default favors higher tiers strongly while
guaranteeing lower tiers a small periodic share; it is tunable per worker, and
larger ratios approach strict behavior.

## Consequences

Out of the box a low-priority job cannot be starved indefinitely by a busy
high-priority tier, which is the safe default for a general queue, at the cost that
strict priority is approximate by default: a lower-tier job occasionally runs ahead
of a waiting higher-tier one. A worker that needs absolute precedence disables the
ratio for pure strict ordering. Because the fairness logic is a per-worker choice
of priority floor rather than a global re-prioritisation, the claim path stays a
simple indexed lookup. An age-based "effective priority" scheme was considered and
rejected: its time-varying ordering cannot ride the claim index efficiently.
