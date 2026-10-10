# Revisit the priority anti-starvation rotation algorithm

**Status:** decided-as-interim; the plan specified the *behaviour*, I chose the
exact algorithm.

## What the plan asked for

Weighted-slot anti-starvation (ADR 22): a claim counter + `priority_ratio`
periodically lowers the priority floor so lower tiers get guaranteed slots
(higher tiers favoured ~ratio per tier), with fallback to an unconstrained claim;
`priority_ratio = None` ⇒ strict priority.

## What was implemented

`floor_for(counter, ratio)` in `src/worker/mod.rs`:

- `None`, or ratio `< 2` ⇒ floor 0 (admit all tiers, high-first) = strict.
- ratio `r ≥ 2`: per claim counter `c`,
  - `c % (r*r) == 0` ⇒ floor 2 (reserve Low only),
  - else `c % r == 0` ⇒ floor 1 (reserve Normal + Low, exclude High),
  - else ⇒ floor 0.
- A reserved-but-empty claim falls back to an unconstrained claim
  (`claim_with_fallback`), so a reserved slot is never wasted.

The counter is per-worker in-memory and increments once per claim attempt.

## Things to reconsider

- **Is the `r` / `r²` cadence the intended "≈ratio per tier" distribution?** With
  `r=4` (default), High is favoured but the exact long-run shares of
  Normal/Low were chosen heuristically, not derived. A property test asserts only
  that every tier is served within an `r²` window
  (`rotation_serves_every_tier_in_a_window`), not specific ratios.
- **Counter semantics:** it increments per *attempt*; empty-queue ticks still
  advance it. Should reservation be driven by *successful* claims instead, or be
  per-tier-aware?
- **Only two reserved levels (Normal-floor, Low-floor).** With three tiers this is
  fine, but if tiers ever grow, the scheme needs generalizing.
- **Interaction with per-kind caps and the kind filter:** the floor is applied
  across the already-cap-filtered kind set; confirm that is the intended
  composition.

## Decision needed

Confirm the rotation gives the fairness profile the user wants, or replace with a
more principled weighted scheme (e.g. explicit per-tier token buckets).
