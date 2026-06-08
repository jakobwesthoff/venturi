# Revisit how the per-task lease override is applied

**Status:** decided-as-interim during implementation; works but has overhead.

## What the plan asked for

`Task::lease(&self) -> Option<Duration>` overrides the worker's default claim
lease so a task known to run long is not reclaimed mid-run (ADR 19).

## The tension

The claim is a single atomic statement that stamps `claim_expires_at = now +
lease` *before* the row's `kind`/payload is known to the worker — so the
per-instance lease cannot be applied inside the claim itself.

## What was implemented

After a successful claim, the worker:
1. asks the registry for the task's lease via an erased closure
   `Registry::lease_for(kind, payload)`, which **deserializes the payload again**
   purely to call `Task::lease()`;
2. if it returns `Some` and differs from the default, issues a second guarded
   `UPDATE` (`Store::extend_lease`) to re-stamp `claim_expires_at`.

So a lease-overriding claim costs an extra payload deserialize (always, to learn
whether there is an override) plus one extra round-trip (only when overriding).
See `apply_task_lease` in `src/worker/mod.rs` and `erased_lease` in
`src/worker/registry.rs`.

## Alternatives to weigh

- **Per-kind lease at registration** instead of per-instance: most tasks return a
  constant lease, so the override could be a value set on `register_with_lease`,
  known before the claim, and folded into the claim statement (no re-stamp, no
  re-deserialize). Downside: loses per-instance variation.
- **Keep per-instance but avoid the always-on deserialize:** only re-stamp when a
  kind is *registered as possibly overriding* (a per-kind flag), so uncapped
  common tasks pay nothing.
- **Accept the current cost** — the extra deserialize is in-memory JSON
  (microseconds) and the extra UPDATE only happens for overriding tasks, which
  are rare. May be fine as-is.

## Decision needed

Whether per-instance lease is worth the double-deserialize, or per-kind lease is
the better contract.
