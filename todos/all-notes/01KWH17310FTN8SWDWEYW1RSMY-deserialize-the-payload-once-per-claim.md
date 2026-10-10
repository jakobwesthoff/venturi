---
title: "Deserialize the payload once per claim"
kind: improvement
component: worker
status: needs-discussion
origin: review
tags: [performance]
---
# Deserialize the payload once per claim

Found in the 2026-07-02 review. Every claimed job's payload is deserialized twice, once for the lease probe and once for the handler, and the lease probe runs on every claim even for tasks that never override `Task::lease`.

## Problem

Two independent code paths deserialize the same payload for each claimed
job:

1. `Worker::apply_task_lease` → `Registry::lease_for` → `erased_lease`
   (`src/worker/registry.rs:138-147`): clones the payload `Value` and runs
   `serde_json::from_value::<T>` on it, only to call `task.lease()` on the
   result.
2. `erased_run` (`src/worker/registry.rs:157`): deserializes the payload
   again to actually run the handler.

The lease-reader path runs on **every** claim (`apply_task_lease` is called
unconditionally after each claim, `src/worker/mod.rs:365,591-594`),
including for the overwhelming majority of tasks that never override
`Task::lease`. The default returns `None`, but finding that out costs a
full `Value` clone plus a complete typed deserialization per claim. For
large payloads this doubles the claim path's CPU cost for nothing.

## Fix (decision needed)

Options, roughly in order of invasiveness:

- Deserialize once: have the erased entry expose a combined "decode payload,
  return (task, lease)" step, or fold the lease read into `erased_run` and
  report the applied lease back to the worker before `handle` runs. Needs a
  small registry restructure since the lease must be known before the run
  future is spawned.
- Skip the lease probe for kinds that cannot override: not expressible
  today, because `Task::lease` is a method with a default body, so the registry
  cannot see "uses the default" at registration time. A
  `const HAS_LEASE_OVERRIDE: bool` (or registering the override explicitly
  on the builder, like the per-kind cap) would make the probe opt-in.
- At minimum, drop the `payload.clone()` by taking the reference-based
  deserializer path (`T::deserialize(&*payload)` via
  `serde_json::Value`'s `Deserializer` impl on references), which removes
  the allocation but keeps the double decode.

The semantics and naming of the same lease-override path are covered by the
lease-override cleanup todo; a restructure here should land together with
that cleanup.

Source: review finding, `src/worker/registry.rs:64-65,130-147,155-157`,
`src/worker/mod.rs:359-366,585-606`.

## Relations

- Relates to: [Clean up the lease-override path (`extend_lease` / `apply_task_lease`)](01KTQ1BD21JAJ99XR9V1NDWYWC-clean-up-the-lease-override-path-extendlease-applytasklease.md)
