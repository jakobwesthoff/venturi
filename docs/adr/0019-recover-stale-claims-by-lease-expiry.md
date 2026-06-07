# 19. Recover stale claims by lease expiry

Date: 2026-06-07

## Status

Accepted

## Context

At-least-once delivery (ADR 3) means a worker can claim a job and then die or
hang before settling it, leaving the row stuck in `claimed` indefinitely unless
something reclaims it. The queue therefore needs to detect an abandoned claim and
return the job to the pending pool.

The simplest detection is a single fixed timeout compared against the claim time.
That works, but it is crude for heterogeneous workloads: a task that legitimately
runs longer than the timeout is falsely reclaimed and executed a second time.
Detection based on process identity, checking whether the claiming process is
still alive, only works on a single host, because process ids carry no meaning
across machines. It therefore does not generalise to a multi-process,
multi-host deployment.

## Decision

**Lease by per-claim expiry.** At claim time the worker stamps
`claim_expires_at = now + lease`. The lease has a worker-level default and an
optional per-task override (`Task::lease()`), so a task known to run long can
request a longer lease. Recovery reclaims any row where
`status = 'claimed' AND claim_expires_at < now`. Detection is timeout-only, with
no process-liveness check, so the mechanism behaves identically on one host or
many. The claiming worker's identity is recorded for diagnostics and for the
journal note. There is no lease renewal or heartbeat in this iteration; the
per-claim expiry column leaves room to add renewal later without a schema change.

**Recovery treatment.** A recovered run is treated as a failed execution:

- it appends a `stale-recovered` entry to the journal (ADR 16), noting the
  expired lease and the worker presumed dead;
- it counts toward the failure backstop (ADR 13), because a worker that
  repeatedly dies on the same job is the poison case the backstop exists to
  catch;
- the job returns to `pending` with backoff applied (`visible_at = now +
  backoff`, ADR 12), so a transient crash does not immediately re-crash a healthy
  worker.

Carried state needs no special handling: it is durable only at settle points
(ADR 15), so a recovered run resumes from the last persisted carry and any
mid-crash mutations are discarded.

**Trigger.** Recovery runs opportunistically at the start of every claim, so the
system is self-healing without a dedicated process. It is also exposed as a
manual operation for an external sweeper or administrative use.

## Consequences

A long-running task is accommodated by a longer lease rather than being falsely
reclaimed; long work that can checkpoint should instead pause (ADR 11), releasing
the claim between steps. A genuinely poisonous job that keeps killing workers
advances the failure budget and is eventually marked `dead` (ADR 13, ADR 5).
Recovery and ordinary retry share the same backoff and journal treatment, so a
crashed run and a failed run read consistently in the history. Opportunistic
recovery adds one bounded, indexed query per claim; it can be throttled or
shifted onto the manual sweeper if that cost ever matters. The per-claim expiry
timestamp and the worker-identity column are schema concerns settled with the
schema. `Task::lease()` joins `priority` and the backoff override as a per-task
setting on the state-free `Task` trait (ADR 17).
