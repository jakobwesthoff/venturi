# 23. Cap per-kind concurrency locally at claim time

Date: 2026-06-07

## Status

Accepted

## Context

A worker has one overall concurrency limit `N` across all the kinds it has
registered (ADR 20). Some kinds need a tighter, kind-specific cap, typically
because each of their handlers consumes a slot in a small local resource held by
the worker process (for example a pool of a few expensive instances). Running such
a kind in a separate worker can bound it, but that cannot express "at most 2 of
kind X and many of kind Y in the same worker," and it forces the capped kind out
of a shared worker.

A cap can be enforced in two places. If a handler acquires a permit after the job
is claimed, the job occupies a worker slot and its lease runs while it waits for
the permit, wasting capacity and risking false stale recovery. Enforcing at claim
time avoids this: a job of an at-capacity kind is simply not claimed and stays
pending.

## Decision

A registered kind may carry a per-kind concurrency cap, set at registration on the
worker (for example, registering the kind with a maximum concurrency). The cap is
configured at registration rather than declared on the task type, because it
reflects this worker's local resource rather than an intrinsic property of the
kind. The cap is local to the worker; across multiple workers the effective limit
is the cap times the number of workers.

The worker tracks the in-flight count per kind. When filling slots it narrows the
claim filter to kinds below their cap: the claim's kind set becomes the registered
kinds whose in-flight count is under their cap, with uncapped kinds always
included. A capped kind that is at its limit is excluded from the claim until one
of its in-flight jobs settles, so its jobs remain pending rather than
claimed-and-idle. This is in-memory bookkeeping, and the narrowed kind set rides
the existing claim filter and index, so there is no schema change and no new
index.

Rate control, throttling a kind over time rather than capping concurrency, is a
separate concern and is deferred (tracked as a todo).

## Consequences

One worker can run heterogeneous caps, such as a small cap on a resource-bound
kind alongside high concurrency for others, without splitting into multiple
workers. Because the cap is local, a deployment that needs a hard global ceiling
on a kind must either run a single worker for that kind or wait for a future
global mechanism. Capped jobs wait in `pending` and incur no claim, slot, or lease
cost while they wait. The per-kind in-flight counts are worker-local state, reset
on restart, which is correct because a worker's claims are released or recovered
when it exits.
