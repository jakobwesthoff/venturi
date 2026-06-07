# 13. Let the task decide when to give up, with a configurable backstop

Date: 2026-06-07

## Status

Accepted

## Context

A fixed, queue-enforced attempt cap is arbitrary across heterogeneous kinds of
work. A task that can see its own run history (ADR 15) can decide for itself when
further retries are pointless and end the job with `TaskError::permanent`
(ADR 11). But a task that returns a retryable error for a failure it does not
recognise would retry forever, accumulating immortal rows and being revived by
stale-claim recovery. An absolute attempt cap exists precisely to reap these
runaway jobs that never decide to stop on their own.

## Decision

The queue does not enforce a per-attempt give-up policy as its primary mechanism.
A task abandons work by returning `TaskError::permanent`, typically based on its
run history.

As a failsafe, the worker carries an absolute attempt backstop that is **enabled
by default at a high value**, is configurable, and can be set to `None` to
disable entirely. The backstop counts **failed** executions, not total runs, so a
cooperative pause loop (ADR 11) never trips it. When a job's failure count reaches
the backstop, it transitions to `dead` (ADR 5).

The backoff strategy (its `base` and `cap`, ADR 12) has a worker-level default and
may be overridden per task. The jitter fraction `f` is worker-level.

## Consequences

The common case is the task ending itself precisely from its own history; the
backstop is a safety valve an operator controls without editing task code, and is
off only by explicit choice. Because the backstop counts failures, pausing and
polling do not consume it.
