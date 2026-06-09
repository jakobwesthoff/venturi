# 13. Let the task decide when to give up, with a configurable backstop

Date: 2026-06-07

## Status

Accepted

Relates to [5. Model job lifecycle with pending, claimed, completed, and dead states; retain all jobs](0005-model-job-lifecycle-with-pending-claimed-completed-and-dead-states-retain-all-jobs.md)

Relates to [11. Signal task outcome through Completed/Pause results, notes, and retryable-by-default errors](0011-signal-task-outcome-through-completed-pause-results-notes-and-retryable-by-default-errors.md)

Relates to [12. Retry with a visible-at gate, Fibonacci backoff, and proportional jitter](0012-retry-with-a-visible-at-gate-fibonacci-backoff-and-proportional-jitter.md)

Relates to [15. Pass an execution context with run history, typed carried state, and a journal attachment](0015-pass-an-execution-context-with-run-history-typed-carried-state-and-a-journal-attachment.md)

Relates to [16. Record every execution and merge in an append-only journal table](0016-record-every-execution-and-merge-in-an-append-only-journal-table.md)

Relates to [17. Split the task abstraction into Task and Handler](0017-split-the-task-abstraction-into-task-and-handler.md)

Relates to [19. Recover stale claims by lease expiry](0019-recover-stale-claims-by-lease-expiry.md)

Relates to [21. Shut down gracefully by draining cooperatively, then releasing](0021-shut-down-gracefully-by-draining-cooperatively-then-releasing.md)

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
