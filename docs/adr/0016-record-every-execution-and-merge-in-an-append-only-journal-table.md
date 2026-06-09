# 16. Record every execution and merge in an append-only journal table

Date: 2026-06-07

## Status

Accepted

Relates to [5. Model job lifecycle with pending, claimed, completed, and dead states; retain all jobs](0005-model-job-lifecycle-with-pending-claimed-completed-and-dead-states-retain-all-jobs.md)

Relates to [10. Deduplicate with a candidacy key and a full-task merge decision](0010-deduplicate-with-a-candidacy-key-and-a-full-task-merge-decision.md)

Relates to [11. Signal task outcome through Completed/Pause results, notes, and retryable-by-default errors](0011-signal-task-outcome-through-completed-pause-results-notes-and-retryable-by-default-errors.md)

Relates to [13. Let the task decide when to give up, with a configurable backstop](0013-let-the-task-decide-when-to-give-up-with-a-configurable-backstop.md)

Relates to [15. Pass an execution context with run history, typed carried state, and a journal attachment](0015-pass-an-execution-context-with-run-history-typed-carried-state-and-a-journal-attachment.md)

Relates to [18. Expose history query and cleanup APIs](0018-expose-history-query-and-cleanup-apis.md)

Relates to [19. Recover stale claims by lease expiry](0019-recover-stale-claims-by-lease-expiry.md)

Relates to [21. Shut down gracefully by draining cooperatively, then releasing](0021-shut-down-gracefully-by-draining-cooperatively-then-releasing.md)

## Context

Storing only failures, as an `errors` array on the job row, and deleting the row
on success loses all trace of completed work and keeps the failure history inline
on a row that is rewritten on every transition. venturi keeps all jobs (ADR 5) and
needs a hole-free record of everything that happens to a job, queryable on its own
and retained alongside the job.

## Decision

Every execution, and every applied deduplication merge (ADR 10), appends one row
to an append-only **journal** table keyed by the job's id. An entry records:

- `job_id`, `kind` (denormalized, so the journal is queryable by kind without
  joining jobs), the run number, and a timestamp;
- `outcome`: `completed`, `paused`, `retried`, `dead`, `stale-recovered`,
  `released`, or `merged`. `released` records a clean shutdown handoff (ADR 21)
  and `merged` records a deduplication merge; both are lifecycle events rather
  than executions and carry the run count at the time so they order correctly
  against runs;
- `note: Option<String>` — the run's conclusion, taken from the `Outcome` or, on
  failure, from the `TaskError`'s message (ADR 11);
- `attachment: Option<Value>` — structured evidence set via `ctx.set_attachment`
  during the run (ADR 15).

Entries are written with plain `INSERT`s, independent of the queue row's claim
lock. The failure history a task reads to decide its give-up policy (ADR 13,
ADR 15) is the failed entries of this journal; there is no separate per-row error
array.

## Consequences

The jobs row stays fixed-size and is not rewritten with a growing log on every
transition, and journal inserts do not contend with claims. The journal is the
job's full event log behind the retained jobs row (ADR 5): one entry per concluded
run, plus a `merged` entry whenever an enqueue coalesced into the job and a
`released` entry on a clean shutdown handoff, so those events are visible whether
or not the job had already run, and there are no holes.
`note` and `attachment` come from their natural sources, the outcome and the
context respectively (ADR 11, ADR 15), so entries share one shape. The
denormalized `kind` lets the journal be queried directly (ADR 18). Journal entries
are removed with their job under unified cleanup (ADR 18).
