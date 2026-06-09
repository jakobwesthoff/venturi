# 5. Model job lifecycle with pending, claimed, completed, and dead states; retain all jobs

Date: 2026-06-07

## Status

Accepted

Relates to [3. Claim jobs with `FOR UPDATE SKIP LOCKED`](0003-claim-jobs-with-for-update-skip-locked.md)

Relates to [13. Let the task decide when to give up, with a configurable backstop](0013-let-the-task-decide-when-to-give-up-with-a-configurable-backstop.md)

Relates to [16. Record every execution and merge in an append-only journal table](0016-record-every-execution-and-merge-in-an-append-only-journal-table.md)

Relates to [18. Expose history query and cleanup APIs](0018-expose-history-query-and-cleanup-apis.md)

Relates to [19. Recover stale claims by lease expiry](0019-recover-stale-claims-by-lease-expiry.md)

## Context

A job moves through a small set of persisted states as it is enqueued, picked up
by a worker, and finished or abandoned. A minimal queue can model only the live
states (`pending`, `claimed`, and a failure terminal) and delete the row on
success, but that discards any audit trail of completed work and leaves
completion and failure retained on different terms. venturi keeps a durable
record (the journal, ADR 16) and needs one consistent retention story across both
terminal outcomes rather than retaining only failures.

## Decision

- Four states: `pending` (eligible to be claimed), `claimed` (held by a worker),
  `completed` (finished successfully), and `dead` (permanently failed or stopped
  by the backstop, ADR 13).
- All jobs are kept in a single jobs table through their terminal states; nothing
  is auto-deleted on completion or death. A partial index on `pending` keeps the
  claim path (ADR 3) scanning only live rows regardless of how many terminal rows
  accumulate.
- The row records a `finished_at` timestamp, set when the job reaches `completed`
  or `dead`, so terminal jobs are queryable by completion time (ADR 18).
- Removal of terminal jobs is an explicit operation (ADR 18), never automatic.

## Consequences

`completed` and `dead` are retained identically, so the retention story is
consistent. The jobs table is both the live queue and the durable job record:
listing past jobs is a filtered scan of it, while per-execution detail lives in
the journal (ADR 16). Table growth is bounded by the cleanup API rather than by
query performance, because the partial index isolates the hot claim path from
terminal rows.
