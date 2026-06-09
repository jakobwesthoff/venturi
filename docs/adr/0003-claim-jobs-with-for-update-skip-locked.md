# 3. Claim jobs with `FOR UPDATE SKIP LOCKED`

Date: 2026-06-07

## Status

Accepted

Relates to [5. Model job lifecycle with pending, claimed, completed, and dead states; retain all jobs](0005-model-job-lifecycle-with-pending-claimed-completed-and-dead-states-retain-all-jobs.md)

Relates to [19. Recover stale claims by lease expiry](0019-recover-stale-claims-by-lease-expiry.md)

Relates to [20. Run a bounded-concurrency claim and dispatch loop](0020-run-a-bounded-concurrency-claim-and-dispatch-loop.md)

Relates to [22. Schedule claims by priority with weighted-slot anti-starvation](0022-schedule-claims-by-priority-with-weighted-slot-anti-starvation.md)

Relates to [24. Build the default storage adapter on tokio-postgres, deadpool, and refinery](0024-build-the-default-storage-adapter-on-tokio-postgres-deadpool-and-refinery.md)

## Context

Workers run against the same queue table concurrently. Each must claim a
distinct job without blocking the others and without two workers claiming the
same job. PostgreSQL's `FOR UPDATE SKIP LOCKED` selects the next eligible row
while skipping rows another transaction already holds, which makes a single
atomic claim statement possible: the next eligible row is selected and updated
in one statement, and concurrent claimers pass over locked rows instead of
queuing behind them.

## Decision

venturi claims jobs with the same pattern: a single
`UPDATE … SET status = 'claimed', … WHERE id = (SELECT id … WHERE status =
'pending' … ORDER BY … LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING *`. The claim
is one statement; no separate read-then-write transaction is required.

## Consequences

Concurrent workers skip rows already locked by another claimer rather than
blocking, so claim throughput scales with worker count. A claimed row is
invisible to other claimers until it is released or recovered; the mechanism for
recovering rows whose worker died is a separate concern, not settled here. The
ordering inside the inner `SELECT` (priority, age) is defined by the schema and
scheduling decisions, also not settled here.
