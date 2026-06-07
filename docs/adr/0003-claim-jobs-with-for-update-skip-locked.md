# 3. Claim jobs with `FOR UPDATE SKIP LOCKED`

Date: 2026-06-07

## Status

Accepted

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
