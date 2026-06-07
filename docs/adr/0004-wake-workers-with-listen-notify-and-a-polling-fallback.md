# 4. Wake workers with LISTEN/NOTIFY and a polling fallback

Date: 2026-06-07

## Status

Accepted

## Context

Workers must react promptly to newly enqueued jobs without polling the table in
a tight loop. PostgreSQL `LISTEN/NOTIFY` lets a producer signal waiting workers
on insert: each consumer holds a dedicated connection issuing `LISTEN`, and
producers `pg_notify` when they enqueue. `LISTEN/NOTIFY` delivery is best-effort,
so a notification can be missed (for example while a worker is between listens),
and a notify-only design would then stall a job indefinitely. A periodic poll as
a fallback bounds that worst case to the poll interval.

## Decision

venturi wakes workers with `LISTEN/NOTIFY` over a connection dedicated to
listening, separate from the pool used for claims. A periodic poll runs as a
fallback regardless of notifications. The poll fallback interval defaults to
30 seconds and is configurable when the queue client is created (ADR 6).

## Consequences

A worker needs at least one connection for listening in addition to its claim
connections. Notifications are a latency optimization, not a correctness
requirement: the poll fallback alone is sufficient to make progress, so a
dropped notification delays a job by at most the poll interval.
