# 4. Wake workers with LISTEN/NOTIFY and a polling fallback

Date: 2026-06-07

## Status

Accepted

Relates to [6. Configure the queue client at construction](0006-configure-the-queue-client-at-construction.md)

Relates to [20. Run a bounded-concurrency claim and dispatch loop](0020-run-a-bounded-concurrency-claim-and-dispatch-loop.md)

Relates to [21. Shut down gracefully by draining cooperatively, then releasing](0021-shut-down-gracefully-by-draining-cooperatively-then-releasing.md)

Relates to [24. Build the default storage adapter on tokio-postgres, deadpool, and refinery](0024-build-the-default-storage-adapter-on-tokio-postgres-deadpool-and-refinery.md)

## Context

Workers must react promptly to claimable work without polling the table in a
tight loop. A job becomes claimable not only on a fresh enqueue but also when a
running job is returned to the pending set: a retry after a failure, a paused
job's scheduled resume, a release back to the pool on shutdown, and a stale claim
reclaimed after its lease expires. A worker that has already computed its wait
does not see such a change until its next poll unless it is signalled.

PostgreSQL `LISTEN/NOTIFY` provides that signal: a consumer holds a connection
issuing `LISTEN`, and a writer queues a `pg_notify`. A queued notification is
delivered only when its transaction commits and is discarded on rollback, so
emitting it inside the same transaction as the row change makes the wakeup atomic
with the change that warranted it. Delivery is still best-effort once committed —
a notification can be missed, for example while a worker is between listens — and
a notify-only design would then stall a job indefinitely. A periodic poll as a
fallback bounds that worst case to the poll interval.

## Decision

venturi wakes workers with `LISTEN/NOTIFY` over a connection dedicated to
listening, separate from the pool used for claims. Every write that returns a job
to the claimable set queues a `pg_notify` within the same transaction as the row
change: enqueue, a retry, a pause's resume, a release, and a stale-claim
recovery. Terminal transitions (completed, dead) produce no claimable work and
emit nothing. The notification carries no payload; a woken worker re-queries its
claimable set rather than acting on the message.

The listening connection is built from the same connection parameters and TLS
connector as the claim pool (ADR 24), so it is always present and uses the same
transport as every other query, with no separate endpoint to configure. A
periodic poll runs as a fallback regardless of notifications; its interval
defaults to 30 seconds and is configurable when the queue client is created
(ADR 6).

## Consequences

A worker opens one connection for listening in addition to its claim connections.
Notifications are a latency optimization, not a correctness requirement: the poll
fallback alone is sufficient to make progress, so a dropped notification delays a
job by at most the poll interval. Because each wakeup commits with the row change
that produced it, a committed claimable row is never left without its
notification, and a rolled-back change emits none.
