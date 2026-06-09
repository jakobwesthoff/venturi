# 6. Configure the queue client at construction

Date: 2026-06-07

## Status

Accepted

Relates to [4. Wake workers with LISTEN/NOTIFY and a polling fallback](0004-wake-workers-with-listen-notify-and-a-polling-fallback.md)

Relates to [24. Build the default storage adapter on tokio-postgres, deadpool, and refinery](0024-build-the-default-storage-adapter-on-tokio-postgres-deadpool-and-refinery.md)

## Context

A shared queue library is dropped into projects with different database setups,
and may need to coexist with other queues or unrelated tables in the same
database. Fixed compile-time names and intervals would prevent that.

## Decision

The queue client is configured when it is created. The configuration includes at
least: the database connection parameters (the connection config and TLS
connector, including the database it targets), the name of the queue table, and
the poll fallback interval (ADR 4). The adapter is given these parameters rather
than a pre-built pool, so it builds both its claim pool and its always-on
listening connection (ADR 4) from them. The full configuration surface is still
being defined; this ADR fixes that these are construction-time settings on the
client instance rather than global constants.

## Consequences

A configurable table name lets multiple independent queues, or a venturi queue
alongside unrelated tables, live in one database. Configuration lives on a
client instance the process holds, rather than on environment-wide defaults.
