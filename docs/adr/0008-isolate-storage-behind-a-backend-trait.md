# 8. Isolate storage behind a backend trait

Date: 2026-06-07

## Status

Accepted

Amended by [26. Keep PostgreSQL as the only backend and remove the postgres feature](0026-keep-postgresql-as-the-only-backend-and-remove-the-postgres-feature.md)

Relates to [7. Provide a layered architecture with independently usable layers](0007-provide-a-layered-architecture-with-independently-usable-layers.md)

Relates to [18. Expose history query and cleanup APIs](0018-expose-history-query-and-cleanup-apis.md)

Relates to [24. Build the default storage adapter on tokio-postgres, deadpool, and refinery](0024-build-the-default-storage-adapter-on-tokio-postgres-deadpool-and-refinery.md)

## Context

venturi targets PostgreSQL and will use a PostgreSQL client library for its first
implementation. The persistence technique should not be welded into the worker
loop and task layers, so that a different storage technique could be provided
later without rewriting everything above it.

## Decision

The storage operations are defined by a backend trait. Everything above storage
(the worker loop, the task registry, dispatch) depends only on that trait, never
on SQL or on driver types directly. A PostgreSQL adapter implements the trait as
the first and primary backend. The concrete PostgreSQL driver and pool are a
separate, not-yet-fixed choice; `tokio-postgres` with a `deadpool` connection
pool is the expected starting point.

## Consequences

The trait's method set defines the contract every backend must satisfy, so it
can only be finalized once the queue operations are settled. Driver and
SQL-dialect specifics stay inside the adapter. A test or alternative backend can
implement the trait without PostgreSQL. This is the storage seam of the layered
architecture (ADR 7).
