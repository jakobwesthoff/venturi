# 8. Isolate storage behind a backend trait

Date: 2026-06-07

## Status

Accepted

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
