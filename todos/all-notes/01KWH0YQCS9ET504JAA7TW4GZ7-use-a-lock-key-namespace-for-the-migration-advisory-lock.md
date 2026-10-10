---
title: "Use a lock key namespace for the migration advisory lock"
kind: improvement
component: postgres
origin: review
tags: [concurrency]
---
# Use a lock key namespace for the migration advisory lock

Found in the 2026-07-02 review. `migrate()` takes a single-argument `bigint` advisory lock, which shares the database-wide keyspace with every other application on that database, so a foreign lock with the same key can block venturi migrations silently.

## Problem

`migrate()` serializes concurrent startups with
`pg_advisory_lock($1)` keyed by `advisory_lock_id(prefix)`, an FNV-1a hash
of the table prefix reinterpreted as `i64`
(`src/postgres/mod.rs:193-198,835-846`).

PostgreSQL advisory locks are database-global and unnamespaced. The
single-argument `bigint` form venturi uses is the same keyspace every other
library or application on that database uses for its own `bigint` advisory
locks. venturi explicitly supports sharing one database (multiple prefixes,
`src/postgres/mod.rs:9-11`), which implies coexisting with other software.
If any cohabiting application takes a long-lived advisory lock whose key
happens to equal `advisory_lock_id(prefix)`, every venturi `migrate()` for
that prefix blocks until the foreign lock is released, with nothing in
either system pointing at the cause. The reverse holds too: while a long
venturi migration runs it can block a foreign application that collides.

This is a low-probability hazard (a random key collision in 2^64), but the
failure mode is a silent cross-application deadlock that is very hard to
diagnose, and PostgreSQL provides a cheap mitigation.

## Suggested fix

Use the two-argument form `pg_advisory_lock(classid int4, objid int4)` with
a fixed, venturi-specific `classid` magic constant and the prefix hash
folded to `int4` as `objid`. The two-key form is the conventional way to
namespace advisory locks per application; a collision then requires another
application to both use the two-key form and pick the same magic. Note the
key change is only a startup-serialization concern, not a data concern:
rolling this out across mixed venturi versions briefly means old and new
processes do not serialize against each other during migration, which is
worth a line in the changelog.

The leak and cancellation windows of the same lock are covered by the
advisory-lock leak todo; a structural rework there should adopt the two-key
form at the same time.

Source: review finding, `src/postgres/mod.rs:186-221,829-846`.

## Relations

- Relates to: [Remove the advisory-lock leak in migrate() on cancellation](01KTM8QQZ2YEHDDHMMFK79DWFP-remove-the-advisory-lock-leak-in-migrate-on-cancellation.md)
