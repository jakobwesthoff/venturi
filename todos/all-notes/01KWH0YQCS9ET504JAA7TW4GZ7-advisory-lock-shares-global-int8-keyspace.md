# Migration advisory lock shares the global `bigint` advisory keyspace

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

Related: `todos/01ktm8qqz2yehddhmmfk79dwfp-advisory-lock-leak-on-crash-comment.md`
covers the leak/cancellation windows of the same lock; a structural rework
there should adopt the two-key form at the same time.

Source: review finding, `src/postgres/mod.rs:186-221,829-846`.
