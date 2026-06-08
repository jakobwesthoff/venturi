# Document the advisory-lock leak window in `migrate()`

## Problem

The migration advisory lock in `migrate()` (`src/postgres/mod.rs:162-183`) is
session-scoped. If the calling process panics between `pg_advisory_lock` and
`pg_advisory_unlock`, the lock stays held until that pooled connection is
recycled or closed. Concurrent workers calling `migrate()` would block on the
lock until then.

This is a known PostgreSQL advisory-lock limitation, not a defect, but the code
does not acknowledge it.

## Suggested fix

Add a code comment near the lock acquisition noting the crash-window behavior
and that recovery relies on the pool recycling / closing the connection. No
behavior change required.

Source: review finding, `src/postgres/mod.rs:162-183`.
