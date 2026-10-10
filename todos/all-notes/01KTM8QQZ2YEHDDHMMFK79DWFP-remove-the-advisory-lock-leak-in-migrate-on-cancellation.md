---
title: "Remove the advisory-lock leak in migrate() on cancellation"
kind: bug
component: postgres
status: needs-discussion
impact: high
origin: review
tags: [concurrency]
---
# Remove the advisory-lock leak in migrate() on cancellation

Found in a review. When the caller drops `migrate()` between taking and releasing its advisory lock, the healthy pooled connection returns with the session lock still held. Every later `migrate()` then blocks on that connection until it dies, so the code comment claiming the lock releases on connection close is wrong for this path. The fix needs a structural change and a decision on approach.

## Problem

The migration advisory lock in `migrate()` (`src/postgres/mod.rs:179-206`) is a
session-scoped `pg_advisory_lock` taken on a pooled connection. It leaks the
lock back into the pool in two ways. Established by reading the code.

1. **Panic window.** A panic between `pg_advisory_lock` and `pg_advisory_unlock`
   leaves the lock held until that pooled connection is recycled or closed.

2. **Cancellation window (the more serious one).** `migrate()` is an ordinary
   future. If the caller drops it between lock and unlock, for example through a
   `tokio::select!` or `timeout` around startup, the *healthy* connection returns
   to the deadpool with `RecyclingMethod::Fast` still holding the session lock.
   deadpool reuses healthy connections indefinitely, so the lock is not
   released by "connection close": every subsequent `migrate()` in every
   process blocks on that specific pooled connection until it happens to die.
   The current code comment ("the lock releases on connection close") does not
   hold for this path.

## Suggested fix

Prefer a structural fix over a comment:

- Take the lock inside a transaction with `pg_advisory_xact_lock`, so a drop or
  rollback releases it automatically. Caveat: refinery's runner drives its own
  per-migration transactions (`src/postgres/migrations.rs:88-100`), so the lock
  and the migration cannot trivially share one transaction. This needs design.
- Or acquire the lock on a dedicated connection that is explicitly discarded
  (not returned to the pool) rather than recycled, so a drop closes it.

Either is more involved than a comment, and it needs a decision on the approach.

Source: review finding, `src/postgres/mod.rs:179-206`.

## Relations

- Relates to: [The stats() snapshot is not atomic across its three queries](01KTM8QQZ2YEHDDHMMFK79DWFH-the-stats-snapshot-is-not-atomic-across-its-three-queries.md), another review finding in the postgres store.
