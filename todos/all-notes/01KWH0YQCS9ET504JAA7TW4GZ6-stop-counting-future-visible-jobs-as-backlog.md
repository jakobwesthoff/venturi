---
title: "Stop counting future-visible jobs as backlog"
kind: bug
component: postgres
status: needs-discussion
origin: review
tags: [observability]
---
# Stop counting future-visible jobs as backlog

Found in the 2026-07-02 review. `PostgresStore::stats()` counts scheduled, paused and backed-off jobs as pending backlog, so "backlog depth" reads high and the oldest pending age grows while nothing is stuck.

## Problem

`PostgresStore::stats()` aggregates pending state with
`WHERE status = 'pending'` only (`src/postgres/mod.rs:663-678`). A job whose
`visible_at` lies in the future (a scheduled `enqueue_at`, a paused job
waiting out `resume_in`, or a retry waiting out its backoff) is `pending`
too, so it is indistinguishable in the snapshot from work that is claimable
right now:

- `pending_by_kind` includes jobs that no worker could claim yet, so
  "backlog depth" reads high even when the queue is fully drained.
- `oldest_pending_age` is `now - min(created_at)` over that same set. A
  single job scheduled a week ahead makes the kind's "oldest pending age"
  grow steadily toward a week, although nothing is stuck. Any alerting built
  on this field ("oldest job waiting > 5 minutes") fires spuriously.

The `Snapshot` field docs (`src/store.rs:537-546`) describe the mechanics
("the age of the oldest pending job, measured from its enqueue time") but a
consumer reading "backlog depth" will not expect scheduled-for-next-week
jobs inside it.

## Fix (decision needed)

Decide what the snapshot should mean, then either:

- Restrict the aggregates to claimable work (`AND visible_at <= now()`),
  making the snapshot a true "ready backlog" view; optionally add a separate
  `scheduled_by_kind` count for future-visible jobs so that information is
  not lost. Note `oldest_pending_age` anchored on `created_at` still
  over-reports for retried jobs (a retry keeps its original `created_at`,
  see `migrations/V3__index_tuning.sql:23-26`); anchoring on `visible_at`
  would measure time-claimable instead.
- Or keep the current semantics and document on `Snapshot` that pending
  includes future-visible jobs and the age is enqueue-anchored, so alert
  authors know what they are measuring.

`FakeStore::stats` (`src/test_support.rs`) must be kept in agreement with
whichever choice is made.

Source: review finding, `src/postgres/mod.rs:658-698`,
`src/store.rs:535-546`, `src/queue.rs:48-55`.

## Relations

- Relates to: [The stats() snapshot is not atomic across its three queries](01KTM8QQZ2YEHDDHMMFK79DWFH-the-stats-snapshot-is-not-atomic-across-its-three-queries.md)
