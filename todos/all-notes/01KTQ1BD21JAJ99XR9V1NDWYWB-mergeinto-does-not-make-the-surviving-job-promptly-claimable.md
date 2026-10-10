---
title: "`merge_into` does not make the surviving job promptly claimable"
kind: bug
component: postgres
status: needs-discussion
origin: review
---
# `merge_into` does not make the surviving job promptly claimable

Found in review. One change request over `merge_into` (`src/postgres/mod.rs`,
`src/test_support.rs`): a Replace/With merge updates the surviving row's payload,
carry, and priority, but does not propagate the two things that govern when and
whether that merged-in work is picked up. Both are decisions about the same
method.

## 1. Merge into a paused/scheduled job inherits the future `visible_at`

When an immediate `enqueue()` merges into a candidate whose `visible_at` is far
in the future (a paused or scheduled job), `merge_into` does not touch
`visible_at`. The surviving row keeps the future visibility, so the
just-enqueued immediate work is not claimable until the pause expires. This
contradicts `Queue::enqueue`'s "eligible to claim at once". The behaviour is
consistent between the fake and PG, but undocumented (`src/postgres/mod.rs:638-655`,
`src/test_support.rs:412-416`, `src/queue.rs:168-192`).

## 2. A priority-raising merge sends no NOTIFY

`merge_into` never calls `self.notify(&tx)`, unlike `enqueue`, `settle` and
`recover`. A `Replace`/`With` that raises an existing pending job's priority
(Normal to High) does not wake parked workers to re-evaluate claim ordering. The
upgrade is not picked up until the next poll (up to `poll_max`, default 30s).

## Decision needed (shared)

Both items turn on the same question: should a merge that advances eligibility
or priority make the surviving job promptly claimable? If yes, the single change
is: in `merge_into`, when the merge advances visibility, set `visible_at =
min(existing, incoming)`, and when it makes the row newly or again promptly
claimable, call `self.notify(&tx)` (matching `recover`). If no, document both
behaviours on `Queue::enqueue`, `Task::merge` and `Store::merge_into`. The merge
visibility and notify rule was never explicitly decided, so confirm the intent
before changing the behaviour.

## Code references

`src/postgres/mod.rs`: `merge_into` compared with `enqueue` and `recover`.
`src/queue.rs:168-192`.

## Relations

- Relates to: [`enqueue` notifies even for future-visible jobs](01KTQ0YDZJ48868FWXMFJ10AHT-enqueue-notifies-even-for-future-visible-jobs.md), the enqueue-side counterpart of item 2: the same "NOTIFY exactly when claimable" principle on a different code path.
