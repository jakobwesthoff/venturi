# `merge_into` does not make the surviving job promptly claimable

One change request over `merge_into` (`src/postgres/mod.rs`,
`src/test_support.rs`): a Replace/With merge updates the surviving row's payload,
carry, and priority, but does not propagate the two things that govern *when and
whether* that merged-in work is picked up. Both are decisions about the same
method.

## 1. Merge into a paused/scheduled job inherits the future `visible_at`

When an immediate `enqueue()` merges into a candidate whose `visible_at` is far
in the future (a paused or scheduled job), `merge_into` does not touch
`visible_at`. The surviving row keeps the future visibility, so the
just-enqueued immediate work is not claimable until the pause expires —
contradicting `Queue::enqueue`'s "eligible to claim at once". Consistent between
the fake and PG, but undocumented (`src/postgres/mod.rs:638-655`,
`src/test_support.rs:412-416`, `src/queue.rs:168-192`).

## 2. A priority-raising merge sends no NOTIFY

`merge_into` never calls `self.notify(&tx)`, unlike `enqueue`/`settle`/`recover`.
A `Replace`/`With` that raises an existing pending job's priority (Normal → High)
does not wake parked workers to re-evaluate claim ordering; the upgrade isn't
picked up until the next poll (up to `poll_max`, default 30s).

## Decision needed (shared)

Both turn on the same question: should a merge that *advances* eligibility or
priority make the surviving job promptly claimable? If yes, the single change is:
in `merge_into`, when the merge advances visibility, set `visible_at =
min(existing, incoming)`, and when it makes the row newly/again promptly
claimable, `self.notify(&tx)` (matching `recover`). If no, document both
behaviors on `Queue::enqueue` / `Task::merge` / `Store::merge_into`. The merge
visibility/notify rule was never explicitly decided; confirm intent before
changing behavior.

Related but separate: suppressing the NOTIFY for *future-visible* plain enqueues
(see `enqueue-notifies-for-future-visible-jobs`) is the inverse of item 2 — the
same "NOTIFY exactly when claimable" principle, but a different code path.

Source: review findings, `src/postgres/mod.rs` `merge_into` vs `enqueue`/`recover`,
`src/queue.rs:168-192`.
