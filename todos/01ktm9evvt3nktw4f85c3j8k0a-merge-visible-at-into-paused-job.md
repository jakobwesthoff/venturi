# An immediate enqueue merged into a paused job stays invisible

## Problem

When an immediate `enqueue()` merges (Replace/With) into a paused candidate whose
`visible_at` is far in the future, `merge_into` (`src/postgres/mod.rs:638-655`,
`src/test_support.rs:412-416`) does not touch `visible_at`. The surviving row
keeps the pause's future visibility, so the just-enqueued work is not claimable
until the pause expires, even though the caller enqueued it for immediate
processing.

The behavior is consistent between `FakeStore` and `PostgresStore`, but it is not
documented in `Queue::enqueue`, `Task::merge`, or `Store::merge_into`.

## Suggested action (decision needed)

1. Document the behavior explicitly: a merge into a paused job inherits the
   pause's `visible_at`.
2. Or, for Replace/With merges, advance visibility so a superseding immediate
   enqueue becomes claimable. If changed, pick the rule deliberately (e.g.
   `visible_at = min(existing, incoming)` to honor "immediate", vs. `max(...)`).

The visibility rule for merges was not explicitly decided; confirm intent before
changing behavior.

Source: review finding, `src/postgres/mod.rs:638-655`, `src/queue.rs:168-192`.
