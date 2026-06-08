# Concurrent enqueues of one dedup key can both apply a merge

## Problem

`dedup_candidate()` and `merge_into()` are not performed atomically
(`src/queue.rs:96-161`, `src/postgres/mod.rs:619-676`). Two concurrent enqueues
of the same `(KIND, dedup_key)` can both read the same pending candidate and
both succeed at `merge_into()` (which guards only on `status = 'pending'`). For
`Replace`/`With` strategies the second merge silently overwrites the first.

This is inherent to the current non-transactional dedup design and is tolerable
for the documented use cases, but it is currently undocumented.

## Options

1. Document the race in `Task::merge`'s docs: under concurrent enqueue of the
   same dedup key, merges are last-writer-wins and not serialized.
2. If exactly-one-merge-per-candidate is required, take a `SELECT ... FOR
   UPDATE` on the candidate row before the merge decision so concurrent
   enqueues serialize on it.

## Related test gap

The fallback path in `apply_merge()` (when `merge_into` returns `false` because
the candidate was claimed in the race window, `src/queue.rs:124-126`) is not
exercised by any test in `tests/dedup.rs`. Worth a regression test that forces
a claim between `dedup_candidate()` and `apply_merge()`.

## Decision needed

Document-only vs. strengthen to `FOR UPDATE`.

Source: review finding, `src/queue.rs:96-161`, `src/postgres/mod.rs:619-676`.
