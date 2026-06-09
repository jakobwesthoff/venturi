# Concurrent enqueues of one dedup key can both apply a merge

## Decision (accepted for now)

Last-writer-wins under concurrent same-key enqueue is **accepted** and now
documented as the contract on `Task::merge` (and a code comment in
`Queue::submit`). No serialization is added. Revisit only if a concrete use case
needs exactly-one-merge semantics — at which point weigh whether we are ready to
accept the downsides of the `SELECT ... FOR UPDATE` option below (holding a
transaction across the user's `merge` callback and added lock contention on the
enqueue hot path), or design a better approach. The carry-reset documentation
note (below) is still worth doing.

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

## Related: the fallback enqueue resets carry

The same race window has a documentation gap on its fallback path. In
`apply_merge` (`src/queue.rs:168-193`), when `merge_into` returns `false` because
the candidate was claimed between `dedup_candidate()` and the merge, the code
falls back to `insert(incoming, ...)` — a brand-new job that does not inherit the
now-claimed candidate's `run_count`, `failure_count`, or carry. A `Merge::With`
that intended to *continue* the in-flight work instead starts from `Default`
carry. Intentional ("no work is lost"), but surprising and undocumented.

Extend `apply_merge`'s doc comment to state the fallback is a fresh job with
default carry, not a continuation. Pure documentation; no behavior change.

## Related test gap

The fallback path in `apply_merge()` (when `merge_into` returns `false` because
the candidate was claimed in the race window, `src/queue.rs:124-126`) is not
exercised by any test in `tests/dedup.rs`. Worth a regression test that forces
a claim between `dedup_candidate()` and `apply_merge()`.

## Decision needed

Document-only vs. strengthen to `FOR UPDATE` (the carry-reset note above is
document-only regardless).

Source: review finding, `src/queue.rs:96-193`, `src/postgres/mod.rs:619-676`.
