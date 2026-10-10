---
title: "`observability::settled` is counted before the settle actually applies"
kind: bug
component: worker
status: needs-discussion
origin: review
tags: [observability, concurrency]
---
# `observability::settled` is counted before the settle actually applies

Found in review. `Worker::settle` emits the settled metric and event before the store write, so a guard miss or a transient store error still counts as a settlement. The fix (emit after `Ok(true)`) is clear, but what to emit on a guard miss is still undecided.

## Problem

In `Worker::settle` the settled metric and event are emitted *before* the store write (`src/worker/mod.rs`, lines 673-687):

```rust
let outcome = journal_outcome_for(&settlement);
crate::observability::settled(&finished.kind, outcome, duration);   // emitted here
...
self.store
    .settle(finished.id, &self.identity, finished.run_no, settlement, journal)
    .await?;                                                         // applied here
```

So a guard miss (the claim was reclaimed, which the epoch guard makes easier to reason about) or a transient store error still increments the settled counter and logs a settled event, even though no transition occurred. Metrics then drift from the journal under contention.

Source: review finding, `src/worker/mod.rs` lines 673-687.

## Suggested fix

Emit `observability::settled` only after `store.settle` returns `Ok(true)`. `settle` already returns the applied/skipped boolean, so thread it through and the metric reflects applied settlements only.

## Open questions

Decide what, if anything, to emit on a guard miss (for example a separate "settle skipped" counter).
