# `observability::settled` is counted before the settle actually applies

## Problem

In `Worker::settle` the settled metric/event is emitted *before* the store write
(`src/worker/mod.rs:673-687`):

```rust
let outcome = journal_outcome_for(&settlement);
crate::observability::settled(&finished.kind, outcome, duration);   // emitted here
...
self.store
    .settle(finished.id, &self.identity, finished.run_no, settlement, journal)
    .await?;                                                         // applied here
```

So a guard miss (the claim was reclaimed — now more reachable to reason about
with the epoch guard) or a transient store error still increments the settled
counter and logs a settled event, even though no transition occurred. Metrics
then drift from the journal under contention.

## Suggested fix

Emit `observability::settled` only after `store.settle` returns `Ok(true)`.
`settle` already returns the applied/skipped boolean; thread it so the metric
reflects applied settlements only. Decide what (if anything) to emit on a guard
miss (e.g. a separate "settle skipped" counter).

Source: review finding, `src/worker/mod.rs:673-687`.
