# A transient notifier-build failure downgrades the worker to poll-only forever

## Problem

`Worker::run` builds the push notifier once at startup
(`src/worker/mod.rs:280-286`):

```rust
let mut notifier = match self.store.notifier().await {
    Ok(notifier) => notifier,
    Err(error) => {
        tracing::warn!(%error, "could not set up notifications; polling only");
        Box::new(crate::store::NeverNotifier)
    }
};
```

If `notifier()` fails once (a DB blip at startup), the worker installs
`NeverNotifier` for its entire lifetime and never tries again. New-work latency
then degrades to `poll_max` (default 30 s) permanently, with only a single warn
line.

This is asymmetric with an already-established `PgNotifier`, which reconnects
forever (`src/postgres/notify.rs:85-96`): a connection drop *after* startup
self-heals, but a failure *at* startup does not.

## Options

1. Retry building the notifier inside the loop when the current notifier is the
   `NeverNotifier` (bounded backoff), so a startup blip self-heals.
2. Make `PgNotifier::connect` defer the initial connection the way `reconnect`
   does, so `notifier()` never fails at startup and the first `recv` connects.

Recommendation: option 2 keeps the worker loop unchanged and removes the
asymmetry at the source. Either way the `notifier()` contract doc should state
the retry behavior.

## Test gap

Hard to exercise without a way to make the first `notifier()` call fail then
succeed; consider a store seam that fails the first build.

Source: review finding, `src/worker/mod.rs:280-286`, `src/postgres/notify.rs:85-96`.
