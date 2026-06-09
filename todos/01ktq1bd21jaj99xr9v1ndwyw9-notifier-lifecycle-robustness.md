# Notifier lifecycle robustness

One change request over the `PgNotifier` connect/reconnect lifecycle
(`src/postgres/notify.rs`) and how the worker installs it
(`src/worker/mod.rs` run loop). Two related defects in the same machinery.

## 1. A transient startup failure downgrades the worker to poll-only forever

`Worker::run` builds the notifier once at startup (`src/worker/mod.rs:280-286`);
on error it installs `NeverNotifier` for the worker's entire lifetime. A single
DB blip at startup degrades new-work latency to `poll_max` (default 30s)
permanently. This is asymmetric with an already-established `PgNotifier`, which
reconnects forever (`src/postgres/notify.rs:85-96`): a drop *after* startup
self-heals, a failure *at* startup does not.

## 2. Reconnect spins at ~1 Hz during a full DB outage

`PgNotifier::reconnect` sleeps ~1s on failure and returns, so `recv` resolves
roughly once per second during an outage; each resulting loop spin fires three
failing queries (`find_stale`, claim, `next_visible_at`) plus warn logs. Bounded
and self-healing, but a noisy ~1 Hz error spin instead of the intended `poll_max`
cadence.

## Suggested fix (one coherent reconnect policy)

Make `PgNotifier` connect lazily/resiliently and back off on failure:
- Defer the initial connection (like `reconnect` does) so `notifier()` never
  fails at startup and a startup blip self-heals — removes defect 1.
- Replace the fixed ~1s reconnect sleep with a growing backoff capped at
  `poll_max`, or stop treating a reconnect-failure return as a wakeup so the loop
  falls back to its poll cadence — removes defect 2.

Both are the same "robust notifier connect/reconnect" change to `notify.rs`.

Source: review findings, `src/worker/mod.rs:280-286`, `src/postgres/notify.rs:85-96`.
