# Notifier reconnect spins at ~1 Hz during a full DB outage

## Problem

`PgNotifier::reconnect` sleeps ~1s on failure and returns, so `recv` resolves
roughly once per second during an outage; each resulting loop spin fires three
failing queries (`find_stale`, claim, `next_visible_at`) plus warn logs
(`src/postgres/notify.rs:85-96`, `src/worker/mod.rs` run loop). Bounded and
self-healing, but a noisy ~1 Hz error spin rather than the intended `poll_max`
cadence.

## Suggested fix

Back off the reconnect (growing delay up to `poll_max`), or stop treating a
reconnect-failure return as a wakeup so the loop falls back to its poll cadence.

Source: review finding R7, `src/postgres/notify.rs:85-96`.
