---
title: "Notifier connect and reconnect are not resilient at startup or during outages"
kind: bug
component: postgres
status: needs-discussion
origin: review
tags: [retry]
---
# Notifier connect and reconnect are not resilient at startup or during outages

Found in review. Two related defects in the `PgNotifier` connect and reconnect lifecycle, plus how the worker installs it. One coherent reconnect policy would fix both. The backoff shape is still open.

## 1. A transient startup failure downgrades the worker to poll-only forever

`Worker::run` builds the notifier once at startup (`src/worker/mod.rs`, lines 280-286). On error it installs `NeverNotifier` for the worker's entire lifetime. A single DB blip at startup degrades new-work latency to `poll_max` (default 30 seconds) permanently.

This is asymmetric with `PgNotifier`, which reconnects forever (`src/postgres/notify.rs`, lines 85-96). A drop after startup self-heals. A failure at startup does not.

## 2. Reconnect spins at about 1 Hz during a full DB outage

`PgNotifier::reconnect` sleeps about 1 second on failure and returns, so `recv` resolves roughly once per second during an outage. Each resulting loop spin fires three failing queries (`find_stale`, claim, `next_visible_at`) plus warn logs. The loop is bounded and self-healing, but it produces a noisy error spin of about 1 Hz instead of the intended `poll_max` cadence.

## Suggested fix

Make `PgNotifier` connect lazily and resiliently, and back off on failure:

- Defer the initial connection (as `reconnect` already does) so `notifier()` never fails at startup and a startup blip self-heals. This removes defect 1.
- Either replace the fixed 1-second reconnect sleep with a growing backoff capped at `poll_max`, or stop treating a reconnect-failure return as a wakeup so the loop falls back to its poll cadence. This removes defect 2. The choice between the two is open.

Both fixes are the same "robust notifier connect and reconnect" change in `notify.rs`.

Source: review findings, `src/worker/mod.rs` lines 280-286, `src/postgres/notify.rs` lines 85-96.
