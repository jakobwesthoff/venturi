# venturi observability

Date: 2026-06-07

How venturi lets an operator see what the queue is doing. It offers three
backend-neutral mechanisms (ADR 25): structured logging, metrics through a
vendor-neutral facade, and a live state snapshot. None of them binds the consuming
application to a particular logging, metrics, or dashboard backend.

## Logging

venturi emits structured `tracing` spans and events for the lifecycle operations:
enqueue, claim, settle (with the resulting outcome), stale-claim recovery, and
shutdown drain. The consuming application owns the subscriber, the levels, and the
formatting. `tracing` is near-free when no subscriber is installed, so the
instrumentation costs nothing until a consumer opts into collecting it.

## Metrics

Behind a feature flag, venturi records counters and histograms through the
`metrics` facade, and the consumer installs whatever recorder or exporter it
wants. The facade is vendor-neutral, so no metrics backend is hard-wired, and with
the feature off a consumer takes no metrics dependency at all.

The recorded series describe the flow of work, for example:

- counters for jobs enqueued, claimed, completed, retried, paused, released, dead,
  and merged;
- a histogram of claim latency;
- a histogram of handler execution duration.

These are event-driven: they advance as operations happen.

## Introspection

A programmatic stats-snapshot call reports current queue state, which counters
cannot reconstruct:

```rust
queue.stats().await? -> Snapshot {
    pending_by_kind:     Map<Kind, u64>,   // backlog depth per kind
    oldest_pending_age:  Map<Kind, Duration>,
    claimed:             u64,              // in-flight across the system
    dead_by_kind:        Map<Kind, u64>,
    // ...
}
```

It is computed with on-demand aggregate queries, not on the hot path, and is
distinct from the history record-query API (ADR 18): that one reads individual job
and journal rows, this one reports live aggregate state. The consumer surfaces the
snapshot however it likes, such as a health endpoint, gauge metrics, or a
dashboard.

## How the three combine

The three mechanisms cover different questions and complement each other:

- **metrics** answer "how fast and how often" — rates and latencies, as events;
- **the stats snapshot** answers "how much is waiting right now" — the current
  backlog and its age, which event counters cannot reproduce;
- **tracing** answers "what happened to this job" — per-operation detail.

## Out of scope

- The choice of metrics recorder or exporter, dashboards, and alerting are the
  consumer's; venturi only emits through the facade and exposes the snapshot.
- Rate-control metrics are deferred along with rate control itself (tracked as a
  todo).
