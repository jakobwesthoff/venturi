# 25. Expose observability through tracing, a metrics facade, and a stats snapshot

Date: 2026-06-07

## Status

Accepted

## Context

Operators need to see what the queue is doing: per-operation logs, operational
metrics such as rates and latencies, and the current backlog. A library must not
hard-wire a logging backend, a metrics backend, or an introspection mechanism onto
the consuming application.

## Decision

Three complementary, backend-neutral mechanisms:

- **Logging via `tracing`.** venturi emits structured spans and events for the
  lifecycle operations: enqueue, claim, settle with its outcome, stale-claim
  recovery, and shutdown drain. The consuming application owns the subscriber,
  levels, and formatting. `tracing` is near-free when no subscriber is attached.
- **Metrics via the `metrics` facade, feature-gated.** When the feature is
  enabled, venturi records vendor-neutral counters and histograms (jobs enqueued,
  claimed, completed, failed, dead, retried; claim latency; handler duration)
  through the `metrics` facade, and the consumer installs whatever recorder or
  exporter it wants. The feature is off by default, so a consumer that does not
  want metrics takes no extra dependency.
- **Introspection via a stats-snapshot API.** A programmatic call returns a
  snapshot of current queue state: backlog depth and oldest-pending age per kind
  and status, the in-flight (claimed) count, and the dead count. It is distinct
  from the history record-query API (ADR 18): that reads job and journal rows,
  this reports live aggregate state. The consumer exposes the snapshot however it
  likes, such as a health endpoint, gauges, or a dashboard.

## Consequences

Each mechanism is decoupled from a specific backend: the consumer chooses the
tracing subscriber, the metrics recorder, and how to surface the stats. Metrics
cost nothing unless the feature is enabled. Event rates and latencies come from the
metrics facade as they happen, while the current backlog, which counters cannot
reconstruct, comes from the stats snapshot; together they cover both flow and
state. The stats queries are on-demand aggregates, not hot-path; they use the
existing indexes where applicable and otherwise scan the bounded pending set, and a
particular stat can get a dedicated index later if it proves expensive at scale.
