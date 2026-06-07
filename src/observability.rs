//! Lifecycle instrumentation: structured `tracing` events always, and metrics
//! through the vendor-neutral `metrics` facade behind the `metrics` feature.
//!
//! `tracing` is near-free when no subscriber is installed, and with the `metrics`
//! feature off the crate takes no metrics dependency at all. The consuming
//! application owns the subscriber, the recorder, and the exporter; venturi only
//! emits. Each helper is one call site for one lifecycle event so the rest of the
//! code stays uncluttered.

use crate::store::JournalOutcome;
use std::time::Duration;

/// A job was enqueued as a new row.
pub(crate) fn enqueued(kind: &str) {
    tracing::debug!(kind, "job enqueued");
    #[cfg(feature = "metrics")]
    metrics::counter!("venturi_jobs_enqueued_total", "kind" => kind.to_owned()).increment(1);
}

/// An enqueue merged into an existing pending job.
pub(crate) fn merged(kind: &str) {
    tracing::debug!(kind, "enqueue merged into a pending job");
    #[cfg(feature = "metrics")]
    metrics::counter!("venturi_jobs_merged_total", "kind" => kind.to_owned()).increment(1);
}

/// A job was claimed after waiting `wait` since it was enqueued.
pub(crate) fn claimed(kind: &str, wait: Duration) {
    tracing::debug!(kind, wait_ms = wait.as_millis() as u64, "job claimed");
    #[cfg(feature = "metrics")]
    {
        metrics::counter!("venturi_jobs_claimed_total", "kind" => kind.to_owned()).increment(1);
        metrics::histogram!("venturi_claim_latency_seconds", "kind" => kind.to_owned())
            .record(wait.as_secs_f64());
    }
}

/// A run was settled with `outcome`, its handler having taken `handler_duration`.
pub(crate) fn settled(kind: &str, outcome: JournalOutcome, handler_duration: Duration) {
    tracing::debug!(
        kind,
        outcome = outcome.as_str(),
        handler_ms = handler_duration.as_millis() as u64,
        "job settled",
    );
    #[cfg(feature = "metrics")]
    {
        metrics::counter!(
            "venturi_jobs_settled_total",
            "kind" => kind.to_owned(),
            "outcome" => outcome.as_str(),
        )
        .increment(1);
        metrics::histogram!("venturi_handler_duration_seconds", "kind" => kind.to_owned())
            .record(handler_duration.as_secs_f64());
    }
}

/// A stale claim was recovered by lease expiry.
pub(crate) fn recovered(kind: &str) {
    tracing::warn!(kind, "stale claim recovered");
    #[cfg(feature = "metrics")]
    metrics::counter!("venturi_jobs_recovered_total", "kind" => kind.to_owned()).increment(1);
}

/// The worker began a graceful-shutdown drain of `in_flight` handlers.
pub(crate) fn shutdown_drain(in_flight: usize) {
    tracing::info!(in_flight, "graceful shutdown: draining in-flight handlers");
}
