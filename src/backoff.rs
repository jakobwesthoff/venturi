//! Retry backoff configuration.
//!
//! A retryable failure is rescheduled after a delay derived from the attempt
//! number. The base delay and the ceiling are configurable; the curve shape and
//! the deterministic jitter that spreads retries arrive with the failure-handling
//! phase. [`Backoff`] is the per-task override of the base and cap; a task
//! returns it from [`crate::task::Task::backoff`] to widen or tighten its own
//! schedule relative to the worker default.

use std::time::Duration;

/// Per-task override of the retry backoff's base delay and ceiling.
///
/// The worker holds a default [`Backoff`]; a task may return its own from
/// [`crate::task::Task::backoff`]. The base scales the curve and the cap bounds
/// the longest delay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    base: Duration,
    cap: Duration,
}

impl Backoff {
    /// A backoff with the given base delay and ceiling.
    ///
    /// `base` scales the per-attempt curve; `cap` is the hard upper bound on any
    /// single delay.
    pub fn new(base: Duration, cap: Duration) -> Backoff {
        Backoff { base, cap }
    }

    /// The base delay that scales the curve.
    pub fn base(&self) -> Duration {
        self.base
    }

    /// The hard ceiling on a single delay.
    pub fn cap(&self) -> Duration {
        self.cap
    }
}

impl Default for Backoff {
    /// The worker-level default: a one-second base climbing to a five-minute cap.
    ///
    /// These are conservative starting points. With the Fibonacci curve the first
    /// two retries are immediate, then the delay grows `1s, 2s, 4s, 7s, …` until
    /// it reaches the five-minute ceiling.
    fn default() -> Backoff {
        Backoff {
            base: Duration::from_secs(1),
            cap: Duration::from_secs(5 * 60),
        }
    }
}
