//! Retry backoff: configuration, the Fibonacci curve, and deterministic jitter.
//!
//! A retryable failure is rescheduled after a delay derived from the attempt
//! number. [`Backoff`] holds the configurable base delay and ceiling; a task
//! returns one from [`crate::task::Task::backoff`] to widen or tighten its own
//! schedule relative to the worker default. The crate computes the realized
//! delay from the Fibonacci curve `min(base * (fib(n) - 1), cap)`, spread by
//! proportional jitter derived deterministically from the job's ULID so the
//! schedule is reproducible and depends on no random-number generator.

use std::time::Duration;
use ulid::Ulid;

/// The floor a [`Backoff`] base delay is clamped to. A zero base makes every
/// computed delay zero (a tight retry loop straight to the backstop); one
/// millisecond keeps the curve meaningful.
const MIN_BASE: Duration = Duration::from_millis(1);

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
    /// single delay. Degenerate inputs are clamped: `base` is floored to 1ms (a
    /// zero base makes every delay zero), and `cap` is raised to at least `base`
    /// (a `cap` below `base` would otherwise clamp every delay to `cap`,
    /// inverting intent).
    pub fn new(base: Duration, cap: Duration) -> Backoff {
        let base = base.max(MIN_BASE);
        let cap = cap.max(base);
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
    /// The worker-level default: a half-second base climbing to a two-minute cap.
    ///
    /// These are conservative starting points. With the Fibonacci curve the first
    /// two retries are immediate, then the delay grows `500ms, 1s, 2s, 3.5s, …`
    /// until it reaches the two-minute ceiling.
    fn default() -> Backoff {
        Backoff {
            base: Duration::from_millis(500),
            cap: Duration::from_secs(120),
        }
    }
}

// =============================================================================
// The Fibonacci backoff curve and its deterministic jitter
// =============================================================================

/// The realized retry delay for a job's `attempt`-th failure under `backoff`,
/// with proportional jitter applied.
///
/// The base curve is `min(base * (fib(attempt) - 1), cap)`. The `fib(n) - 1`
/// shaping yields multipliers `0, 0, 1, 2, 4, 7, …`, so the first two retries are
/// immediate and the delay then climbs until it reaches the cap. Proportional
/// jitter spreads the realized delay into `[delay * (1 - fraction), delay]`,
/// derived deterministically from the job's ULID and the attempt number so the
/// schedule is reproducible and venturi pulls in no random-number generator.
pub(crate) fn retry_delay(backoff: &Backoff, fraction: f64, attempt: u32, id: Ulid) -> Duration {
    let delay = base_delay(backoff, attempt);
    if delay.is_zero() {
        return Duration::ZERO;
    }

    // factor lands in [1 - fraction, 1): full delay when the unit sample is near
    // 1, the smallest delay when it is near 0.
    let fraction = fraction.clamp(0.0, 1.0);
    let unit = jitter_unit(id, attempt);
    let factor = 1.0 - fraction * (1.0 - unit);

    let nanos = delay.as_nanos() as f64 * factor;
    Duration::from_nanos(nanos as u64)
}

/// The un-jittered base delay: `min(base * (fib(attempt) - 1), cap)`.
fn base_delay(backoff: &Backoff, attempt: u32) -> Duration {
    let multiplier = u128::from(fib_minus_one(attempt));
    let base = backoff.base().as_nanos();
    let cap = backoff.cap().as_nanos();
    let delay = base.saturating_mul(multiplier).min(cap);
    // `delay` is bounded by `cap`, which for any sane configuration fits in u64.
    Duration::from_nanos(delay.min(u128::from(u64::MAX)) as u64)
}

/// `fib(n) - 1` with `fib(1) = fib(2) = 1`, saturating at `u64::MAX`.
///
/// The `- 1` makes the first two attempts return zero (immediate retry), and the
/// curve `0, 0, 1, 2, 4, 7, 12, …` climbs from there.
fn fib_minus_one(n: u32) -> u64 {
    if n <= 2 {
        return 0;
    }
    let (mut prev, mut curr) = (1u64, 1u64);
    for _ in 3..=n {
        let next = prev.saturating_add(curr);
        prev = curr;
        curr = next;
    }
    curr.saturating_sub(1)
}

/// A deterministic unit sample in `[0, 1)` from a job id and attempt number.
///
/// The 128-bit ULID is folded into two 64-bit halves and run, together with the
/// attempt, through a SplitMix64 finalizer. Distinct jobs and distinct attempts
/// of one job therefore get distinct, reproducible jitter without any global RNG.
fn jitter_unit(id: Ulid, attempt: u32) -> f64 {
    let bytes = id.to_bytes();
    let hi = u64::from_be_bytes(bytes[0..8].try_into().expect("8 bytes"));
    let lo = u64::from_be_bytes(bytes[8..16].try_into().expect("8 bytes"));
    let mixed = splitmix64(hi ^ splitmix64(lo ^ u64::from(attempt)));

    // The top 53 bits give a uniform value in [0, 1) at f64 precision.
    (mixed >> 11) as f64 / ((1u64 << 53) as f64)
}

/// The SplitMix64 finalizer: a fast, well-distributed bit mixer.
fn splitmix64(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_id() -> Ulid {
        Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("valid ULID")
    }

    #[test]
    fn default_backoff_is_500ms_base_and_120s_cap() {
        let backoff = Backoff::default();
        assert_eq!(backoff.base(), Duration::from_millis(500));
        assert_eq!(backoff.cap(), Duration::from_secs(120));
    }

    #[test]
    fn new_clamps_degenerate_base_and_cap() {
        // A zero base floors above zero so delays are not uniformly zero.
        let floored = Backoff::new(Duration::ZERO, Duration::from_secs(10));
        assert_eq!(floored.base(), MIN_BASE);
        assert_eq!(floored.cap(), Duration::from_secs(10));

        // A cap below base is raised to base rather than inverting intent.
        let raised = Backoff::new(Duration::from_secs(5), Duration::from_secs(1));
        assert_eq!(raised.base(), Duration::from_secs(5));
        assert_eq!(raised.cap(), Duration::from_secs(5));
    }

    #[test]
    fn fib_minus_one_matches_the_documented_curve() {
        let got: Vec<u64> = (1..=8).map(fib_minus_one).collect();
        assert_eq!(got, vec![0, 0, 1, 2, 4, 7, 12, 20]);
    }

    #[test]
    fn first_two_attempts_are_immediate() {
        let backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(300));
        assert_eq!(retry_delay(&backoff, 0.5, 1, fixed_id()), Duration::ZERO);
        assert_eq!(retry_delay(&backoff, 0.5, 2, fixed_id()), Duration::ZERO);
    }

    #[test]
    fn delay_is_capped() {
        let backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(10));
        // With no jitter (fraction 0) the delay equals the capped base.
        let delay = retry_delay(&backoff, 0.0, 40, fixed_id());
        assert_eq!(delay, Duration::from_secs(10));
    }

    #[test]
    fn jitter_is_deterministic() {
        let backoff = Backoff::default();
        let a = retry_delay(&backoff, 0.5, 7, fixed_id());
        let b = retry_delay(&backoff, 0.5, 7, fixed_id());
        assert_eq!(a, b);
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(300));
        let fraction = 0.5;
        for attempt in 3..=20 {
            let base = base_delay(&backoff, attempt);
            let actual = retry_delay(&backoff, fraction, attempt, fixed_id());
            let low = base.as_nanos() as f64 * (1.0 - fraction);
            assert!(
                (actual.as_nanos() as f64) >= low - 1.0,
                "attempt {attempt}: {actual:?} below lower bound"
            );
            assert!(
                actual <= base,
                "attempt {attempt}: {actual:?} above base {base:?}"
            );
        }
    }

    proptest::proptest! {
        /// For any job, attempt, and jitter fraction, the realized delay is
        /// deterministic and lands within `[base * (1 - fraction), base]`.
        #[test]
        fn delay_is_deterministic_and_bounded(
            bits in proptest::prelude::any::<u128>(),
            attempt in 1u32..80,
            fraction in 0.0f64..=1.0,
        ) {
            let backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(300));
            let id = Ulid::from(bits);

            let first = retry_delay(&backoff, fraction, attempt, id);
            let second = retry_delay(&backoff, fraction, attempt, id);
            proptest::prop_assert_eq!(first, second);

            let base = base_delay(&backoff, attempt);
            let low = base.as_nanos() as f64 * (1.0 - fraction);
            proptest::prop_assert!(first <= base);
            proptest::prop_assert!((first.as_nanos() as f64) >= low - 1.0);
        }
    }
}
