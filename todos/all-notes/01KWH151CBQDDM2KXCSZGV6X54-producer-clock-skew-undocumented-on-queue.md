# Producer-side clock skew is undocumented on `Queue::enqueue`/`enqueue_at`

## Problem

Claim eligibility is evaluated in **database** time: the claim predicate is
`visible_at <= now()` with PostgreSQL's `now()`
(`src/postgres/mod.rs:276-277`). But the `visible_at` a producer writes
comes from the **producer's** clock: `Queue::enqueue` stamps
`visible_at = Utc::now()` and `enqueue_at` stamps the caller-supplied
instant (`src/queue.rs:43-55`).

Consequence: if a producer's clock runs ahead of the database's by X, every
"immediate" `enqueue()` produces a job that is invisible to claims for X.
`enqueue`'s doc says "The job becomes eligible to claim at once"
(`src/queue.rs:37-39`), which is only true under aligned clocks.

The crate does document a clock assumption, but only in the **worker**
module docs (`src/worker/mod.rs:10-19`), and that text covers the instants
the *worker* writes back (retry/pause `visible_at`, `finished_at`,
`recorded_at`). A producer author has no reason to read the worker docs;
the assumption they depend on is stated nowhere they will look. Producer
processes (web frontends, cron hosts) are also more likely than DB/worker
hosts to have loosely synced clocks.

## Suggested fix

Doc-only. State the assumption where producers read:

- On `Queue::enqueue`: eligibility is evaluated against database time;
  "at once" assumes the producer's clock is reasonably aligned with the
  database's (same NTP framing as the worker doc).
- On `Queue::enqueue_at`: `when` is compared against database time.

Alternatively (behavior change, needs a decision): stamp immediate
enqueues' `visible_at` with the database's `now()` in the INSERT instead of
a bind parameter, which removes the skew for the `enqueue()` path entirely
(`enqueue_at` necessarily keeps the caller's instant). `created_at` would
stay producer-stamped or move along with it; note `created_at` feeds claim
ordering and the claim-latency metric.

Source: review finding, `src/queue.rs:37-55`, `src/postgres/mod.rs:266-287`,
`src/worker/mod.rs:10-19`.
