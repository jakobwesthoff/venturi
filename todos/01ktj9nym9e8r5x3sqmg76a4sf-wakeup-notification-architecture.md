# Revisit the wakeup / notification architecture

**Status:** decided-as-interim during implementation; needs design discussion.
**Priority:** high (user explicitly flagged this).

## Desired behaviour (user intent)

A push notification should drive the worker to react as fast as possible — with
no busy-waiting — in **every** situation where a job becomes (or will become)
claimable. The wait timeout must be a **true fallback only**, used solely to
recover from a genuinely lost notification, never as the normal path for picking
up work. Today that is not the case.

## What exists now

The worker's wait is always `min(next_visible_at, poll_max)` (see
`Worker::wait_duration` in `src/worker/mod.rs`) plus an optional notifier branch
in the `select!`. Concretely:

- **`NOTIFY` is emitted only on enqueue** of a brand-new row
  (`PostgresStore::enqueue` runs `SELECT pg_notify($channel, '')`; channel =
  `{prefix}_jobs`). Independent-merge inserts also enqueue, so they notify.
- **No notification is sent when a job becomes eligible *later*:** a backoff
  retry's `visible_at` arriving, a paused job's resume time, a future
  `enqueue_at`, or a `released` job. These rely entirely on the
  `next_visible_at` "smart wait" (an indexed `min(visible_at)` query) to time the
  wake. That is correct and reasonably prompt, but it is *poll/compute-driven*,
  not push-driven — it is exactly the "timeout as primary mechanism" the user
  wants to avoid.
- **The listener is opt-in and `NoTls`-only.** A worker only *receives*
  notifications if the store was given a DSN via `PostgresStore::with_listen`
  (which `PostgresStore::connect` sets automatically). Without it, `notifier()`
  returns a `NeverNotifier` and the worker runs purely on the poll.

## Why it ended up this way (constraints encountered)

1. **deadpool hides the connection message stream.** You cannot receive `NOTIFY`
   on a pooled client; the background `Connection` that yields
   `AsyncMessage::Notification` is spawned away. A dedicated, owned
   `tokio_postgres` connection is required (implemented as `PgNotifier` in
   `src/postgres/notify.rs`, polling `connection.poll_message`).
2. **The adapter is built from a `Pool`, not from connection params.**
   `PostgresStore::new(pool, prefix)` is TLS-agnostic by P0 design (caller builds
   the pool with `NoTls` or a rustls connector). A `deadpool` `Pool` does not
   expose the DSN it was built from, so the listener params must be supplied
   separately — hence `with_listen(dsn)` and its opt-in nature.
3. **TLS for the listener is generic and was deferred.** `PgNotifier` uses
   `tokio_postgres::connect(dsn, NoTls)`. Supporting TLS means threading a
   generic `MakeTlsConnect` connector through the non-generic `PostgresStore`
   struct (type-erasing the connector), which was out of scope for the initial
   build.

## Options to discuss

- **Notify on every transition to a *currently* claimable state** (enqueue,
  release, and any settle that re-pends with `visible_at <= now`). Cheap and
  closes the immediate-eligibility gap.
- **Future-eligibility wakeups without polling.** For delayed/retry/paused jobs
  the eligibility time is known at settle. Options: (a) keep the
  `next_visible_at` timer but treat it as the *scheduling* mechanism, not a
  fallback (arguably already push-like, since it wakes exactly at the time, not
  on an interval); (b) a dedicated in-process timer wheel keyed on the soonest
  `visible_at`, refreshed on every settle/enqueue, so the wait is event-driven
  rather than recomputed each loop; (c) `pg_notify` scheduled via `pg_cron`/a
  trigger (heavier, cross-process). Clarify whether the user considers the
  `next_visible_at` timer acceptable as "not a timeout fallback" or wants an
  explicit notify for these too.
- **Make the listener work for all deployments (incl. TLS).** Either: accept a
  caller-provided connector / a "listener factory" closure on the store; or have
  the consumer pass a `tokio_postgres::Config` (+ connector) explicitly; or
  re-architect `PostgresStore` to own connection params and build both the pool
  and the listener. Decide whether listening should be **on by default** rather
  than opt-in.
- **Reconnect / missed-notification semantics.** `PgNotifier::recv` currently
  reconnects on drop and returns so the loop re-polls (covering anything missed).
  Confirm this is the intended "fallback only" safety net and that `poll_max`
  should perhaps be larger (or removed) once push covers all cases.

## Acceptance criteria for the revisit

- New work (immediate or delayed) is acted on via a push wakeup in all normal
  cases; the timeout fires only after a dropped/lost notification.
- Listening works for TLS deployments, ideally without a second plaintext
  endpoint.
- No busy-waiting; no regression in the existing scheduling integration tests
  (`tests/scheduling.rs`).
