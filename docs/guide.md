# venturi: a getting-started guide

This guide walks you from a first job to advanced usage. It assumes you know
Rust and `async`/`await` with tokio, and that you have a PostgreSQL database to
point at.

Contents:

1. [Concepts in one minute](#1-concepts-in-one-minute)
2. [Setup](#2-setup)
3. [Your first task](#3-your-first-task)
4. [Producing work: the queue](#4-producing-work-the-queue)
5. [Consuming work: the worker](#5-consuming-work-the-worker)
6. [Outcomes: complete, pause, retry, give up](#6-outcomes-complete-pause-retry-give-up)
7. [Carried state and the execution context](#7-carried-state-and-the-execution-context)
8. [Retries, backoff, and giving up](#8-retries-backoff-and-giving-up)
9. [Deduplication and merging](#9-deduplication-and-merging)
10. [Scheduling: priority, delays, and caps](#10-scheduling-priority-delays-and-caps)
11. [Reliability: leases, recovery, and shutdown](#11-reliability-leases-recovery-and-shutdown)
12. [Operations: history, cleanup, and stats](#12-operations-history-cleanup-and-stats)
13. [Observability: tracing and metrics](#13-observability-tracing-and-metrics)
14. [Configuration reference](#14-configuration-reference)
15. [Deploying: producers and workers as separate binaries](#15-deploying-producers-and-workers-as-separate-binaries)

---

## 1. Concepts in one minute

A **task** is one serializable struct you define. The same struct is the job's
payload, its deduplication identity, and the value your handler receives.

The struct implements two traits:

- [`Task`] — identity and enqueue-time policy (the producer side). It is
  state-free, so a binary that only enqueues never depends on your handler's
  runtime dependencies.
- [`Handler<S>`] — the execution logic, run against your shared state `S` (the
  worker side).

A **producer** turns a typed task into a stored job through a [`Queue`]. A
**worker** claims jobs, runs them through their handler, and settles the outcome.
Storage lives behind the [`Store`] trait; the default adapter is PostgreSQL.

Every job carries a stable `KIND` string. It is the bridge between your typed
code and the type-erased rows in storage, and it routes a stored job back to the
right handler.

## 2. Setup

Add the crate and the runtime:

```toml
[dependencies]
venturi = "0.1"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
tokio-util = "0.7"   # for the shutdown CancellationToken
```

Connect to PostgreSQL and apply the schema once at startup:

```rust
use std::sync::Arc;
use venturi::postgres::PostgresStore;
use venturi::store::Store;

let dsn = "host=localhost user=postgres password=postgres dbname=postgres";
let store = Arc::new(PostgresStore::connect(dsn, "venturi")?);
store.migrate().await?;
```

`"venturi"` is the **table prefix**: every table and index is named from it
(`venturi_jobs`, `venturi_journal`, …), so several independent queues can share
one database. A prefix must be a short, lowercase identifier (`[a-z0-9_]`,
starting with a letter, at most 39 characters).

`connect` builds a `NoTls` connection for you. For TLS, enable the `rustls`
feature and use [`PostgresStore::connect_rustls(dsn, prefix, client_config)`][`connect_rustls`];
to supply a different connector, use
[`PostgresStore::from_config(config, tls, prefix)`][`from_config`]. The store
builds its claim pool and its `LISTEN` connection from the same parameters, so
push wakeups always use the same transport as the pool.

## 3. Your first task

```rust
use serde::{Deserialize, Serialize};
use venturi::{Context, Handler, Outcome, Task, TaskError};

#[derive(Serialize, Deserialize)]
struct SendEmail {
    to: String,
    subject: String,
}

impl Task for SendEmail {
    const KIND: &'static str = "send_email";
    type Carry = (); // nothing carried between runs
}
```

`KIND` must be unique per task type and stable across releases. `Carry` is typed
state carried between runs of the same job; `()` for tasks that keep nothing
(more in [§7](#7-carried-state-and-the-execution-context)).

Now the execution side. `App` is whatever your handlers need — HTTP clients,
mailers, database handles. It is shared (by reference) across all runs:

```rust
#[derive(Clone)]
struct App {
    // mailer, http client, etc.
}

impl Handler<App> for SendEmail {
    async fn handle(&self, _ctx: &mut Context<()>, app: &App) -> Result<Outcome, TaskError> {
        // `self` is the deserialized payload; `app` is your shared state.
        send_the_email(app, &self.to, &self.subject).await?; // `?` retries on error
        Ok(Outcome::completed())
    }
}
```

## 4. Producing work: the queue

A [`Queue`] is a cheap, cloneable handle over a store. It needs only `Task`, not
your handler or `App`, so a producer binary stays lean.

```rust
use venturi::Queue;

let queue = Queue::new(store.clone());

let id = queue
    .enqueue(SendEmail { to: "a@example.com".into(), subject: "Welcome".into() })
    .await?;
```

`enqueue` returns the job's [`Ulid`] id. The job is eligible immediately. To
schedule it for later, use `enqueue_at(task, when)` (see
[§10](#10-scheduling-priority-delays-and-caps)).

## 5. Consuming work: the worker

A [`Worker`] is built over your shared state and a store, with handlers
registered by type:

```rust
use tokio_util::sync::CancellationToken;
use venturi::Worker;

let worker = Worker::builder(App { /* ... */ }, store.clone())
    .register::<SendEmail>()
    .concurrency(16) // I/O-bound: raise above the default
    .build();

// Your application owns signal handling and hands venturi a cancellation token.
let shutdown = CancellationToken::new();
{
    let shutdown = shutdown.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        shutdown.cancel();
    });
}

worker.run(shutdown).await; // claims/dispatches until cancelled, then drains
```

`register::<T>()` both teaches the worker how to deserialize and run a kind and
adds it to the claim filter: a worker only claims kinds it has registered. Run
several worker processes for horizontal scale; each is its own loop, contending
safely over the same queue.

## 6. Outcomes: complete, pause, retry, give up

A run returns `Result<Outcome, TaskError>`, which encodes four decisions:

```rust
// Success:
Ok(Outcome::completed())                 // done
Ok(Outcome::completed_with("note"))      // done, with a journal note

// Cooperative pause (NOT a failure): re-pend and resume after the delay.
Ok(Outcome::pause_in(Duration::from_secs(30)))
Ok(Outcome::pause_in_with(Duration::ZERO, "checkpoint")) // ZERO yields immediately

// Failure:
Err(some_error.into())                   // retryable (any `?` does this)
Err(TaskError::permanent("resource gone")) // dead immediately, no retry
```

- **Complete** moves the job to a terminal `completed` state.
- **Pause** returns the job to pending, eligible again after `resume_in`, with
  its carry persisted. It is not a failure and does not consume the retry
  backstop — ideal for a multi-step job checkpointing progress or polling an
  external condition.
- A **retryable error** reschedules with backoff. Any error propagated with `?`
  becomes retryable, so the failure path is ergonomic.
- **`TaskError::permanent`** sends the job straight to dead.

The run's **note** rides the outcome (or, on failure, the error's message). For
structured evidence gathered during the run, use the context's attachment (next).

## 7. Carried state and the execution context

The handler receives `&mut Context<Self::Carry>`:

```rust
impl Handler<App> for SyncContacts {
    async fn handle(&self, ctx: &mut Context<SyncCursor>, app: &App) -> Result<Outcome, TaskError> {
        // How many times this job has run, and its prior outcomes.
        let attempt = ctx.run_count();
        let failures = ctx.history().iter().filter(|e| e.is_failure()).count();

        // Read and mutate the carried state. Persisted on both pause and retry.
        let cursor = ctx.carry().page;
        let (next, done) = app.fetch_page(cursor).await?;
        ctx.carry_mut().page = next;

        // Attach structured evidence to this run's journal entry (any outcome).
        ctx.set_attachment(serde_json::json!({ "fetched": next - cursor }));

        if done {
            Ok(Outcome::completed())
        } else {
            // Checkpoint and continue; the carry survives to the next run.
            Ok(Outcome::pause_in(Duration::ZERO))
        }
    }
}
```

`Carry` is any `Serialize + DeserializeOwned + Default` type. It is the job's
private working state, persisted whenever the job re-pends (pause or retry), and
distinct from the journal, which is the immutable historical record.

The context also exposes graceful-shutdown signals (`is_cancelled`, `cancelled`),
covered in [§11](#11-reliability-leases-recovery-and-shutdown).

## 8. Retries, backoff, and giving up

A retryable failure is rescheduled by a Fibonacci backoff:
`min(base * (fib(n) - 1), cap)` for attempt `n`, with multipliers
`0, 0, 1, 2, 4, 7, …`. The first two retries are immediate, then the delay
climbs to the cap. Proportional jitter spreads the delay deterministically from
the job's ULID, so the schedule is reproducible and pulls in no RNG.

Defaults are a 1-second base and a 5-minute cap. Override per worker:

```rust
use venturi::Backoff;
use std::time::Duration;

Worker::builder(app, store)
    .backoff(Backoff::new(Duration::from_millis(500), Duration::from_secs(60)))
    // ...
```

or per task, by overriding `Task::backoff` to return `Some(Backoff::new(..))`.

**Who decides to give up?** Primarily the task: judge from `ctx.run_count()` and
`ctx.history()` and return `TaskError::permanent(..)`. As a failsafe, the worker
carries an absolute backstop on *failed* executions (pauses never count). It
defaults high; tune or disable it:

```rust
Worker::builder(app, store)
    .backstop(Some(10)) // dead after 10 failed executions
    .backstop(None)     // or: never auto-dead; the task decides entirely
```

## 9. Deduplication and merging

Deduplication is two layers, both on `Task`.

First, a cheap, indexed **candidacy key**. Return one from `dedup_key`; `None`
(the default) never coalesces:

```rust
use venturi::{DedupKey, Merge, Pending};

impl Task for FetchBookmark {
    const KIND: &'static str = "fetch_bookmark";
    type Carry = ();

    fn dedup_key(&self) -> Option<DedupKey> {
        Some(DedupKey::from(self.bookmark_id)) // at most one pending fetch per bookmark
    }
}
```

Second, when an enqueue collides with a pending job sharing its
`(KIND, dedup_key)`, your **`merge`** decides what happens. It sees the existing
job's full state — payload, typed carry, run count, and journal — through a
[`Pending`]:

```rust
fn merge(&self, existing: &Pending<Self>) -> Merge<Self> {
    Merge::Replace
}
```

The four decisions:

- **`Keep`** — the incoming task is redundant; leave the existing job untouched.
- **`Replace`** (the default) — replace the existing payload with the incoming
  one and reset the carry to its default.
- **`With { task, carry }`** — replace with a computed payload and carry,
  continuing the existing work (escalate priority, union payloads, advance a
  cursor).
- **`Independent`** — not really a duplicate; enqueue as a new, separate row.

`Keep`, `Replace`, and `With` act on the existing row, so its journal is
preserved and a `merged` entry records the decision. The candidacy index is
non-unique, so `Independent` siblings are allowed.

## 10. Scheduling: priority, delays, and caps

**Priority.** Three tiers, defaulting to `Normal`:

```rust
use venturi::Priority;

impl Task for SendReceipt {
    const KIND: &'static str = "send_receipt";
    type Carry = ();
    fn priority(&self) -> Priority { Priority::High }
}
```

Claims order by priority then age. To prevent a flood of high-priority work from
starving lower tiers, the worker runs **weighted-slot anti-starvation** by
default (`priority_ratio(Some(4))`): it periodically reserves a claim for lower
tiers, favouring higher tiers by roughly the ratio per tier. Set
`priority_ratio(None)` for strict priority.

**Delayed and scheduled jobs.** Enqueue eligible-in-the-future work:

```rust
use chrono::Utc;
queue.enqueue_at(task, Utc::now() + chrono::Duration::hours(1)).await?;
```

The worker wakes exactly when a delayed job (a future enqueue, a backoff retry,
or a paused job's resume) becomes eligible, rather than only at its poll
interval.

**Per-kind concurrency caps.** When a kind needs a tighter bound than the global
concurrency — typically because each handler holds a slot in a small local
resource — register it capped:

```rust
Worker::builder(app, store)
    .concurrency(32)
    .register::<SendEmail>()       // uncapped, up to 32 at once
    .register_capped::<Reindex>(2) // at most 2 reindex jobs at once
    .build();
```

At-cap kinds are simply excluded from claiming until one of their jobs settles,
so their work stays pending rather than claimed-and-idle.

## 11. Reliability: leases, recovery, and shutdown

**Leases and recovery.** Every claim takes a lease (worker default 15 minutes).
If a worker dies mid-run, the lease expires and any worker recovers the job: it
re-pends with backoff and records a `stale-recovered` journal entry (counted as a
failed execution). A long-running task can request a longer lease by overriding
`Task::lease`. Recovery is timeout-only, so it behaves identically on one host or
many.

**Graceful shutdown.** When you cancel the token passed to `run`, the worker:

1. stops claiming new jobs;
2. raises a cooperative cancel signal and waits up to `shutdown_timeout`
   (default 30s) for handlers to wind down on their own;
3. at the deadline, force-aborts any straggler and releases it (recorded as
   `released`, not a failure; eligible immediately).

A handler observes the signal through its context and typically checkpoints with
a pause:

```rust
async fn handle(&self, ctx: &mut Context<Cursor>, app: &App) -> Result<Outcome, TaskError> {
    loop {
        if ctx.is_cancelled() {
            return Ok(Outcome::pause_in(Duration::ZERO)); // checkpoint and yield
        }
        // ... or react mid-await:
        tokio::select! {
            _ = ctx.cancelled() => return Ok(Outcome::pause_in(Duration::ZERO)),
            step = app.do_one_step(ctx.carry()) => { /* update carry, continue */ }
        }
    }
}
```

Every settle and release is guarded by claim ownership, so a slow handler can
never settle a job another worker has already reclaimed.

## 12. Operations: history, cleanup, and stats

All jobs are retained until you delete them, and every execution and merge is
recorded in an append-only journal.

**History query.** Filter jobs and read a job's journal timeline:

```rust
use venturi::store::{HistoryFilter, Status};

let recent_dead = queue.jobs(&HistoryFilter {
    status: Some(Status::Dead),
    kind: Some("send_email".into()),
    limit: Some(50),
    ..Default::default()
}).await?;

let timeline = queue.job_journal(job_id).await?; // every entry for one job
```

**Cleanup.** Bulk-delete terminal jobs by age and criteria; the journal is
removed by cascade:

```rust
use venturi::store::CleanupCriteria;
use chrono::Utc;

let removed = queue.cleanup(&CleanupCriteria {
    finished_before: Utc::now() - chrono::Duration::days(30),
    kind: None,
    status: None, // both completed and dead
}).await?;
```

**Stats snapshot.** Live aggregate state that counters cannot reconstruct:

```rust
let s = queue.stats().await?;
// s.pending_by_kind, s.oldest_pending_age, s.claimed, s.dead_by_kind
```

Surface it however you like — a health endpoint, gauges, a dashboard.

## 13. Observability: tracing and metrics

venturi emits structured `tracing` events for the lifecycle (enqueue, claim,
settle with outcome, stale recovery, shutdown drain). It is near-free until you
install a subscriber; the consuming application owns the subscriber, levels, and
formatting.

Behind the `metrics` feature, venturi also records counters and histograms
through the vendor-neutral `metrics` facade (jobs enqueued/claimed/settled by
outcome/merged/recovered, claim-latency and handler-duration histograms). Enable
it and install your own recorder/exporter:

```toml
venturi = { version = "0.1", features = ["metrics"] }
```

With the feature off, the crate takes no metrics dependency at all.

## 14. Configuration reference

All worker knobs are set on the builder; the defaults are conservative starting
points.

| Knob | Default | What it does |
|---|---|---|
| `concurrency(n)` | `max(1, min(8, cores/2))` | Max jobs in flight. Raise for I/O-bound work. |
| `poll_max(d)` | `30s` | Upper bound on the idle wait; a missed notification delays a job by at most this. |
| `lease(d)` | `15m` | Claim lease; must exceed a handler's real runtime. `Task::lease` overrides per task. |
| `shutdown_timeout(d)` | `30s` | Grace window before stragglers are force-released. |
| `backoff(Backoff)` | base `1s`, cap `5m` | Retry curve. `Task::backoff` overrides per task. |
| `backstop(Option<u32>)` | high | Absolute failed-execution cap before dead; `None` disables. |
| `priority_ratio(Option<u32>)` | `Some(4)` | Anti-starvation ratio; `None` is strict priority. |

Cross-process wakeups are always on: the store opens a `LISTEN` connection from
the same parameters it builds the pool with, and every write that makes a job
claimable (an enqueue, a retry, a pause's resume, a release, a recovered stale
claim) signals it. `poll_max` only bounds the delay of a dropped notification.

## 15. Deploying: producers and workers as separate binaries

Because `Task` is state-free, a producer binary (say, an HTTP API) implements
only `Task` and depends on neither your handlers nor `App`:

```rust
// producer crate
impl Task for SendEmail { /* KIND, Carry, dedup_key, priority */ }

let queue = Queue::new(store);
queue.enqueue(SendEmail { /* ... */ }).await?;
```

A separate worker binary adds `impl Handler<App>` and registers the type. The
shared `KIND` string is the only coupling between them. Scale workers by running
more processes — each claims independently and safely.

That is the whole model: define a struct, implement two traits, enqueue from one
side, run a worker on the other. Everything else — retries, dedup, scheduling,
recovery, shutdown, and observability — is policy you configure.

[`Task`]: https://docs.rs/venturi/latest/venturi/task/trait.Task.html
[`Handler<S>`]: https://docs.rs/venturi/latest/venturi/task/trait.Handler.html
[`Queue`]: https://docs.rs/venturi/latest/venturi/queue/struct.Queue.html
[`Worker`]: https://docs.rs/venturi/latest/venturi/worker/struct.Worker.html
[`Store`]: https://docs.rs/venturi/latest/venturi/store/trait.Store.html
[`Pending`]: https://docs.rs/venturi/latest/venturi/task/struct.Pending.html
[`Ulid`]: https://docs.rs/ulid/latest/ulid/struct.Ulid.html
[`connect_rustls`]: https://docs.rs/venturi/latest/venturi/postgres/struct.PostgresStore.html#method.connect_rustls
[`from_config`]: https://docs.rs/venturi/latest/venturi/postgres/struct.PostgresStore.html#method.from_config
