# venturi worker / runtime model

Date: 2026-06-07

This document describes how a worker claims, runs, and settles jobs. It
consolidates the decisions accepted in ADRs 3, 4, 19, 20, 21, and 22 into one
narrative. It introduces no new decisions and finalizes nothing still under
discussion; see the closing section for what is deliberately left out.

The task-authoring surface a handler programs against (`Task`, `Handler<S>`,
`Context`, `Outcome`, `Priority`, `Backoff`) is defined in
`docs/design/task-model.md`. This document uses those types and does not redefine
them.

## The claim-and-dispatch loop

A worker is a single loop that keeps an in-flight set of at most `N` running
handler tasks and feeds it from the queue. Each iteration recovers any abandoned
claims, fills every free slot by claiming and spawning one job at a time, and then
waits until something lets it make progress again.

```text
loop {
    recover_stale();                       // reclaim expired leases (ADR 19)

    while in_flight < N {
        match claim_next(registered_kinds, priority_floor) {
            Some(job) => spawn_handler(job),   // takes a slot
            None      => break,                // nothing claimable right now
        }
    }

    select {
        finished = next_finished_handler() => settle(finished),  // frees a slot
        _        = notification()          => {}                 // new work enqueued
        _        = timeout(min(next_visible_at - now, poll_max)) => {}
        _        = shutdown()              => break,             // graceful stop
    }
}
```

Claiming is one row per free slot rather than a batch, so a job's lease (below)
begins when its work begins rather than while it waits its turn inside the worker.
With `N = 1` the in-flight set holds a single job and the worker processes strictly
serially. Horizontal scale comes from running more worker processes, each its own
loop, not from nesting loops inside one worker (ADR 20).

## Claiming

A worker claims only the task kinds it has registered: the claim filter is exactly
the registered set, so a worker never claims a kind it cannot handle. A claim is a
single statement that atomically marks the next eligible row claimed and returns
it, ordered by priority then age and skipping rows another worker already holds
(ADR 3):

```sql
UPDATE jobs
SET status = 'claimed', claimed_by = $worker, claim_expires_at = now() + $lease
WHERE id = (
    SELECT id FROM jobs
    WHERE status = 'pending'
      AND visible_at <= now()
      AND kind = ANY($registered_kinds)
      AND priority >= $priority_floor          -- anti-starvation, below
    ORDER BY priority, created_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING *;
```

`visible_at <= now()` gates eligibility, so delayed work (a retry's backoff, a
paused job's resume, a job scheduled for the future) is invisible until its time.
`SKIP LOCKED` lets concurrent workers and concurrent claims within one worker take
distinct rows without blocking each other. The kind set passed to a claim is the
worker's registered kinds minus any currently at their per-kind concurrency cap
(see below).

## Concurrency

A worker runs one bounded loop with an in-flight set capped at `N`. `N` is the only
concurrency knob; there is no second level of parallelism inside a worker.

Worker concurrency is a property of the workload, not the host. I/O-bound handlers
spend their time awaiting and can run many at once, bounded by downstream and
connection-pool capacity. Handlers that are CPU-bound or that block a thread are
bounded by the async runtime's worker-thread budget, beyond which more concurrency
starves the runtime. Handlers gated by an external resource are bounded by that
resource. Because the library cannot know which case applies, the default `N` is a
safe starting point, not an optimum (ADR 20):

```text
N = max(1, min(8, available_parallelism() / 2))
```

This default is a safety floor: it stays under the runtime's worker-thread budget
so that handlers which block a thread cannot starve the runtime on a small host,
and the cap keeps it modest on a large one. It errs low on purpose, since too
little concurrency costs visible throughput a user can raise, whereas too much can
silently overwhelm a downstream or the runtime. Tune `N` up for I/O-bound work,
set it to match the size of any external resource pool a handler depends on, and
keep blocking work in a blocking-aware spawn rather than reaching for a higher `N`.

## Per-kind concurrency caps

`N` bounds total in-flight work, but a single kind sometimes needs a tighter cap,
typically because each of its handlers holds a slot in a small local resource the
worker owns (ADR 23). A kind may be registered with a per-kind cap. The cap is set
at registration rather than on the task type, because it reflects this worker's
local resource, and it is local to the worker, so across several workers the
effective limit is the cap times the number of workers.

The worker tracks the in-flight count per kind and narrows the claim filter to
kinds below their cap: the kind set for a claim is the registered kinds whose
in-flight count is under their cap, with uncapped kinds always included. A kind at
its cap is excluded from the claim until one of its in-flight jobs settles, so its
jobs stay `pending` rather than claimed-and-idle, costing no slot or lease while
they wait. This is in-memory bookkeeping and rides the existing claim filter and
index. It lets one worker run a small cap on a resource-bound kind alongside high
concurrency for others, without splitting into separate workers.

## Wakeup and waiting

When the loop cannot make progress by claiming, it sleeps until the soonest of
four things wakes it (ADR 4, ADR 20):

- a running handler finishes, freeing a slot;
- a notification arrives on a dedicated listen connection, signalling newly
  enqueued work;
- a timeout of `min(time until the next future visible_at, poll_max)` elapses;
- the shutdown signal fires.

The notification path only fires on enqueue. Work that becomes eligible *later*
produces no notification: a retry whose backoff elapses, a paused job whose resume
time arrives, a job scheduled for the future. The timeout's first term, computed
from a cheap indexed lookup of the nearest not-yet-eligible job among the worker's
kinds, wakes the loop exactly when such a job becomes claimable. `poll_max` bounds
the wait when nothing is scheduled, so a missed notification delays a job by at
most `poll_max` rather than stalling the queue.

## Settlement

When a handler finishes, the loop reaps it and settles its outcome in one place.
A normal return settles by its `Outcome` (complete, pause, retry, or dead, per the
task model); a handler task that ended in a panic settles as a failed execution
(ADR 20).

Panic isolation relies on the async runtime catching a panicking task at its
boundary and surfacing it as a join error rather than crashing the process. This
holds when the binary unwinds on panic. When the binary is built to abort on panic
instead, a handler panic terminates the whole process, which is the worker-death
case already covered by stale-claim recovery. Correctness therefore never depends
on the consuming binary's panic configuration, only on how quickly and gracefully a
panic is absorbed.

Settlement and release are guarded by claim ownership: the write applies only if
the worker still holds the claim (the row is still `claimed` by this worker). A
handler that is slow or being aborted cannot settle or release a job that another
worker has since reclaimed, which keeps the settlement path and the recovery path
free of double-settlement.

## Stale-claim recovery

At-least-once delivery means a worker can claim a job and then die before settling
it, leaving the row stuck in `claimed`. Recovery returns such a job to the pending
pool (ADR 19).

The lease is a per-claim expiry: a claim stamps `claim_expires_at = now + lease`,
and recovery reclaims any row where `status = 'claimed' AND claim_expires_at <
now`. The lease has a worker default (15 minutes) and an optional per-task override
via `Task::lease()`, so a task known to run long can request a longer one.
Detection is timeout-only, with no process-liveness check, so it behaves
identically on one host or many.

A recovered run is treated as a failed execution: it appends a `stale-recovered`
entry to the journal noting the expired lease and the worker presumed dead, counts
toward the failure backstop, and re-enters `pending` with backoff applied, so a
transient crash does not immediately re-crash a healthy worker. Carried state is
the last value persisted at a settle point, so a recovered run resumes from there
and any mid-crash mutations are discarded.

Recovery runs opportunistically at the start of every claim, so the system is
self-healing without a dedicated process. It is also exposed as a manual operation
for an external sweeper or administrative use.

## Graceful shutdown

Shutdown is driven by a programmatic signal the worker observes; the consuming
application wires operating-system signals to it, and venturi installs none of its
own (ADR 21). On shutdown the worker:

1. stops claiming new jobs;
2. raises a cooperative cancellation signal visible to every in-flight handler and
   waits up to `shutdown_timeout` (30 seconds by default) for handlers to
   wind down on their own terms. A handler observes the signal through its context,
   either by polling `ctx.is_cancelled()` at a safe point or by awaiting
   `ctx.cancelled()` inside a `select!` to react mid-await, and typically returns
   `Pause` to checkpoint its carry, settling through the normal path with no lost
   progress;
3. at the timeout, force-aborts any handler still running and releases its job.

A release from a clean shutdown is not a failure: the operator chose to stop and
the job did not misbehave. A released job is recorded as a `released` journal event,
does not count toward the failure backstop, and returns to `pending` with
`visible_at = now` so another worker picks it up immediately. This is distinct from
`stale-recovered`, which represents a crash and does count as a failure with
backoff. With cooperation in place, a forced `released` is the exception rather
than the rule. After everything has settled or been released, the worker tears down
its listen connection and returns.

## Priority and anti-starvation

Priority is a fixed three-tier enum, `High` / `Normal` / `Low`, defaulting to
`Normal`, and the claim orders by priority then age. Strict priority ordering would
let a sustained stream of higher-priority work starve lower-priority jobs
indefinitely, so weighted-slot anti-starvation is on by default (ADR 22).

The worker keeps a claim counter and, on a cadence set by `priority_ratio`, lowers
the priority floor for a single claim so lower tiers receive guaranteed slots.
Higher tiers are favored by roughly the ratio per tier while every tier keeps a
nonzero long-run share. A claim that reserves a lower tier and finds it empty falls
back to an unconstrained claim, so a reserved slot is never wasted. The constraint
is the `priority >= floor` filter shown in the claim statement, which rides the
existing claim index. Setting `priority_ratio` off makes every claim unconstrained,
which is exactly strict priority.

## Loop resilience

The loop never crashes on a transient database error. A failed claim or settlement
retries with backoff rather than tearing down the worker. The listen connection
reconnects with backoff if it drops, and the polling fallback covers the gap in the
meantime so progress continues. A settlement that finds the job already reclaimed,
detected by the ownership guard, is skipped rather than retried.

## Worker identity

Each worker records a `claimed_by` identity of the form `host:pid` on every claim.
It is used for diagnostics and for the note attached to a `stale-recovered` journal
entry; it is not used for recovery decisions, which are timeout-only.

## Config defaults

All knobs are set at worker construction; the defaults are conservative starting
points.

| Knob | Default | Tuning note |
|---|---|---|
| `concurrency` (`N`) | `max(1, min(8, cores/2))` | Raise for I/O-bound work; match an external resource pool; keep blocking work in a blocking-aware spawn. |
| `poll_max` | `30s` | Upper bound on wait when nothing is scheduled; lower for snappier pickup of missed notifications at the cost of more polling. |
| `lease` | `15m` | Must exceed a handler's real runtime; override per task with `Task::lease()` for long jobs; lower for faster crash recovery. |
| `shutdown_timeout` | `30s` | Grace window for handlers to wind down before force-release; raise for handlers that checkpoint slowly. |
| `priority_ratio` | `4` | Higher favors top tiers more strongly; off yields strict priority. |
| `claimed_by` | `host:pid` | Identity recorded per claim for diagnostics. |

## Worker construction and entry point

A worker is generic over the consumer's shared state `S` and registers task types
by the bound `T: Handler<S>`. Registration both teaches the worker how to
deserialize and run a kind and defines the claim filter.

```rust
/// Built over the consumer's shared state. One worker, one `S`.
struct Worker<S> { /* registry, config, runtime state */ }

impl<S> Worker<S> {
    fn builder(state: S) -> WorkerBuilder<S>;
}

impl<S> WorkerBuilder<S> {
    fn register<T: Handler<S>>(self) -> Self;       // also sets the claim filter
    fn register_capped<T: Handler<S>>(self, max: usize) -> Self;  // per-kind concurrency cap (ADR 23)

    fn concurrency(self, n: usize) -> Self;
    fn poll_max(self, d: Duration) -> Self;
    fn lease(self, d: Duration) -> Self;            // default lease; Task::lease overrides
    fn shutdown_timeout(self, d: Duration) -> Self;
    fn priority_ratio(self, ratio: Option<u32>) -> Self;  // None => strict priority

    fn build(self) -> Worker<S>;
}

impl<S> Worker<S> {
    /// Runs the claim/dispatch loop until `shutdown` is triggered, then drains
    /// (cooperatively, up to `shutdown_timeout`) and returns.
    async fn run(self, shutdown: CancellationToken);
}
```

## End-to-end example

A bookmark service runs background fetching and email sending. `App` is the shared
state; `FetchBookmark` and `SendEmail` are task types whose `Task` and
`Handler<App>` impls are defined per the task model.

```rust
#[derive(Clone)]
struct App {
    http: reqwest::Client,
    db: deadpool_postgres::Pool,
    mailer: Mailer,
}

#[tokio::main]
async fn main() {
    let app = App::connect().await;

    let worker = Worker::builder(app)
        .register::<FetchBookmark>()
        .register::<SendEmail>()
        .concurrency(32)              // I/O-bound: raised above the default
        .build();

    // The application owns signal handling and hands venturi a cancellation token.
    let shutdown = CancellationToken::new();
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            shutdown.cancel();
        });
    }

    worker.run(shutdown).await;       // claims FetchBookmark + SendEmail only
}
```

The worker claims only `FetchBookmark` and `SendEmail` rows, runs up to 32 at a
time, wakes on enqueue notifications and at each job's eligibility time, recovers
abandoned claims at 15-minute leases, and on Ctrl-C drains cooperatively for 30
seconds before releasing whatever remains.

## Out of scope / not yet decided

This document covers the worker/runtime model only. The following are deliberately
excluded and belong to other parts:

- Rate control: throttling a kind over time, for example against an external rate
  limit. Deferred and tracked as a todo. (Per-kind concurrency caps are covered
  above.)
- The database schema: column definitions, types, and the indexes that back the
  claim, recovery, eligibility, and history access paths.
- Observability and metrics: logging, queue-state introspection, and metric
  emission.
