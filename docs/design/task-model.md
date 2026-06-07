# venturi task-authoring model

Date: 2026-06-07

This document describes the public surface a consuming project programs against
to define, register, and enqueue work. It consolidates the decisions accepted in
ADRs 9, 10, 11, 12, 13, 14, 15, and 17 into one narrative. It introduces no new
decisions and finalizes nothing still under discussion; see the closing section
for what is deliberately left out.

## The two roles a task plays

A unit of work in venturi is one struct, defined by the consuming project, that
serves as both the payload and the identity of a job. That struct is touched at
two distinct sites with different needs:

- **Producers** enqueue work. They need to identify a task, set its priority, and
  decide how it deduplicates against work already queued. They do not run it, so
  they have no need of the worker's runtime dependencies.
- **Workers** run work. They need everything a producer needs plus the actual
  execution logic and the dependencies that logic requires.

These two sites can live in different binaries. A typical deployment separates an
HTTP server that enqueues work from one or more worker binaries that process it.
If enqueueing required the worker's dependency type, a producer binary would be
forced to depend on things it never uses.

The model therefore splits across two traits implemented on the same struct:
`Task` (state-free, what a producer and storage need) and `Handler<S>` (the
execution side, what a worker needs). `Handler<S>` has `Task` as a supertrait,
because running a job first requires identifying and deserializing it; a handler
with no identity cannot exist in this system, and the supertrait collapses the
registration bound to a single trait.

```rust
/// Identity and enqueue-time policy. State-free: usable by a producer that
/// never runs the work. Implemented directly on the payload struct.
trait Task: Serialize + DeserializeOwned + Send + Sync + 'static + Sized {
    /// Stable discriminator stored alongside the payload and used to route
    /// the job back to its handler. Must be unique per task type and stable
    /// across releases.
    const KIND: &'static str;

    /// Scheduling priority. Producers set it implicitly by which task they
    /// enqueue; the value participates in claim ordering.
    fn priority(&self) -> Priority {
        Priority::Normal
    }

    /// Candidacy key for deduplication. `None` means the task is never
    /// coalesced. Two pending tasks with the same `(KIND, key)` are collision
    /// candidates, found through an index.
    fn dedup_key(&self) -> Option<DedupKey> {
        None
    }

    /// State carried between runs of the same job, also visible to `merge`.
    /// `()` for tasks that keep nothing.
    type Carry: Serialize + DeserializeOwned + Default + Send;

    /// Decide what happens when a pending task with the same `(KIND, dedup_key)`
    /// already exists. Called only on a collision. `Pending` gives the existing
    /// job's payload, typed carry, run count, and journal, so the decision is
    /// informed by content and history, and can continue in-progress work.
    fn merge(&self, existing: &Pending<Self>) -> Merge<Self> {
        Merge::Replace
    }

    /// Per-task override of the retry backoff. `None` uses the worker default.
    fn backoff(&self) -> Option<Backoff> {
        None
    }
}

/// Execution side, parameterized over the worker's shared state `S`. Adds the
/// handler. A producer crate never implements this.
trait Handler<S>: Task {
    async fn handle(
        &self,
        ctx: &mut Context<Self::Carry>,
        state: &S,
    ) -> Result<Outcome, TaskError>;
}
```

A producer crate implements `Task` with no knowledge of `S`. A worker crate adds
`impl Handler<S>`. In a single-binary project both impls sit together on the same
struct.

## The `KIND` discriminator

`const KIND` is the stable string under which a task type is stored and routed.
It is the bridge between the typed world the consumer writes and the type-erased
world the storage layer sees. Because it is a `const`, it is available without an
instance, which is what registration and dispatch lookup require.

## The type-erased boundary and the registry

Payloads cross a JSON boundary in storage, so the storage layer only ever sees a
`kind` string and an opaque payload value. Type safety is recovered at exactly
two points: at enqueue, where a typed `Task` is serialized; and at dispatch,
where a stored payload is deserialized back into its concrete type before
`handle` runs.

Tasks are registered against a worker by type. Internally this is a type-erased
registry keyed by the `KIND` string. Each entry knows how to deserialize the
stored payload into one concrete task type and invoke that type's `handle` with
the worker's `&S`. Because every handler in a given worker takes the same `&S`,
the registry is homogeneous: every entry has the same erased shape.

```rust
/// Generic over the consumer's shared state. One worker, one `S`.
struct Worker<S> { /* registry, configuration */ }

impl<S> Worker<S> {
    /// Register a task type. The bound `T: Handler<S>` guarantees the type is
    /// both identifiable (`Task`) and runnable against this worker's state.
    fn register<T: Handler<S>>(&mut self) -> &mut Self { /* ... */ self }
}
```

The `KIND` string, the payload type, and the handler are one unit the compiler
ties together. An enqueued task cannot reach a handler expecting a different
payload, and adding a new kind of work is implementing a trait and registering a
type rather than editing a central enum. The library owns no domain enum.

## Deduplication: candidacy key plus full-task merge

Deduplication is two layers, both expressed on `Task`.

The first layer, `dedup_key`, is cheap and indexed. It answers "which pending
task could this collide with?" without scanning the backlog. Returning `None`
opts out of coalescing entirely.

The second layer, `merge`, runs only when a pending candidate with the same
`(KIND, dedup_key)` exists. That candidate may be a paused, already-run job; it is
treated like any other pending job. `merge` receives the existing job's full state
as a `Pending<Self>` and returns a `Merge<Self>`:

```rust
struct Pending<T: Task> {
    payload: T,
    carry: T::Carry,
    run_count: u32,
    journal: Vec<JournalEntry>,
}

enum Merge<T: Task> {
    /// The incoming task is redundant; leave the existing one untouched.
    Keep,
    /// The existing payload is replaced by the incoming one; carry resets to default.
    Replace,
    /// Replace the existing job with a computed payload and carry, continuing its work.
    With { task: T, carry: T::Carry },
    /// Not a duplicate after all; enqueue as a new row.
    Independent,
}
```

Because `merge` sees the existing payload, its typed `carry`, its run count, and
its journal, the decision is informed by content and history. `With { task, carry }`
is the content-aware case: it can escalate priority, union payloads, or hand the
surviving job a modified carry so it continues in-progress work. `Replace` swaps
the payload and starts the carry fresh; `Keep` and `Independent` cover the trivial
cases. `Keep`, `Replace`, and `With` act on the existing row, so its journal is
preserved and the job stays trackable across the merge, and each appends a
`merged` journal entry recording the decision. `Independent` is a plain new
enqueue. Candidate selection stays an indexed lookup, so enqueue cost does not
grow with backlog size.

## Running a task: `handle`, the context, and outcomes

```rust
async fn handle(
    &self,
    ctx: &mut Context<Self::Carry>,
    state: &S,
) -> Result<Outcome, TaskError>;
```

`&self` is the deserialized payload, `state` is the shared worker dependencies,
and `ctx` is the execution context (below). The return type encodes the four
things a run can decide.

### The four outcomes

```rust
enum Outcome {
    Completed { note: Option<String> },
    Pause { resume_in: Duration, note: Option<String> },
}
```

- `Ok(Outcome::Completed { .. })` completes the job.
- `Ok(Outcome::Pause { resume_in, .. })` is a cooperative pause, **not a
  failure**. The job returns to pending and becomes eligible again after
  `resume_in` (`Duration::ZERO` is allowed for an immediate yield). The carried
  state is persisted, no failure is recorded, and the retry backstop is not
  consumed. This is the tool for a multi-step job checkpointing its progress or a
  job polling an external condition.
- `Err(_)` is a failure, **retryable by default**: any error propagated with `?`
  becomes a retry, scheduled with backoff. This keeps the failure path ergonomic.
- `Err(TaskError::permanent(_))` sends the job straight to dead. This is how a
  task abandons work it knows will never succeed.

Constructors keep the common cases terse:

```rust
Outcome::completed()              // Completed { note: None }
Outcome::completed_with(msg)      // Completed { note: Some(msg) }
Outcome::pause_in(d)              // Pause { resume_in: d, note: None }
Outcome::pause_in_with(d, msg)    // Pause { resume_in: d, note: Some(msg) }
```

### Note versus attachment

A run records two distinct things, and they come from two different places
because they have two different natures.

The **note** is the run's *conclusion*: a short human-readable statement of what
the run decided. It is bound to the outcome, so it travels *with* the outcome.
For a success or a pause it is the optional `note` field on `Outcome`. For a
failure it is the `TaskError`'s own message. There is no separate place to put it,
which is what makes success and failure symmetric: each is an outcome carrying a
note.

The **attachment** is structured evidence *gathered during* the run: metrics,
identifiers, partial results. It is independent of which outcome the run reaches,
so it is set on the context as the run proceeds, not on the outcome:

```rust
ctx.set_attachment(json!({ "bytes_fetched": 412_000, "status": 304 }));
```

`set_attachment` takes a `serde_json::Value`, is last-write-wins, and is available
for any outcome, including before returning an `Err`. Because the note rides the
outcome and the attachment rides the context, the two never compete for the same
slot, and a failed run can still carry structured evidence.

Both feed the journal, the per-execution record. This document refers to the
journal only as the concept that receives a `note`, an `attachment`, and the
failure history a task reads; its storage layout and query surface are out of
scope here (ADR 16).

### The execution context

```rust
struct Context<Carry> { /* ... */ }

impl<Carry> Context<Carry> {
    /// How many times this job has been executed, including the current run.
    fn run_count(&self) -> u32 { /* ... */ }

    /// Prior outcomes for this job, from which the failure count is read. This
    /// is the basis for a task's give-up decision.
    fn history(&self) -> &[JournalEntry] { /* ... */ }

    /// Read the carried state.
    fn carry(&self) -> &Carry { /* ... */ }

    /// Mutate the carried state. Persisted for the next run on both retry and
    /// pause.
    fn carry_mut(&mut self) -> &mut Carry { /* ... */ }

    /// Set the structured attachment for this run's journal entry. Last write
    /// wins.
    fn set_attachment(&mut self, value: serde_json::Value) { /* ... */ }

    /// Whether a graceful shutdown has been signalled. A long handler can poll
    /// this at a safe point and stop early, typically by returning `Pause` to
    /// checkpoint its carry.
    fn is_cancelled(&self) -> bool { /* ... */ }

    /// Resolves when a graceful shutdown is signalled, for use inside a `select!`
    /// to react in the middle of a long await. The shutdown mechanism itself
    /// belongs to the worker model.
    fn cancelled(&self) -> impl Future<Output = ()> { /* ... */ }
}
```

`run_count` and `history` are what let a task decide its own fate from its real
past rather than a fixed counter. `carry`/`carry_mut` expose the carried state.
`set_attachment` records evidence. `is_cancelled`/`cancelled` let a handler
observe a graceful shutdown and wind down cleanly; the shutdown mechanism itself
is part of the worker model.

### Carried state

`Carry` is a typed associated value on `Task`, bounded
`Serialize + DeserializeOwned + Default` and defaulting to `()`. It lives on
`Task` rather than `Handler` because `merge` reads and writes it at enqueue (see
deduplication above). The handler reads it through `ctx.carry()` and mutates it
through `ctx.carry_mut()`; the value is persisted for the next run on both retry
and pause. A task that keeps nothing leaves `Carry` at its default `()` and pays
nothing. The carried state is the
job's private working state, distinct from the journal, which is the immutable
historical record.

## Retries, backoff, and giving up

venturi does not enforce a per-attempt cap as its primary mechanism. A task ends
itself when it judges further retries pointless, by returning
`TaskError::permanent`, and it makes that judgement from its own run history
(`ctx.run_count()`, `ctx.history()`).

As a failsafe against a task that keeps returning retryable errors for a failure
it does not recognize, the worker carries an absolute attempt backstop. It is
enabled by default at a high value, is configurable, and can be set to `None` to
disable. The backstop counts **failed** executions, not total runs, so a
cooperative pause loop never trips it. When a job's failure count reaches the
backstop, it transitions to dead.

A retryable failure is rescheduled by a backoff policy that maps an attempt
number to a base delay. Fibonacci is the initial policy, shaped as
`min(base * (fib(n) - 1), cap)`. The `fib(n) - 1` form yields multipliers
`0, 0, 1, 2, 4, 7, 12, …`, so the first two retries are immediate and the curve
climbs after that; `cap` is a hard ceiling on the delay. The realized delay then
has proportional jitter applied with a worker-level fraction `f`, landing within
`[delay*(1-f), delay]`. The jitter offset is derived deterministically from the
job's ULID and attempt number rather than from a random-number generator, so the
schedule is reproducible and venturi pulls in no `rand` dependency (ADR 14). The
backoff `base` and `cap` have a worker-level default and may be overridden per
task through `Task::backoff`; the jitter fraction `f` is worker-level.

## End-to-end example

A bookmark service fetches page content in the background. `FetchBookmark` is the
task; `App` is the worker's shared state.

```rust
use serde::{Deserialize, Serialize};
use std::time::Duration;

// ---- The task: identity + enqueue-time policy (producer side) ----

#[derive(Serialize, Deserialize)]
struct FetchBookmark {
    bookmark_id: Ulid,
    url: String,
}

impl Task for FetchBookmark {
    const KIND: &'static str = "fetch_bookmark";

    type Carry = (); // single-shot; nothing carried between runs

    fn priority(&self) -> Priority {
        Priority::Normal
    }

    // At most one pending fetch per bookmark.
    fn dedup_key(&self) -> Option<DedupKey> {
        Some(DedupKey::from(self.bookmark_id))
    }

    // A newer enqueue for the same bookmark supersedes the pending one.
    fn merge(&self, _existing: &Pending<Self>) -> Merge<Self> {
        Merge::Replace
    }
}

// ---- The worker's shared dependencies ----

#[derive(Clone)]
struct App {
    http: reqwest::Client,
    db: deadpool_postgres::Pool,
}

// ---- The execution side (worker crate) ----

impl Handler<App> for FetchBookmark {
    async fn handle(
        &self,
        ctx: &mut Context<Self::Carry>,
        app: &App,
    ) -> Result<Outcome, TaskError> {
        // A response we will never be able to parse is permanent; the `?`
        // on a transient network error retries by default.
        let resp = app.http.get(&self.url).send().await?;
        if resp.status() == reqwest::StatusCode::GONE {
            return Err(TaskError::permanent("resource gone"));
        }

        let body = resp.text().await?;
        let bytes = body.len();
        store_content(&app.db, self.bookmark_id, &body).await?;

        ctx.set_attachment(serde_json::json!({ "bytes": bytes }));
        Ok(Outcome::completed_with(format!("fetched {bytes} bytes")))
    }
}

// ---- Wiring a worker (worker binary) ----

fn build_worker(app: App) -> Worker<App> {
    let mut worker = Worker::new(app);
    worker.register::<FetchBookmark>();
    worker
}

// ---- Enqueuing from a producer (e.g. an HTTP handler), which knows only Task ----

async fn on_bookmark_created(queue: &Queue, bookmark_id: Ulid, url: String) {
    queue
        .enqueue(FetchBookmark { bookmark_id, url })
        .await
        .expect("enqueue fetch_bookmark");
}
```

The producer side (`on_bookmark_created`) compiles against `Task` alone and never
names `App`. The worker side adds `impl Handler<App>` and registers the type. The
same `FetchBookmark` struct is the payload, the dedup identity, and the unit the
handler receives.

## Out of scope / not yet decided

This document covers only the task-authoring surface. The following are
deliberately excluded and remain under discussion or belong to other documents:

- The worker/runtime model: concurrency, the claim/poll loop, and graceful
  shutdown.
- Stale-claim recovery and leasing.
- The database schema (columns, indexes, the JSONB shapes behind the payload,
  carry, and journal).
- Completion and retention, and the job lifecycle states.
- The journal's storage layout and its external query and cleanup API. The
  journal appears here only as the concept that receives a run's `note`,
  `attachment`, and the failure history a task reads (ADR 16).
