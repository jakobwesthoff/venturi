# venturi implementation and testing plan

Date: 2026-06-07

How venturi will be built and tested, grounded in the accepted design (ADRs
0001–0025 and `design/`). The plan is shaped by three decisions: a single crate
with feature flags; a walking skeleton built first, then features layered on as
vertical slices; and real-PostgreSQL integration tests on ephemeral containers
with prefix isolation, alongside pure-logic unit tests and an in-memory fake
backend.

## Crate layout

A single `venturi` crate. The backend trait (ADR 8) is a module seam, not a
separate crate; a workspace split is deferred until a second backend exists.

```
venturi/
  src/
    task/          Task, Handler<S>, Outcome, Merge, Pending, Priority,
                   Backoff, Context, TaskError, the type-erased registry
    worker/        Worker<S> + builder, claim/dispatch loop, settlement,
                   recovery, shutdown, concurrency, priority rotation, caps
    store/         the backend trait (the operation contract)
    postgres/      default adapter: pool, claim/settle/recover SQL,
                   migrations (refinery + prefix substitution), schema
    observability/ tracing instrumentation, metrics (feature-gated), stats
    error.rs       public error types
  migrations/      SQL files with the {{prefix}} placeholder (V1 = schema)
```

Features: `postgres` (default) enables the adapter; `metrics` (opt-in) enables the
`metrics`-facade emission. The backend trait is public so a consumer can supply an
alternative adapter; public errors are `thiserror` enums (no `anyhow` in the
public surface).

## Dependency stack (ADR 24)

`tokio`, `tokio-postgres` (with `with-chrono` and `with-serde_json`),
`deadpool-postgres`, `refinery`, a rustls TLS connector, `serde` + `serde_json`,
`ulid`, `chrono`, `tracing`, `tokio-util` (cancellation token), `thiserror`, and
`metrics` (behind the feature). Dev-dependencies add an ephemeral-Postgres
container harness and tokio test utilities.

## Testing strategy

Three layers, each targeting what it is best at:

- **Unit tests (no database)** for pure logic: the Fibonacci backoff with the
  `fib(n)-1` shaping and cap, the deterministic jitter derived from `(ulid,
  attempt)` and its bounds, the `Merge` decision application, the priority-floor
  rotation, and the counter transitions.
- **An in-memory fake backend** implementing the storage trait, to test the worker
  loop without a database: concurrency accounting, settlement routing, shutdown
  drain and cooperative cancellation, recovery triggering, per-kind caps, and
  priority rotation. Fast and deterministic.
- **Real-PostgreSQL integration tests** for the behavior that *is* the SQL: claim
  ordering and `SKIP LOCKED` under concurrent workers (no double-claim),
  `visible_at` gating, dedup candidacy and the merge outcomes, stale-claim
  recovery, FK-cascade cleanup, prefixed migrations, and the history/stats
  aggregates. Each test run uses an ephemeral container; the configurable table
  prefix isolates tests that share one database, enabling parallelism.

Property tests cover invariants (realized backoff within `[delay*(1-f), delay] ≤
cap`; dedup never loses a pending obligation). CI runs unit and fake-backend tests
always and the integration suite against a Postgres service via Docker.

## Phases

Each phase is buildable and testable on its own. A phase lists its goal, the
deliverables, the tests that prove it, and the exit criterion.

### P0 — Scaffold and substrate

- **Goal:** a buildable crate with the storage substrate and the test harness.
- **Deliverables:** crate + features + dependency stack; the backend trait
  signatures; the PostgreSQL adapter skeleton (deadpool pool, connection config,
  TLS); the migration applier (read `migrations/*.sql`, substitute `{{prefix}}`,
  run via refinery with a per-prefix history table, ADR 24); the V1 migration =
  the `jobs` and `journal` tables and indexes from `design/schema.md`; error
  types; the ephemeral-container test harness handing out a prefixed store.
- **Tests:** the schema migrates cleanly under a given prefix; two prefixes
  coexist independently; the harness spins a container, migrates, and connects.
- **Exit:** `cargo test` brings up Postgres, applies the schema, and connects.

### P1 — Walking skeleton

- **Goal:** a minimal enqueue → claim → run → complete path end to end.
- **Deliverables:** `Task` + `Handler<S>` (just `KIND` and `handle`); the
  type-erased registry; `Worker<S>` + builder (`register`, `concurrency`);
  `enqueue` (insert); claim (`UPDATE … WHERE id = (SELECT … FOR UPDATE SKIP
  LOCKED)` over registered kinds, ordered by `priority, created_at`); the bounded
  dispatch loop (cap `N`, spawn, reap, settle); `complete`. Outcomes limited to
  success and a generic failure for now.
- **Tests:** unit for registry dispatch; fake-backend for claim→run→complete and
  `N` bounding; integration for a real enqueue/claim/run/complete and for
  concurrent workers never double-claiming (ADR 3).
- **Exit:** a registered handler runs a job to completion against real Postgres.

### P2 — Outcomes and failure handling

- **Implements:** ADR 11, 12, 13, 14, and the counters from ADR 5/15.
- **Deliverables:** the `Outcome` enum (`Completed{note}`, `Pause{resume_in,
  note}`); `TaskError` with retryable-by-default and `permanent`; the `visible_at`
  eligibility gate; Fibonacci backoff (`base*(fib(n)-1)` capped) with proportional
  jitter derived from `(ulid, attempt)`; settlement routing for
  complete/pause/retry/dead; `run_count`/`failure_count`; task-decides-death plus
  the configurable backstop.
- **Tests:** unit for the backoff curve and jitter determinism/bounds;
  fake-backend for outcome routing and the backstop tripping at the failure cap;
  integration for retry with growing `visible_at`, permanent → `dead`, pause
  re-pending after `resume_in`, and correct counters.
- **Exit:** retry, backoff, pause, and dead all behave against real Postgres.

### P3 — Journal and execution context

- **Implements:** ADR 15, 16 (and the read side feeding ADR 18).
- **Deliverables:** a journal write on every settle; `Context<Carry>` exposing
  `run_count`, `history`, `carry`/`carry_mut` (persisted on retry and pause),
  and `set_attachment`; `note` sourced from the outcome or the error.
- **Tests:** unit for the context accessors; integration that every execution
  writes a journal entry with the right outcome/note/attachment, that carry
  round-trips across pause and retry, and that history reflects prior runs.
- **Exit:** a hole-free journal and a carry that survives re-runs.

### P4 — Deduplication

- **Implements:** ADR 10.
- **Deliverables:** `dedup_key`; `merge(&self, &Pending<Self>) -> Merge<Self>`
  with `Keep`/`Replace`/`With`/`Independent`; the candidacy lookup on the partial
  index; applying the merge on the existing row (payload and carry); the `merged`
  journal event.
- **Tests:** unit for applying each `Merge` variant; integration for a colliding
  enqueue triggering merge, each variant's effect, the `merged` entry, merging
  into a paused job (carry kept or continued), and `Independent` siblings under
  the non-unique index.
- **Exit:** dedup and merge correct, including against paused jobs.

### P5 — Reliability: recovery and shutdown

- **Implements:** ADR 19, 21.
- **Deliverables:** the `claim_expires_at` lease (default 15m, `Task::lease()`
  override); opportunistic recovery at claim start plus a manual `recover_stale`;
  the `stale-recovered` event with `failure_count` and backoff re-pend; graceful
  shutdown (programmatic signal, cooperative `ctx.is_cancelled()`/`cancelled()`,
  `shutdown_timeout` drain, force-release with the `released` event); the
  claim-ownership guard on settle and release.
- **Tests:** fake-backend for the drain (cooperative pause, force-release) and
  recovery triggering; integration that a claim past its lease is recovered as
  `stale-recovered` with backoff, that shutdown re-pends in-flight as `released`,
  and that the ownership guard blocks a double-settle after reclaim.
- **Exit:** crash recovery and clean shutdown both correct.

### P6 — Scheduling: priority, anti-starvation, caps, wakeup

- **Implements:** ADR 4, 20, 22, 23.
- **Deliverables:** the three-tier priority (smallint) and `ORDER BY priority,
  created_at`; the weighted-slot floor rotation (`priority_ratio` default 4,
  off ⇒ strict); per-kind concurrency caps (in-flight tracking narrowing the claim
  kind set); `LISTEN/NOTIFY` on a dedicated connection with `pg_notify` on
  enqueue, the poll fallback, and the `min(next_visible_at, poll_max)` wait with
  listen reconnect.
- **Tests:** unit/fake for the rotation distribution, the cap narrowing, and the
  wait-timeout computation; integration for priority ordering, lows not starved
  under a high-priority stream, a per-kind cap bounding in-flight, prompt NOTIFY
  wakeups, and a delayed job claimed right at its `visible_at`.
- **Exit:** priority, caps, and wakeup behave, and lower tiers make progress.

### P7 — Operations: query, cleanup, stats, observability

- **Implements:** ADR 18, 25.
- **Deliverables:** the history query API (filter by kind/status/time, plus a
  per-job journal timeline); the cleanup API (by age and criteria, cascading the
  journal); the stats snapshot (depth and oldest-pending age per kind/status,
  in-flight, dead); tracing instrumentation across the lifecycle operations; the
  feature-gated metrics emission.
- **Tests:** integration for the query filters, cleanup removing jobs and their
  journal via cascade, and correct stats; a test recorder confirming metrics are
  emitted with the feature on; presence of the expected tracing spans.
- **Exit:** introspection and ops APIs work and observability is wired.

### P8 — Hardening and polish

- **Goal:** confidence and ergonomics.
- **Deliverables:** concurrency stress tests (many workers racing); property tests
  for the backoff/jitter/dedup invariants; literate doc comments following the
  Rust API guidelines; a README with a usage walkthrough; runnable examples (a
  producer and a worker); CI configuration (a Postgres service, `fmt`, `clippy`,
  the test suites).
- **Exit:** the suites are green in CI and the examples run.

## Conventions during implementation

- Apply the documentation and citation constraints to code comments and docs as
  well: no references to other projects, self-contained explanations.
- Literate, why-focused comments; `expect` with concise justifications over
  `unwrap`; public APIs measured against the Rust API guidelines.
- Add a `todos/` entry for any deferred nuance found mid-build, rather than
  silently widening scope.

## Open implementation-time questions

- Verify the `_jobs_claim` index against the real multi-kind claim query with
  `EXPLAIN`; switch to a `(priority, created_at)`-leading variant if the
  `MergeAppend` over per-kind scans underperforms (noted in `design/indexes.md`).
- Confirm the exact ephemeral-container crate and its Docker requirements in CI.
- Re-evaluate a workspace split only if and when a second storage backend is
  added.
