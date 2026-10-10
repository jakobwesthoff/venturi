---
title: "Design docs have drifted from the shipped code"
kind: chore
component: project
origin: review
---
# Design docs have drifted from the shipped code

Found in the 2026-07-02 review. `docs/design/` presents itself as the current design ("these documents synthesize the accepted ADRs into coherent narratives", `docs/design/README.md:7-8`). Several statements and sketches no longer match the shipped code. `schema.md` and `indexes.md` were checked and are accurate; the drift is in the other four files.

## 1. API sketches and examples show a pre-builder / pre-store API

- `task-model.md:396-399`: the wiring example uses `Worker::new(app)` and `worker.register::<FetchBookmark>()` (an `&mut self` registration also sketched at `task-model.md:115-119`). The shipped API is `Worker::builder(state, store).register::<T>().build()` (`src/worker/mod.rs:300-308`); `Worker::new` does not exist.
- `worker-model.md:273` sketches `fn builder(state: S)` and the example at `worker-model.md:314` calls `Worker::builder(app)`. Both omit the required `store` argument.
- The builder sketch (`worker-model.md:276-287`) and the config table (`worker-model.md:253-260`) omit `backoff`, `backstop`, `jitter_fraction`, and `panic_policy`, all of which exist on the builder (`src/worker/mod.rs:222-273`). The table also lists `claimed_by` as a knob, but the identity is not configurable (`src/worker/mod.rs:898-904`).

## 2. Trait/context sketches predate later additions

- `task-model.md`'s `Task` sketch (lines 38-73) lacks `Task::lease` (`src/task.rs:255-258`), although `worker-model.md:177-178` references it.
- `task-model.md`'s `Context` sketch (lines 250-278) lacks `Context::id` and `Context::cancellation_token`, both shipped (`src/context.rs:111,160`; CHANGELOG "Unreleased").

## 3. `worker-model.md` "Loop resilience" overclaims

`worker-model.md:236-240` states a failed settlement "retries with backoff" and the listen connection "reconnects with backoff". Neither is true today. Settlement is single-attempt: a failure is logged and abandoned to lease recovery (`src/worker/mod.rs:664-666`), tracked in the settlement todo below. The reconnect is a fixed ~1s sleep (`src/postgres/notify.rs:93`), tracked in the notifier todo below. Either implement those todos and keep the text, or reword the doc to the current behavior; today it promises resilience properties the code lacks.

## 4. `observability.md` inaccuracies

- Claims "structured `tracing` spans and events" (`observability.md:12`). The code emits only events, no spans (`src/observability.rs`).
- The example counter list includes "released" (`observability.md:27-28`), but `observability::settled` is never called for a Release settlement. `Worker::release` writes to the store without emitting it (`src/worker/mod.rs:683-701`), so no released counter/label ever appears.

## 5. Stale ADR range in the index

`docs/design/README.md:55-56` says decisions are "recorded in `../adr/0001` through `0025`"; ADR 26 exists.

Source: review finding, `docs/design/*.md` checked file-by-file against `src/worker/mod.rs`, `src/task.rs`, `src/context.rs`, `src/observability.rs`, `src/postgres/notify.rs`, `migrations/*.sql`.

## Relations

- Relates to: [Settlement and shutdown-release are single-attempt](01KTPZPD9B4F0PJDMGQ5PJKXAH-settlement-and-shutdown-release-are-single-attempt.md), the settlement retry gap that Section 3 depends on.
- Relates to: [Notifier connect and reconnect are not resilient at startup or during outages](01KTQ1BD21JAJ99XR9V1NDWYW9-notifier-connect-and-reconnect-are-not-resilient-at-startup-or-during.md), the fixed reconnect sleep that Section 3 depends on.
