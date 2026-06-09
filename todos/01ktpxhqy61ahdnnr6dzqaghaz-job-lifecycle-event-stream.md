# Add a job-lifecycle event stream (payload NOTIFY + subscribe API)

**Status:** designed, deferred. A downstream consumer (latent-ink's Activity
view) wants to observe job lifecycle transitions live — to patch individual rows
in a UI instead of refetching — and venturi has no API for that today. This is a
new, reusable public capability, not an app hack.

## Why the existing notification path cannot be reused

`PostgresStore` already has `LISTEN`/`NOTIFY`, but it is a **worker wakeup**, not
an event feed (`src/postgres/mod.rs`, `notify`/`notifies_on_repend`):

- The payload is empty (`pg_notify($1, '')`) — it carries no job id or status.
- It is coalesced to capacity 1 (`src/postgres/notify.rs` `listen`), so distinct
  transitions collapse into a single "re-query" wakeup.
- It fires only on re-pending transitions (enqueue, retry, pause, release,
  stale-recover). It deliberately does **not** fire on `Complete` or `Dead`,
  because those produce no claimable work.

A UI feed needs the opposite: a per-transition event, carrying enough row data to
patch a row, on *every* transition including the terminal ones. So this is a new
mechanism alongside the wakeup, not a change to it.

## Decided shape

- **Transport: a second NOTIFY channel `{prefix}_events`** carrying a lean JSON
  payload, distinct from the coalesced `{prefix}_jobs` wakeup channel. Chosen over
  an in-process `tokio::sync::broadcast` because venturi advertises multi-host
  operation (`src/store.rs` `find_stale` doc): an in-process bus would silently
  miss events emitted by other worker processes. NOTIFY is distribution-correct
  (any LISTENing process sees every event) and atomic with the row commit (issued
  inside the same transaction, like the existing wakeup).

## Design

### Event value type (`src/store.rs`)

A lean `JobEvent` that is the stable vocabulary of the feed:

```
JobEvent {
    transition: EventKind,   // Enqueued, Claimed, Completed, Retried,
                             // Paused, Dead, Released, StaleRecovered, Merged
    id, kind, status, priority,
    created_at, finished_at, run_count, failure_count,
}
```

- Deliberately omits `payload`/`carry`: events are about lifecycle, not content,
  and this keeps the NOTIFY payload small (Postgres caps it at 8000 bytes).
- Carries every other `JobRecord` field, so a consumer can both patch an existing
  row and materialise a brand-new one with no follow-up query.
- `EventKind` is one variant per row mutation. Mark both types `non_exhaustive`
  if they may grow.

### Subscription API

- `Store::subscribe_events(&self) -> Result<Box<dyn EventStream>, Error>` (or a
  receiver type), mirroring the existing `Store::notifier`. Default impl: a stream
  that never yields (parallel to `NeverNotifier`), so non-postgres backends opt
  out cleanly. Postgres overrides it with a payload `LISTEN`.
- Re-expose as `Queue::subscribe_events()` so producers/consumers reach it from
  the public handle (`src/queue.rs`).
- The payload listener is a variant of `src/postgres/notify.rs` `listen()` that
  forwards `notification.payload().to_owned()` instead of `()`, over a **larger,
  non-coalesced** channel (each event matters individually; capacity-1 coalescing
  would drop events). Parse JSON → `JobEvent` in the subscription's `recv`.

### Emission points (`src/postgres/mod.rs`)

Emit `pg_notify('{prefix}_events', json)` inside the existing transaction in:
`enqueue`, `settle` (all five `Settlement` arms), `recover`, `merge_into`, and
`claim_next`.

- **Build the event uniformly from a `JobRecord`** (drop payload/carry, attach the
  `EventKind`), reusing `rows::job_from_row`.
- `claim_next` already does `RETURNING {columns}` — the row is in hand. It is a
  bare `UPDATE…RETURNING` on a pooled connection today, so wrap it in a
  transaction so the claim and its event commit atomically.
- `settle` and `recover` currently `tx.execute` their `UPDATE`s; change to
  `RETURNING {columns}` + `query_opt` so the post-transition row is available to
  build the event.
- `enqueue` has no row to return; build the event from the `NewJob`
  (status=pending, run_count=0, failure_count=0, finished_at=None).
- `merge_into` changes a pending row's payload/priority; return or rebuild the
  surviving row for the `Merged` event.

## Problems / caveats to handle

- **Reconnect gap.** Like `PgNotifier`, the events listener must reconnect on a
  dropped connection; events delivered during the gap are lost (NOTIFY has no
  backlog). The subscription should surface a reconnect as a distinct signal (a
  `Resync`/`Lagged`-style marker) so a consumer can refetch once rather than drift
  silently. Document that the feed is **at-most-once** across reconnects, by
  design — durability lives in the job rows, not the event channel.
- **Ordering.** NOTIFY delivers in commit order per connection; fine for a single
  listener. Document that no global ordering is promised across listeners.
- **Channel sizing.** A non-coalesced bounded channel can still fill under a flood;
  on overflow, prefer signalling a resync over unbounded buffering.
- **`claim_next` transaction cost.** Wrapping the claim in a transaction adds a
  round trip on the hot claim path. Measure; the claim is already a single
  statement so the overhead should be small, but confirm it does not regress
  throughput under contention.

## API-stability notes (fold into the pre-1.0 review)

See `01ktj9nym9e8r5x3sqmg76a4sm-public-api-surface-review.md`. `JobEvent` /
`EventKind` join the public vocabulary; decide `non_exhaustive`, and whether the
default-empty-stream pattern on `Store` is the right opt-out for alternative
backends.

## Downstream

latent-ink consumes this to push row-level updates to its Activity view over SSE
(latent-ink todo `01ktpxhqy61ahdnnr6dzqaghb0-live-activity-via-sse.md`). That work
is blocked on this feature landing.
