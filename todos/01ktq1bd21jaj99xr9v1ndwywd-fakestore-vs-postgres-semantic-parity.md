# FakeStore vs PostgresStore semantic parity

One change request: audit and align the in-memory `FakeStore` with the
PostgreSQL adapter (or document the divergences) so a unit test against the fake
does not pass while the real adapter would behave differently. Two known
divergences; both are about the fake silently disagreeing with PG.

## 1. `HistoryFilter::limit` negative value

`HistoryFilter.limit: Option<i64>` (`src/store.rs`). `FakeStore::query_jobs`
clamps via `limit.max(0) as usize` (treats negative as 0); `PostgresStore`
binds the raw `i64` into `LIMIT $N`, which PostgreSQL rejects at runtime. A user
who tests against the fake never sees the failure PG raises in production.

Fix (decision): change the field to `Option<u64>` (removes the invalid domain at
compile time; cast to `i64` for the bind — a pre-1.0 breaking change), or add a
matching runtime clamp/guard in `PostgresStore::query_jobs`.

## 2. Dedup-candidate tie-break (low confidence)

`FakeStore::dedup_candidate` breaks an equal `created_at` deterministically by id
(`min_by_key((created_at, id))`); the adapter's `ORDER BY created_at LIMIT 1`
leaves ties to the planner. Only observable with colliding timestamps.

Fix: give the adapter a deterministic tie-break (`ORDER BY created_at, id`) so the
two agree, or document that ties are unspecified.

## Note

A related fake-fidelity item — how `FakeStore` computes `oldest_pending_age`
(`fakestore-oldest-pending-age-comment`) — is a pure comment clarification, not a
behavioral divergence, so it is kept as its own todo rather than folded here.

Source: review findings, `src/store.rs`, `src/postgres/mod.rs`
query_jobs/dedup_candidate, `src/test_support.rs`.
