# FakeStore vs PostgresStore semantic parity

One change request: audit and align the in-memory `FakeStore` with the
PostgreSQL adapter (or document the divergences) so a unit test against the fake
does not pass while the real adapter would behave differently. Three known
divergences; all are about the fake silently disagreeing with PG.

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

## 3. `find_stale` batch bound and ordering

`PostgresStore::find_stale` returns at most 100 rows, oldest expiry first
(`ORDER BY claim_expires_at LIMIT 100`, `src/postgres/mod.rs:451-465`);
`FakeStore::find_stale` returns every stale claim in arbitrary map order
(`src/test_support.rs:348-361`). A test that relies on all stale claims being
recovered in one `recover_stale` pass passes against the fake but would need
several passes against PG once more than 100 claims are stale, and any
ordering assumption on the recovery sequence holds only on PG.

Fix: mirror the bound and ordering in the fake (sort by `claim_expires_at`,
truncate to 100), sharing the constant with the adapter so they cannot drift.

## Note

A related fake-fidelity item — how `FakeStore` computes `oldest_pending_age`
(`fakestore-oldest-pending-age-comment`) — is a pure comment clarification, not a
behavioral divergence, so it is kept as its own todo rather than folded here.

Source: review findings, `src/store.rs`, `src/postgres/mod.rs`
query_jobs/dedup_candidate, `src/test_support.rs`.
