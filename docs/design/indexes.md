# Database index requirements

A running tracker of the indexes venturi needs for fast operation. Each entry ties
an access path to the decision that introduces it. This feeds the eventual schema
design document; the exact DDL and final column ordering are settled there, but
the *requirements* are recorded here as they are decided so none is lost.

Organizing principle (ADR 5): hot-path indexes over the jobs table are **partial
on `status`**, so they stay small and never scan retained terminal rows
(`completed`, `dead`). Terminal-row access paths (listing, cleanup) use their own
indexes.

## Jobs table

| # | Access path | Filter / key | Order / aggregate | Index requirement | Source |
|---|---|---|---|---|---|
| 1 | Claim next eligible job | `status = 'pending' AND kind = ANY(kinds) AND visible_at <= now` | `priority ASC, created_at ASC`, `LIMIT 1 FOR UPDATE SKIP LOCKED` | Two complementary partial indexes `WHERE status = 'pending'`: `(kind, priority, created_at, visible_at)` (selective when a worker's kinds are few or sparse) and `(priority, created_at, visible_at)` (walks the claim's order so a multi-kind claim is an indexed top-1, not a sort of every candidate). The planner picks per query. `visible_at` is a **trailing key column** (V3, not a heap residual): `visible_at <= now()` is evaluated in-index as a non-boundary scan key, so future-visible rows are skipped without a heap fetch while the `(priority, created_at)` prefix preserves the claim order. | ADR 3, 20 |
| 2 | Soonest future eligibility | `status = 'pending' AND visible_at > now AND kind = ANY(kinds)` | `min(visible_at)` | Partial index `WHERE status = 'pending'` ordered by `visible_at` (to read the nearest future eligibility cheaply). May or may not combine with #1; reconcile in schema design. | ADR 20 |
| 3 | Deduplication candidacy lookup | `kind = ? AND dedup_key = ? AND status = 'pending'` | oldest sibling: `created_at ASC LIMIT 1` | Partial index on `(kind, dedup_key, created_at)` `WHERE dedup_key IS NOT NULL AND status = 'pending'` (kept small: only pending, dedupable rows; trailing `created_at` returns the oldest sibling without a sort, V3). | ADR 10 |
| 4 | Stale-claim recovery | `status = 'claimed' AND claim_expires_at < now` | scan expired | Partial index on `claim_expires_at` `WHERE status = 'claimed'`. | ADR 19 |
| 5 | History listing | `status = ? AND kind = ? AND finished_at` within a window | by `finished_at` | Index on the terminal-row listing columns, e.g. `(status, kind, finished_at)`. Not partial on a single status, since it serves `completed` and `dead`. | ADR 18 |
| 6 | Terminal-job cleanup | `finished_at < cutoff` (optionally `+ status`, `+ kind`) | bulk delete | A status-filtered cleanup uses #5's `status` prefix. A status-less cleanup has no leading-column match on #5, so a partial index on `(finished_at)` `WHERE finished_at IS NOT NULL` (V3) serves it directly; partial so live rows never enter it. | ADR 5, 18 |

## Journal table

| # | Access path | Filter / key | Order | Index requirement | Source |
|---|---|---|---|---|---|
| 7 | Per-job timeline + cascade cleanup | `job_id = ?` | by `id ASC` | Index on `(job_id, id)` (leading `job_id` serves reading a job's full history and the `ON DELETE CASCADE`; trailing `id` returns the timeline in order without a sort, V3). | ADR 16, 18 |
| 8 | Global journal query | `kind = ?` within a time window | by recorded time | _Speculated, not built._ No code path issues a journal query by kind/time; the only journal read is per-job (#7). The `(kind, recorded_at)` index originally added for this was unused and dropped in V3. The denormalized `kind` column remains for ad-hoc operator SQL, unindexed. | ADR 16, 18 |

## Notes

- Columns referenced above (`kind`, `priority`, `status`, `created_at`,
  `visible_at`, `claim_expires_at`, `finished_at`, `dedup_key`, journal `outcome`
  / recorded time / `run_no`) are the ones these access paths touch. The full
  column set and types are settled in the schema design, not here.
- Entries #1 and #2 want different orderings of the same partial-pending set
  (`(priority, created_at)` vs `visible_at`); whether one composite index serves
  both or they are separate is a schema-design decision.
- The claim keeps two partial indexes (entry #1) because a single `kind`-leading
  index cannot serve the multi-kind claim well: with several kinds it cannot
  produce the global `(priority, created_at)` order, so it gathers every matching
  pending row and sorts the set (spilling to disk), and with many kinds the planner
  drops the index for a sequential scan. `EXPLAIN ANALYZE` over a 200k-row pending
  set across twelve kinds measured this at tens of milliseconds, against ~0.03 ms
  once a `(priority, created_at)`-leading partial index let the claim walk in order
  and stop at the first lockable match. The `kind`-leading index is retained for
  workers whose kinds are sparse, where touching only those kinds' entries wins.
- Introspection stats (ADR 25) are on-demand aggregate queries (counts by status
  and kind, oldest-pending age, in-flight and dead counts). They are not hot-path
  and reuse the claim and history indexes where applicable, otherwise scanning the
  bounded pending set; no dedicated index is added now, but a heavy stat could
  justify one later.
- This tracker is updated whenever a new decision introduces an access path
  (for example, rate control may add one).
