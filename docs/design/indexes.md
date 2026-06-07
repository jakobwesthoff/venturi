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
| 1 | Claim next eligible job | `status = 'pending' AND kind = ANY(kinds) AND visible_at <= now` | `priority ASC, created_at ASC`, `LIMIT 1 FOR UPDATE SKIP LOCKED` | Partial index `WHERE status = 'pending'` supporting the kind filter, the `visible_at` eligibility bound, and the `(priority, created_at)` ordering. | ADR 3, 20 |
| 2 | Soonest future eligibility | `status = 'pending' AND visible_at > now AND kind = ANY(kinds)` | `min(visible_at)` | Partial index `WHERE status = 'pending'` ordered by `visible_at` (to read the nearest future eligibility cheaply). May or may not combine with #1; reconcile in schema design. | ADR 20 |
| 3 | Deduplication candidacy lookup | `kind = ? AND dedup_key = ? AND status = 'pending'` | one row | Partial index on `(kind, dedup_key)` `WHERE dedup_key IS NOT NULL AND status = 'pending'` (kept small: only pending, dedupable rows). | ADR 10 |
| 4 | Stale-claim recovery | `status = 'claimed' AND claim_expires_at < now` | scan expired | Partial index on `claim_expires_at` `WHERE status = 'claimed'`. | ADR 19 |
| 5 | History listing | `status = ? AND kind = ? AND finished_at` within a window | by `finished_at` | Index on the terminal-row listing columns, e.g. `(status, kind, finished_at)`. Not partial on a single status, since it serves `completed` and `dead`. | ADR 18 |
| 6 | Terminal-job cleanup | `status IN ('completed','dead') AND finished_at < cutoff` | bulk delete | Index on `(status, finished_at)`; may reuse #5. | ADR 5, 18 |

## Journal table

| # | Access path | Filter / key | Order | Index requirement | Source |
|---|---|---|---|---|---|
| 7 | Per-job timeline + cascade cleanup | `job_id = ?` | by `run_no` / recorded time | Index on `job_id` (serves both reading a job's full history and removing its entries when the job is cleaned). | ADR 16, 18 |
| 8 | Global journal query | `kind = ? AND outcome = ?` within a time window | by recorded time | Index on the denormalized `kind` plus recorded time (and/or `outcome`), so the journal is queryable directly without joining the jobs table. | ADR 16, 18 |

## Notes

- Columns referenced above (`kind`, `priority`, `status`, `created_at`,
  `visible_at`, `claim_expires_at`, `finished_at`, `dedup_key`, journal `outcome`
  / recorded time / `run_no`) are the ones these access paths touch. The full
  column set and types are settled in the schema design, not here.
- Entries #1 and #2 want different orderings of the same partial-pending set
  (`(priority, created_at)` vs `visible_at`); whether one composite index serves
  both or they are separate is a schema-design decision.
- Introspection stats (ADR 25) are on-demand aggregate queries (counts by status
  and kind, oldest-pending age, in-flight and dead counts). They are not hot-path
  and reuse the claim and history indexes where applicable, otherwise scanning the
  bounded pending set; no dedicated index is added now, but a heavy stat could
  justify one later.
- This tracker is updated whenever a new decision introduces an access path
  (for example, rate control may add one).
