-- venturi default adapter schema, version 3.
--
-- Index tuning from a full query/index audit. Each change is a DROP + CREATE
-- against indexes defined in V1, because an already-applied migration's body is
-- immutable (refinery checksums it): all schema evolution lands in a new version.
-- The `{{prefix}}` token is substituted before refinery runs this, as in V1/V2.

-- =============================================================================
-- Claim hot path: filter `visible_at` inside the index.
-- =============================================================================
--
-- The claim runs `... WHERE status = 'pending' AND visible_at <= now()
-- AND kind = ANY($) AND priority >= $ ORDER BY priority, created_at LIMIT 1`.
-- V1 left `visible_at` as a heap residual, reasoning that a btree cannot use it
-- without losing the `(priority, created_at)` ordering. That holds only for a
-- *boundary* (range start/stop) use. Appended as a trailing key column,
-- `visible_at <= now()` is instead evaluated as a non-boundary scan key against
-- each index tuple (an `Index Cond`, not a heap `Filter`), so ineligible —
-- typically future-visible — rows are skipped without a heap fetch, while the
-- leading `(priority, created_at)` prefix still drives the ordered, pipelined
-- `LIMIT 1`.
--
-- This matters because a retry or scheduled job keeps its original `created_at`
-- (a retry settlement rewrites only `visible_at`), so future-visible rows cluster
-- at the front of the claim order and would otherwise be heap-fetched and
-- rejected by every claim of every worker until they mature.
DROP INDEX {{prefix}}_jobs_claim;
CREATE INDEX {{prefix}}_jobs_claim
    ON {{prefix}}_jobs (kind, priority, created_at, visible_at)
    WHERE status = 'pending';

DROP INDEX {{prefix}}_jobs_claim_priority;
CREATE INDEX {{prefix}}_jobs_claim_priority
    ON {{prefix}}_jobs (priority, created_at, visible_at)
    WHERE status = 'pending';

-- =============================================================================
-- Dedup candidate: serve `ORDER BY created_at LIMIT 1` from the index.
-- =============================================================================
--
-- `dedup_candidate` picks the oldest pending sibling for `(kind, dedup_key)`.
-- Appending `created_at` returns that row directly instead of sorting siblings.
DROP INDEX {{prefix}}_jobs_dedup;
CREATE INDEX {{prefix}}_jobs_dedup
    ON {{prefix}}_jobs (kind, dedup_key, created_at)
    WHERE dedup_key IS NOT NULL AND status = 'pending';

-- =============================================================================
-- Terminal-job cleanup without a status filter.
-- =============================================================================
--
-- A status-less `cleanup` predicates on `finished_at IS NOT NULL AND
-- finished_at < $1`, which the `(status, kind, finished_at)` history index
-- cannot serve (no leading-column match), forcing a sequential scan. This
-- partial index leads with `finished_at` and covers exactly the terminal rows,
-- making the delete an indexed range scan; live rows never enter it.
CREATE INDEX {{prefix}}_jobs_finished
    ON {{prefix}}_jobs (finished_at)
    WHERE finished_at IS NOT NULL;

-- =============================================================================
-- Journal per-job timeline: serve `ORDER BY id` from the index.
-- =============================================================================
--
-- `journal(job_id)` reads a job's entries `ORDER BY id`. Entries under one
-- `job_id` are TID-ordered, not id-ordered, so the V1 `(job_id)` index needed a
-- sort node; `(job_id, id)` returns them already ordered. The leading `job_id`
-- still backs the `ON DELETE CASCADE` foreign key, so this fully replaces it.
DROP INDEX {{prefix}}_journal_job;
CREATE INDEX {{prefix}}_journal_job
    ON {{prefix}}_journal (job_id, id);

-- =============================================================================
-- Drop the unused journal-by-kind index.
-- =============================================================================
--
-- `{{prefix}}_journal_kind (kind, recorded_at)` was created for "journal global
-- queries by kind over time", but no code path issues such a query: the only
-- journal read is per-job by `job_id`. It cost a write on every settle, recover,
-- and merge for no read benefit, so it is removed.
DROP INDEX {{prefix}}_journal_kind;
