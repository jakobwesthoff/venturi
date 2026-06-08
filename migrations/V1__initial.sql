-- venturi default adapter schema, version 1.
--
-- The literal token `{{prefix}}` is replaced with the configured table prefix by
-- a plain string substitution before this file is handed to refinery. Index and
-- constraint names are prefixed too, so a prefix must stay short enough to keep
-- every generated identifier within PostgreSQL's 63-character limit.

-- =============================================================================
-- {{prefix}}_jobs: the live queue and the durable job record.
-- =============================================================================
CREATE TABLE {{prefix}}_jobs (
    id                text        PRIMARY KEY,
    kind              text        NOT NULL,
    payload           jsonb       NOT NULL,
    priority          smallint    NOT NULL DEFAULT 1
                                  CHECK (priority IN (0, 1, 2)),
    status            text        NOT NULL
                                  CHECK (status IN ('pending', 'claimed', 'completed', 'dead')),
    created_at        timestamptz NOT NULL,
    visible_at        timestamptz NOT NULL,
    claim_expires_at  timestamptz,
    claimed_by        text,
    finished_at       timestamptz,
    run_count         integer     NOT NULL DEFAULT 0,
    failure_count     integer     NOT NULL DEFAULT 0,
    carry             jsonb       NOT NULL DEFAULT 'null'::jsonb,
    dedup_key         text
);

-- =============================================================================
-- {{prefix}}_journal: the append-only per-execution log.
-- =============================================================================
CREATE TABLE {{prefix}}_journal (
    id           bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id       text        NOT NULL
                             REFERENCES {{prefix}}_jobs (id) ON DELETE CASCADE,
    kind         text        NOT NULL,
    run_no       integer     NOT NULL,
    recorded_at  timestamptz NOT NULL,
    outcome      text        NOT NULL
                             CHECK (outcome IN ('completed', 'paused', 'retried',
                                                'dead', 'stale-recovered',
                                                'released', 'merged')),
    note         text,
    attachment   jsonb
);

-- =============================================================================
-- Indexes: each realizes one access path.
-- =============================================================================

-- Claim path. The claim selects the highest-priority oldest eligible row among a
-- worker's kinds, ordered by `(priority, created_at)`. Two complementary partial
-- indexes serve it, and the planner picks per query and statistics; `visible_at`
-- is a residual filter in both (a btree cannot range-filter it and preserve the
-- priority/age ordering).
--
-- The kind-leading index is selective when a worker handles few kinds or its
-- kinds are sparse in the pending set: it touches only those kinds' entries.
CREATE INDEX {{prefix}}_jobs_claim
    ON {{prefix}}_jobs (kind, priority, created_at)
    WHERE status = 'pending';

-- The priority-leading index walks rows in the claim's own `(priority,
-- created_at)` order, so a multi-kind claim filters `kind` as a residual and stops
-- at the first lockable match instead of collecting every candidate and sorting.
-- This keeps the common multi-kind claim an indexed top-1 rather than a
-- sort-the-whole-pending-set (or, for many kinds, a sequential scan).
CREATE INDEX {{prefix}}_jobs_claim_priority
    ON {{prefix}}_jobs (priority, created_at)
    WHERE status = 'pending';

-- Soonest future eligibility, for the worker's wait timeout. Different ordering
-- than the claim index, so a separate partial-pending index.
CREATE INDEX {{prefix}}_jobs_visible
    ON {{prefix}}_jobs (kind, visible_at)
    WHERE status = 'pending';

-- Deduplication candidacy lookup. Non-unique: the merge decision's Independent
-- outcome permits sibling pending rows with the same (kind, dedup_key).
CREATE INDEX {{prefix}}_jobs_dedup
    ON {{prefix}}_jobs (kind, dedup_key)
    WHERE dedup_key IS NOT NULL AND status = 'pending';

-- Stale-claim recovery: expired leases.
CREATE INDEX {{prefix}}_jobs_lease
    ON {{prefix}}_jobs (claim_expires_at)
    WHERE status = 'claimed';

-- History listing and terminal-job cleanup.
CREATE INDEX {{prefix}}_jobs_history
    ON {{prefix}}_jobs (status, kind, finished_at);

-- Journal per-job timeline and the FK cascade (PostgreSQL does not auto-index
-- the referencing column of a foreign key).
CREATE INDEX {{prefix}}_journal_job
    ON {{prefix}}_journal (job_id);

-- Journal global queries by kind over time.
CREATE INDEX {{prefix}}_journal_kind
    ON {{prefix}}_journal (kind, recorded_at);
