# venturi default adapter schema

Date: 2026-06-07

This document defines the PostgreSQL schema of venturi's **default storage
adapter**. Storage sits behind a backend trait (ADR 8), so these tables are the
default adapter's concern, not something the worker loop, registry, or task layers
depend on. The column and index choices realize decisions recorded across the
ADRs; the access paths the indexes serve are tracked in `docs/design/indexes.md`.

The adapter owns two tables, named from a configurable prefix (ADR 6):
`{{prefix}}_jobs` (the live queue and the durable job record) and
`{{prefix}}_journal` (the append-only per-execution log). The literal token
`{{prefix}}` below is what the migration files contain; it is replaced with the
configured prefix when migrations are applied.

## Migrations

Migrations are authored as ordinary SQL files that use the `{{prefix}}` token
wherever a table or index name appears. At apply time the adapter reads each file,
replaces `{{prefix}}` with the configured prefix using a plain string substitution,
and runs the result through refinery's runner, with refinery's migration-history
table name also set per prefix (ADR 24). Two queues with different prefixes in the
same database therefore track and apply their migrations independently.

The V1 migration is exactly the table and index DDL in this document, written with
the `{{prefix}}` placeholder. Index and constraint names are prefixed too, so for
a long prefix they must stay within PostgreSQL's 63-character identifier limit.

## `{{prefix}}_jobs`

```sql
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
```

| Column | Type | Null | Default | Purpose |
|---|---|---|---|---|
| `id` | `text` | no | — | The job's ULID in its canonical 26-character form; primary key (ADR 2). |
| `kind` | `text` | no | — | The task `KIND` discriminator that routes the job to its handler (task model). |
| `payload` | `jsonb` | no | — | The serialized task payload. |
| `priority` | `smallint` | no | `1` | Tier: 0 High, 1 Normal, 2 Low. Numeric so `ORDER BY priority ASC` puts High first; the `CHECK` pins the three tiers (ADR 22). |
| `status` | `text` | no | — | Lifecycle state, constrained to the four states (ADR 5). |
| `created_at` | `timestamptz` | no | — | Enqueue time; the age tiebreak in claim ordering (ADR 3). |
| `visible_at` | `timestamptz` | no | — | Eligibility gate: equals the enqueue time for immediate work, a future time for delayed, backoff, paused, or scheduled work (ADR 12, ADR 20). |
| `claim_expires_at` | `timestamptz` | yes | — | Lease expiry, set at claim; null when the row is not `claimed` (ADR 19). |
| `claimed_by` | `text` | yes | — | Claiming worker identity (`host:pid`); null when not `claimed` (ADR 19, ADR 20). |
| `finished_at` | `timestamptz` | yes | — | Set when the job reaches `completed` or `dead`; drives history queries by completion time (ADR 5, ADR 18). |
| `run_count` | `integer` | no | `0` | Incremented at each claim; read by `ctx.run_count()` (ADR 15). |
| `failure_count` | `integer` | no | `0` | Incremented on a retryable failure or a stale-recovery; the backstop reads it (ADR 13, ADR 19). |
| `carry` | `jsonb` | no | `'null'::jsonb` | The typed carried state; the default-empty `()` serializes to JSON `null` (ADR 15). |
| `dedup_key` | `text` | yes | — | Deduplication candidacy key; null means the task is never coalesced (ADR 10). |

## `{{prefix}}_journal`

```sql
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
```

| Column | Type | Null | Purpose |
|---|---|---|---|
| `id` | `bigint` identity | no | Surrogate primary key for the append-only log. |
| `job_id` | `text` | no | The job this entry belongs to; `ON DELETE CASCADE` gives unified cleanup, so removing a job removes its journal (ADR 16, ADR 18). |
| `kind` | `text` | no | Denormalized from the job, so the journal is queryable by kind without joining `{{prefix}}_jobs` (ADR 16, ADR 18). |
| `run_no` | `integer` | no | The run number this entry records. |
| `recorded_at` | `timestamptz` | no | When the entry was written. |
| `outcome` | `text` | no | The recorded outcome, constrained to the execution and lifecycle outcomes (ADR 11, ADR 16, ADR 19, ADR 21, ADR 10). |
| `note` | `text` | yes | The run's conclusion, or on failure the error message (ADR 11). |
| `attachment` | `jsonb` | yes | Structured evidence set via `ctx.set_attachment` during the run (ADR 15). |

## Indexes

Each index realizes an access path from `docs/design/indexes.md`. The DDL below
is the schema's **effective shape through migration V3**; the files under
`migrations/` are the source of truth and evolve the schema by appending new
versions (an applied migration's body is immutable, so a tuning change is a
`DROP`/`CREATE` in a later version rather than an edit to an earlier one).

```sql
-- #1 Claim path: highest-priority oldest eligible row per kind (ADR 3, 20, 22).
-- Two complementary partial indexes; the planner picks per query. `visible_at` is
-- a trailing key column (V3), filtered in-index without a heap fetch.
CREATE INDEX {{prefix}}_jobs_claim
    ON {{prefix}}_jobs (kind, priority, created_at, visible_at)
    WHERE status = 'pending';
CREATE INDEX {{prefix}}_jobs_claim_priority
    ON {{prefix}}_jobs (priority, created_at, visible_at)
    WHERE status = 'pending';

-- #2 Soonest future eligibility, for the worker's wait timeout (ADR 20).
CREATE INDEX {{prefix}}_jobs_visible
    ON {{prefix}}_jobs (kind, visible_at)
    WHERE status = 'pending';

-- #3 Deduplication candidacy lookup (ADR 10). Non-unique (see note). Trailing
-- `created_at` (V3) returns the oldest pending sibling without a sort.
CREATE INDEX {{prefix}}_jobs_dedup
    ON {{prefix}}_jobs (kind, dedup_key, created_at)
    WHERE dedup_key IS NOT NULL AND status = 'pending';

-- #4 Stale-claim recovery: expired leases (ADR 19).
CREATE INDEX {{prefix}}_jobs_lease
    ON {{prefix}}_jobs (claim_expires_at)
    WHERE status = 'claimed';

-- #5 History listing and status-filtered cleanup (ADR 5, ADR 18).
CREATE INDEX {{prefix}}_jobs_history
    ON {{prefix}}_jobs (status, kind, finished_at);

-- #6 Status-less terminal-job cleanup (V3): partial on terminal rows.
CREATE INDEX {{prefix}}_jobs_finished
    ON {{prefix}}_jobs (finished_at)
    WHERE finished_at IS NOT NULL;

-- History keyset pagination (V2): `created_at DESC, id DESC` backward scan.
CREATE INDEX {{prefix}}_jobs_created
    ON {{prefix}}_jobs (created_at, id);

-- #7 Journal per-job timeline and the FK cascade (ADR 16, ADR 18). Trailing `id`
-- (V3) returns the timeline in order without a sort.
CREATE INDEX {{prefix}}_journal_job
    ON {{prefix}}_journal (job_id, id);
```

Notes on specific indexes:

- **`_jobs_claim` / `_jobs_claim_priority` and the `visible_at` predicate.** Both
  are partial on `status = 'pending'` and lead with the claim's `ORDER BY priority,
  created_at`; with `LIMIT 1 FOR UPDATE SKIP LOCKED` the planner takes the
  highest-priority oldest eligible row. `visible_at` is the trailing key column, so
  `visible_at <= now()` is evaluated in-index as a non-boundary scan key (an
  `Index Cond`, not a heap `Filter`): future-visible rows — which keep their
  original `created_at` and so cluster at the front of the claim order — are skipped
  without a heap fetch, while the `(priority, created_at)` prefix still drives the
  ordered, pipelined top-1. (V1 originally left `visible_at` as a heap residual on
  the narrower premise that a btree cannot *range*-filter it and preserve the
  order; V3 carries it as an in-index filter instead.)
- **`_jobs_visible`** is a separate partial-pending index because the wait-timeout
  lookup orders by `visible_at`, a different ordering than the claim index's
  `(priority, created_at)`; one btree cannot serve both orderings (noted in the
  tracker).
- **`_jobs_dedup` is non-unique.** The `merge` decision's `Independent` outcome
  (ADR 10) permits more than one pending row with the same `(kind, dedup_key)`, so
  the index is a lookup aid, not a uniqueness constraint.
- **History listing and cleanup.** `_jobs_history` serves status+kind+`finished_at`
  listing and a status-filtered cleanup directly. A status-less cleanup
  (`finished_at < cutoff`, no status) has no leading-column match on it, so the
  partial `_jobs_finished` (V3) serves that bulk delete as an indexed range scan
  over only the terminal rows.
- PostgreSQL does not auto-create an index on a foreign key's referencing column,
  so `_journal_job` is defined explicitly to back both the per-job timeline read
  (whose `ORDER BY id` the trailing `id` satisfies) and the `ON DELETE CASCADE`.
- **No journal-by-kind index.** A speculative `(kind, recorded_at)` index existed
  in V1 for a "journal global queries by kind over time" path that no code issues;
  it was unused and dropped in V3. The denormalized journal `kind` column remains
  for ad-hoc operator SQL, unindexed.

## Out of scope / not yet decided

- **Rate-control state.** Columns or tables for throttling a kind over time are
  deferred and tracked as a todo; nothing here supports rate limiting.
- **Global cross-worker coordination tables.** Per-kind concurrency caps are
  local, in-memory worker state (ADR 23), so no shared coordination table exists;
  any future global cap or rate limit would add its own.
- **Observability and metrics tables.** Logging, queue-state introspection, and
  metric emission are a separate part and add no schema here.
