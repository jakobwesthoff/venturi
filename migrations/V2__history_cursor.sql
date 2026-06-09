-- venturi default adapter schema, version 2.
--
-- Adds the index that backs keyset history pagination. The literal `{{prefix}}`
-- token is substituted with the configured table prefix before this file is
-- handed to refinery, exactly as in V1.

-- =============================================================================
-- History pagination keyset scan.
-- =============================================================================
--
-- The history query orders by `(created_at DESC, id DESC)` and pages with a
-- `(created_at, id)` keyset bound. A btree on `(created_at, id)` serves both: a
-- page is a backward range scan that stops after `LIMIT` rows, with no sort and
-- no offset to walk past.
--
-- The existing `{{prefix}}_jobs_history` index leads with `(status, kind,
-- finished_at)` for terminal-job cleanup and cannot provide this ordering, so
-- this is a distinct access path rather than a replacement. A kind/status-
-- filtered page uses this index for the ordering and applies the filter as a
-- residual; a dedicated composite (e.g. `(kind, created_at, id)`) can be added
-- later if a selective filtered listing proves hot enough to justify the extra
-- write cost on the enqueue/claim path.
CREATE INDEX {{prefix}}_jobs_created
    ON {{prefix}}_jobs (created_at, id);
