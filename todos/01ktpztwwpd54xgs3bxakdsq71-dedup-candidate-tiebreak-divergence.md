# Dedup-candidate tie-break diverges fake vs PG (low confidence)

## Problem

`FakeStore::dedup_candidate` breaks an equal `created_at` deterministically by id
(`min_by_key((created_at, id))`), while the adapter's `ORDER BY created_at
LIMIT 1` leaves ties to the planner (`src/postgres/mod.rs` dedup_candidate). Only
observable with colliding timestamps.

Same fake-vs-PG divergence class as the `HistoryFilter::limit` todo.

## Suggested fix

Align the adapter's ordering with a deterministic tie-break (`ORDER BY
created_at, id`) so the fake and PG agree, or document that ties are unspecified.

Source: review finding R3, `src/postgres/mod.rs` dedup_candidate, `src/test_support.rs` dedup_candidate.
