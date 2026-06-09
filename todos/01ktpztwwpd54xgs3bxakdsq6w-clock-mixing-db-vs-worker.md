# Clock mixing between DB time and worker time (low confidence)

## Problem

Claims and lease expiry use database `now()` (`src/postgres/mod.rs` claim_next,
find_stale, recover), while settle/recovery timestamps and retry `visible_at` use
the worker's `Utc::now()` (`src/worker/mod.rs`). Under worker clock skew,
retry/pause schedules and `finished_at` shift relative to the lease arithmetic.

Likely immaterial at realistic skews; recorded for awareness, not action.

## Notes

If ever addressed, the consistent choice is to compute scheduling instants in
database time as well, or to document the assumption that worker and database
clocks are reasonably aligned.

Source: review finding, REST bucket.
