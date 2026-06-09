# Minor review findings (low severity, batched)

Collected low-severity items from the full-codebase review. Each is small and
independent; none is a correctness defect.

## 1. `apply()` migration comment overstates the substitution

`src/postgres/migrations.rs:50-58`: the comment says the prefix is substituted
"into each migration's name and body" and that the name's prefix keeps refinery's
recorded identities distinct. The code substitutes only the body
(`sql.replace("{{prefix}}", prefix)`); the name passed to `Migration::unapplied`
is the literal `V1__initial` etc. Isolation actually comes from the per-prefix
history table (`set_migration_table_name`). The comment is wrong; the code is
correct. Fix the comment to describe the body-only substitution and the
history-table isolation.

## 2. Keep merge churns a dead row version

`src/postgres/mod.rs:689-696`: the Keep branch issues `SET dedup_key = dedup_key`
purely to test whether the candidate is still pending. That writes a new row
version (plus WAL) on every Keep. An `EXISTS` / `SELECT ... FOR UPDATE` inside
the transaction would report pendingness without churning the row. Low impact
unless Keep merges are very frequent.

## 3. Clock mixing between DB time and worker time (low confidence)

Claims and lease expiry use database `now()` (`src/postgres/mod.rs` claim_next,
find_stale, recover), while settle/recovery timestamps and retry `visible_at`
use the worker's `Utc::now()`. Under worker clock skew, retry/pause schedules
and `finished_at` shift relative to lease arithmetic. Likely immaterial at
realistic skews; noted for awareness, not action.

## 4. `NewJob.priority` accepts out-of-range tiers from direct `Store` users

`src/store.rs` `NewJob.priority: i16` lets a direct `Store` consumer pass a value
outside `0..=2`, learning of it only via the schema CHECK-constraint error. The
`Queue` handle always passes valid values, so library users on the intended path
are unaffected. Consider validating at the `Store::enqueue` boundary or
documenting the constraint on `NewJob.priority`.

Source: full-codebase review, REST bucket.
