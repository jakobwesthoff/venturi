# Minor implementation decisions to confirm

**Status:** small choices made during the build; individually low-stakes, grouped
here so none is lost. Confirm-or-change.

- **`Cargo.lock` is committed.** Library convention is debatable; included for
  reproducible integration-test builds. Decide whether to keep it tracked or
  `.gitignore` it.
- **Table-prefix max length is 39 chars.** Derived so the longest generated
  identifier `{prefix}_refinery_schema_history` stays within Postgres's 63-char
  limit. The validator also requires `^[a-z][a-z0-9_]*$`. Confirm the rule set.
- **Worker identity `host:pid` uses `HOSTNAME` env or `"unknown"`.** There is no
  real hostname syscall (no `hostname`/`gethostname` dependency). `claimed_by` is
  diagnostic-only (recovery is timeout-based), so this is low-risk, but the host
  part is `"unknown"` when `HOSTNAME` is unset. Decide whether to add a real
  hostname lookup.
- **Dedup candidate selection = oldest pending** (`ORDER BY created_at LIMIT 1`)
  when several pending siblings share `(kind, dedup_key)` (possible after
  `Merge::Independent`). Confirm "oldest" is the right pick vs. newest/any.
- **`find_stale` batches at `LIMIT 100`** per recovery pass (recovered over
  several loop ticks). Confirm the batch size / that bounded recovery is fine.
- **`NOTIFY` on enqueue is a separate statement** after the `INSERT`, not in the
  same transaction (`PostgresStore::enqueue`). A crash between insert and notify
  loses the notify, but the poll/`next_visible_at` fallback still picks the job
  up. Tie-in with [[wakeup-notification-architecture]]; confirm acceptable or
  fold the notify into the insert path / a trigger.
- **Claim-latency metric = wait since `created_at`.** `observability::claimed`
  records `now - created_at` as "claim latency". Confirm that is the intended
  meaning (vs. time the claim query took, or wait since `visible_at`).
- **`metrics-util` is a dev-dependency** only (for the metrics test recorder);
  the runtime `metrics` dep is feature-gated. No action unless the test-recorder
  approach changes.
