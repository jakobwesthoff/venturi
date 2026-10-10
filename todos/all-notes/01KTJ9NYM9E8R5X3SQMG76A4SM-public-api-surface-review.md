# Review the public API surface and stability

**Status:** decisions made during the build that affect the public contract;
worth a dedicated review against the Rust API guidelines before a stable release.

## Items to review

- **`Store` trait was grown phase-by-phase** rather than fully specified up front
  (a deliberate deviation from the plan's "all signatures in P0", flagged at the
  time). It is now a large **public** trait. Decide whether to: keep it public and
  stable (third parties can implement alternative backends), seal it
  (`mod sealed`), or split read/write/admin concerns. This determines how much of
  the storage contract is a stability commitment.
- **`TaskError` deliberately does NOT implement `std::error::Error`.** This is
  required so the blanket `From<E: Error>` (which gives the ergonomic `?`-retries)
  does not collide with the reflexive `From<T> for T`. Consequence: `TaskError`
  can't be used directly with `anyhow`/`?` in *outer* code, and tools expecting
  `Error` won't accept it. Confirm this trade-off is acceptable, or find an
  alternative (e.g. a dedicated `IntoTaskError` trait instead of blanket `From`).
- **No high-level manual stale-recovery API.** Recovery is exposed only via the
  low-level `Store::find_stale` + `Store::recover`. ADR 19 mentions a manual
  operation "for an external sweeper". Decide whether a convenience like
  `Queue::recover_stale()` / a `Maintenance` handle is wanted (it needs the
  worker's backoff config to compute re-pend delays, which the `Queue` does not
  currently hold).
- **Storage value types are exposed as the history/stats return types**
  (`JobRecord`, `JournalRecord`, `Snapshot`, `HistoryFilter`, `CleanupCriteria`,
  `Status`, `JournalOutcome` from `venturi::store`). Confirm these are the right
  *public* operations surface, or whether the operations API should return
  dedicated, leaner view types instead of raw store records.
- **`Queue::store()` / `PostgresStore::pool()` accessors** leak the backend
  handles. Confirm that is intended (useful for sharing a pool) vs. encapsulation.
- General Rust API guidelines pass: `#[non_exhaustive]` on public enums/structs
  that may grow (e.g. `Snapshot`, `HistoryFilter`, `Outcome`?), `must_use` where
  appropriate, naming, and `Debug`/`Clone` derive coverage.

## Decision needed

A pre-1.0 API-stability review: what is part of the stable contract, what gets
sealed/`non_exhaustive`, and confirmation of the `TaskError`-not-`Error` choice.
