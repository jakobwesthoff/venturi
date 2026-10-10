# Security review: add a dependency audit to CI (and the audit record)

Security pass over the full source tree, 2026-07-02. One actionable
finding; the rest of this file records what was audited and found clean so
the next security pass can diff against it instead of starting over.

## Actionable: no RustSec dependency audit anywhere

Neither the local toolchain nor CI runs any advisory audit:
`.github/workflows/ci.yml` runs only `just ci` and `just integration-test`;
no `cargo audit`/`cargo deny` step exists, and neither tool is installed
locally. `Cargo.lock` is committed (pinned transitive versions across ~the
whole tokio/postgres stack), so vulnerable-version detection is meaningful
and cheap, but today a RustSec advisory against any pinned dependency goes
unnoticed until someone happens to bump it.

Fix: add a CI step, e.g. `cargo install cargo-audit && cargo audit` (or the
`rustsec/audit-check` action, or `cargo-deny` with an `advisories` check,
which also covers license and duplicate-version policy). Run it on a
schedule as well as on push, since advisories land independently of
commits.

## Audited and found clean (with evidence)

- **SQL construction / injection.** Every dynamically built statement
  interpolates only: the table prefix, constant column lists
  (`JOB_COLUMNS`/`JOURNAL_COLUMNS`, `src/postgres/rows.rs:10,45`), constant
  clause fragments, and numeric parameter indices; all runtime values are
  bound as parameters (verified across every query in
  `src/postgres/mod.rs`, including the dynamically assembled
  `query_jobs`/`cleanup` WHERE builders at lines 552-656). The prefix is
  validated at the single construction choke point
  (`from_config` → `validate_prefix`, `src/postgres/mod.rs:76,795-827`:
  `^[a-z][a-z0-9_]*$`, ≤39 chars) and the struct fields are private, so no
  path constructs a store around the validation. The interpolated `LISTEN
  {channel}` (`src/postgres/notify.rs:152`) and refinery history-table name
  (`src/postgres/migrations.rs:65-68`) derive from the same validated
  prefix.
- **Unsafe code.** None in `src/` (grep verified). Panic containment uses
  safe `AssertUnwindSafe` with a documented soundness argument
  (`src/worker/registry.rs:181-190`).
- **Payload handling.** Payloads/carries/attachments are opaque
  `serde_json::Value`s; serde_json's recursion limit bounds parsing of
  hostile nesting, and the writers of the jobs table are within the trust
  boundary (they hold the same DB credentials). Undecodable rows surface
  as errors rather than panics (`src/postgres/rows.rs`; variant naming
  tracked separately in
  [[01kwh1210vhty9y9b4w2jaszvj-corrupt-row-errors-misreported-as-config]]).
- **Secret leakage.** venturi never logs the DSN or config; tracing sites
  log only error displays, kinds, counts, and durations
  (`src/observability.rs`, `tracing::warn!/error!` sites in
  `src/worker/mod.rs`, `src/postgres/notify.rs`). `PostgresStore` has no
  `Debug` impl that could dump connection parameters. One consumer-facing
  caveat worth documenting some day: handler error messages become
  persistent journal notes (`src/worker/mod.rs:842-847`), so secrets
  embedded in consumer error strings would be stored in the database and
  surfaced by history APIs — a consumer contract, not a venturi defect.
- **Resource-exhaustion angles** are covered by their own todos rather
  than repeated here:
  [[01kwh0rmbt948ff5xhyxeytxdw-context-history-unbounded-and-silently-empty]]
  (unbounded journal loads),
  [[01kwh0nrvc9wdv9gpmq46cxatk-dedup-key-unbounded-and-empty-values]]
  (index-row-size failures from user-derived keys),
  [[01kwh0yqcs9et504jaa7tw4gz7-advisory-lock-shares-global-int8-keyspace]]
  (cross-application advisory-lock blocking), and
  `todos/01ktq1bd21jaj99xr9v1ndwyw9-notifier-lifecycle-robustness.md`
  (reconnect spin during outages). NOTIFY floods are bounded by the
  capacity-1 coalescing receiver (`src/postgres/notify.rs:124-129`);
  `find_stale` is batch-bounded (`LIMIT 100`).
- **TLS.** Plaintext `connect` vs `connect_rustls`/`from_config` is an
  explicit, documented consumer choice (`src/postgres/mod.rs:90-117`); the
  listener always follows the pool's TLS path, so there is no accidental
  plaintext side channel for notifications.

Source: security review, `.github/workflows/ci.yml`, `Cargo.toml`,
`Cargo.lock`, full `src/` read.
