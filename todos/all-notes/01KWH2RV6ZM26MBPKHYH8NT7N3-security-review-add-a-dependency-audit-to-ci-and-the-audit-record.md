---
title: "Security review: add a dependency audit to CI (and the audit record)"
kind: chore
component: project
origin: review
tags: [security, ci]
---
# Security review: add a dependency audit to CI (and the audit record)

Found in the 2026-07-02 security review of the full source tree. One actionable finding; the rest of this file records what was audited and found clean so the next security pass can diff against it instead of starting over.

## Actionable: no RustSec dependency audit anywhere

Neither the local toolchain nor CI runs any advisory audit: `.github/workflows/ci.yml` runs only `just ci` and `just integration-test`; no `cargo audit`/`cargo deny` step exists, and neither tool is installed locally. `Cargo.lock` is committed (pinned transitive versions across ~the whole tokio/postgres stack), so vulnerable-version detection is meaningful and cheap, but today a RustSec advisory against any pinned dependency goes unnoticed until someone happens to bump it.

Fix: add a CI step, e.g. `cargo install cargo-audit && cargo audit` (or the `rustsec/audit-check` action, or `cargo-deny` with an `advisories` check, which also covers license and duplicate-version policy). Run it on a schedule as well as on push, since advisories land independently of commits.

## Audited and found clean (with evidence)

- **SQL construction / injection.** Every dynamically built statement interpolates only: the table prefix, constant column lists (`JOB_COLUMNS`/`JOURNAL_COLUMNS`, `src/postgres/rows.rs:10,45`), constant clause fragments, and numeric parameter indices; all runtime values are bound as parameters (verified across every query in `src/postgres/mod.rs`, including the dynamically assembled `query_jobs`/`cleanup` WHERE builders at lines 552-656). The prefix is validated at the single construction choke point (`from_config` → `validate_prefix`, `src/postgres/mod.rs:76,795-827`: `^[a-z][a-z0-9_]*$`, ≤39 chars) and the struct fields are private, so no path constructs a store around the validation. The interpolated `LISTEN {channel}` (`src/postgres/notify.rs:152`) and refinery history-table name (`src/postgres/migrations.rs:65-68`) derive from the same validated prefix.
- **Unsafe code.** None in `src/` (grep verified). Panic containment uses safe `AssertUnwindSafe` with a documented soundness argument (`src/worker/registry.rs:181-190`).
- **Payload handling.** Payloads/carries/attachments are opaque `serde_json::Value`s; serde_json's recursion limit bounds parsing of hostile nesting, and the writers of the jobs table are within the trust boundary (they hold the same DB credentials). Undecodable rows surface as errors rather than panics (`src/postgres/rows.rs`). Variant naming is tracked separately in the corrupt-row todo listed under Relations.
- **Secret leakage.** venturi never logs the DSN or config; tracing sites log only error displays, kinds, counts, and durations (`src/observability.rs`, `tracing::warn!/error!` sites in `src/worker/mod.rs`, `src/postgres/notify.rs`). `PostgresStore` has no `Debug` impl that could dump connection parameters. One consumer-facing caveat worth documenting some day: handler error messages become persistent journal notes (`src/worker/mod.rs:842-847`), so secrets embedded in consumer error strings would be stored in the database and surfaced by history APIs. That is a consumer contract, not a venturi defect.
- **Resource-exhaustion angles** are covered by their own todos rather than repeated here (see Relations): unbounded journal loads, index-row-size failures from user-derived keys, cross-application advisory-lock blocking, and reconnect spin during outages. NOTIFY floods are bounded by the capacity-1 coalescing receiver (`src/postgres/notify.rs:124-129`); `find_stale` is batch-bounded (`LIMIT 100`).
- **TLS.** Plaintext `connect` vs `connect_rustls`/`from_config` is an explicit, documented consumer choice (`src/postgres/mod.rs:90-117`); the listener always follows the pool's TLS path, so there is no accidental plaintext side channel for notifications.

Source: security review, `.github/workflows/ci.yml`, `Cargo.toml`, `Cargo.lock`, full `src/` read.

## Relations

- Relates to: [Report corrupt rows as a data error, not a config error](01KWH1210VHTY9Y9B4W2JASZVJ-report-corrupt-rows-as-a-data-error-not-a-config-error.md), the variant naming for undecodable rows.
- Relates to: [`Context::history` is loaded unbounded per run](01KWH0RMBT948FF5XHYXEYTXDW-contexthistory-is-loaded-unbounded-per-run.md), the unbounded journal load.
- Relates to: [`DedupKey` accepts unbounded and empty strings; limits surface as opaque DB errors](01KWH0NRVC9WDV9GPMQ46CXATK-dedupkey-accepts-unbounded-and-empty-strings-limits-surface-as-opaque.md), the index-row-size failures from user-derived keys.
- Relates to: [Use a lock key namespace for the migration advisory lock](01KWH0YQCS9ET504JAA7TW4GZ7-use-a-lock-key-namespace-for-the-migration-advisory-lock.md), the cross-application advisory-lock blocking.
- Relates to: [Notifier connect and reconnect are not resilient at startup or during outages](01KTQ1BD21JAJ99XR9V1NDWYW9-notifier-connect-and-reconnect-are-not-resilient-at-startup-or-during.md), the reconnect spin during outages.
