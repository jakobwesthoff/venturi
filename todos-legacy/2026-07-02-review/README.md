# Full codebase review, 2026-07-02

Continuation of the review whose earlier findings live as flat files in
`todos/` (see the `01kt*`/`01kv*` files there). Findings from this pass are
written incrementally into this folder as `ulid-short-description.md`, one
self-sufficient todo per finding or coherent finding group. Existing flat
todos are cross-referenced, not duplicated.

## Operations and status

| #  | Operation                                                                  | Status  |
|----|----------------------------------------------------------------------------|---------|
| 01 | `src/lib.rs`, `src/error.rs`, `src/observability.rs`                       | done    |
| 02 | `src/task.rs` (Task trait, TaskError, Merge)                               | done    |
| 03 | `src/backoff.rs`                                                           | done (no findings) |
| 04 | `src/context.rs`, `src/outcome.rs`                                         | done    |
| 05 | `src/queue.rs`                                                             | done    |
| 06 | `src/store.rs` (Store trait + value types)                                 | done    |
| 07 | `src/postgres/mod.rs` part 1: construction, migrate, enqueue, claim        | done (no new findings) |
| 08 | `src/postgres/mod.rs` part 2: settle, recover, merge_into, stats, queries  | done    |
| 09 | `src/postgres/{notify,rows,migrations}.rs` + `migrations/*.sql`            | done    |
| 10 | `src/worker/mod.rs` part 1: config/builder, run loop, claim/dispatch       | done    |
| 11 | `src/worker/mod.rs` part 2: reap/settle/release, recovery, lease, priority | done    |
| 12 | `src/worker/registry.rs`                                                   | done    |
| 13 | `src/test_support.rs` (FakeStore fidelity)                                 | done    |
| 14 | `tests/` suite: coverage gaps, `tests/common` harness                      | done    |
| 15 | `examples/`, README, `docs/guide.md`, CHANGELOG accuracy                   | done    |
| 16 | `docs/adr/`, `docs/design/` consistency with the code                      | done    |
| 17 | `Cargo.toml`, features, `Justfile`, packaging metadata                     | done    |
| 18 | Mechanical sweep: clippy, `cargo doc` warnings, TODO/FIXME scan            | done    |
| 19 | Security review (deliberately last): SQL construction, payload handling,   | done    |
|    | DoS vectors, dependency audit, information leakage in logs/errors          |         |

Update the status column as operations complete so a future session can
resume from the first non-`done` row.

## Review complete — findings index (2026-07-02)

All 19 operations are done. Eighteen findings were filed in this folder
(chronological ULID order = review order, security last), and three existing
flat todos were updated in place rather than duplicated:
`01ktm8qqz2yehddhmmfk79dwfh-stats-snapshot-non-atomic.md` (corrected the
mechanism description), `01ktq1bd21jaj99xr9v1ndwywd-fakestore-vs-postgres-semantic-parity.md`
(added the `find_stale` bound/ordering divergence as item 3).

Correctness / behavior:
- [[01kwh0nrvc9wdv9gpmq46cxatk-dedup-key-unbounded-and-empty-values]]
- [[01kwh0rmbt948ff5xhyxeytxdw-context-history-unbounded-and-silently-empty]]
- [[01kwh0yqcs9et504jaa7tw4gz6-stats-counts-scheduled-jobs-as-backlog]]
- [[01kwh0yqcs9et504jaa7tw4gz7-advisory-lock-shares-global-int8-keyspace]]
- [[01kwh151cbqddm2kxcszgv6x53-worker-run-silently-exits-with-no-registered-kinds]]
- [[01kwh17310ftn8swdweyw1rsmz-silent-kind-collision-on-register]]

Efficiency:
- [[01kwh0t7cshk8dce79gw392wza-dedup-enqueue-eagerly-loads-full-journal]]
- [[01kwh17310ftn8swdweyw1rsmy-payload-deserialized-twice-per-claim]]

API surface / errors:
- [[01kwh0vpfzzyxrspw9evqezytd-failure-count-i32-vs-run-count-u32]]
- [[01kwh1210vhty9y9b4w2jaszvj-corrupt-row-errors-misreported-as-config]]

Documentation:
- [[01kwh0ky8gt824c69sh80dxt1j-front-door-docs-stale-feature-and-version]]
- [[01kwh151cbqddm2kxcszgv6x54-producer-clock-skew-undocumented-on-queue]]
- [[01kwh2f06vwn4v32pjt5mpk1bk-design-docs-drifted-from-shipped-api]]
- [[01kwh32fk3p6gmatg0svg5wwh6-adr-text-contradicts-current-code]]
- [[01kwh2qbrymfxjqq9rb46eppwn-rustdoc-unresolved-link-crate-store]]

Tests / packaging:
- [[01kwh29asa2sht2mn6b5x3vggb-test-gaps-backoff-override-and-filter-params]]
- [[01kwh2jx9nh9ze86spd5091x4r-package-ships-todos-and-lacks-msrv]]

Security (last):
- [[01kwh2rv6zm26mbpkhyh8nt7n3-security-review-add-dependency-audit-to-ci]]

Areas reviewed with no new findings beyond existing todos: `src/backoff.rs`,
`src/error.rs`, `src/observability.rs`, `postgres/mod.rs` part 1 (construction,
migrate, enqueue, claim), `postgres/notify.rs`, `postgres/migrations.rs`,
`examples/`, CHANGELOG, `docs/design/schema.md`, `docs/design/indexes.md`,
Justfile/CI recipes, and the mechanical sweep (`just ci` clean, no
TODO/FIXME markers, no `unsafe`).
