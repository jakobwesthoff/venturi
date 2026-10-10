# Several ADRs contain claims the current code contradicts, with no superseding record

An ADR is a point-in-time record, so drift is only a defect when the ADR's
decision text is now wrong and nothing records the change. Sweep of all 26
ADRs against current `src/` (2026-07-02); six confirmed items. Before fixing,
decide the convention: amend the ADR in place with a dated amendment note, or
record changes in a new superseding ADR (ADR 1 governs the practice).

## 1. ADR 4 + ADR 6: the poll interval is not queue-client configuration

- `docs/adr/0004-...md:48-50`: the poll fallback interval "is configurable
  when the queue client is created (ADR 6)".
- `docs/adr/0006-...md:21-24`: client construction config includes "the poll
  fallback interval (ADR 4)" and "the name of the queue table".
- Code: the poll ceiling is a **worker** setting, `WorkerBuilder::poll_max`
  (`src/worker/mod.rs:195-198`, default 30s). Client construction is
  `PostgresStore::connect/connect_rustls/from_config` + `with_max_pool_size`
  (`src/postgres/mod.rs:64-133`); no poll setting exists there. Also "the
  name of the queue table" is in reality a table-name *prefix* naming all
  three tables; ADR 24:36-37 records the prefix but ADR 6's own text was
  never updated.

## 2. ADR 19: recovery cadence and cost are per loop iteration, not per claim

- `docs/adr/0019-...md:71-72`: "Recovery runs opportunistically at the start
  of every claim"; `:83-84`: "recovery adds one bounded, indexed query per
  claim".
- Code: `recover_stale()` runs once per **loop iteration**
  (`src/worker/mod.rs:339-341`), after which the fill loop performs up to
  `concurrency` claims; and recovery is a `find_stale` scan (LIMIT 100)
  plus one guarded UPDATE+INSERT transaction per stale job
  (`src/postgres/mod.rs:451-526`), not one query.
- ADR 20:43-44 ("Each iteration runs stale-claim recovery") matches the code
  but does not amend ADR 19. Note `docs/design/worker-model.md:189` repeats
  ADR 19's "at the start of every claim" phrasing and needs the same
  correction (companion to
  [[01kwh2f06vwn4v32pjt5mpk1bk-design-docs-drifted-from-shipped-api]]).

## 3. ADR 20: panic isolation mechanism is `catch_unwind`, not a join error

- `docs/adr/0020-...md:74-76`: panic isolation "relies on the runtime
  catching a panicking task at its boundary and surfacing it as a join
  error".
- Code: panics are caught inside the run future via
  `AssertUnwindSafe(..).catch_unwind()` and settled per `PanicPolicy`
  (`src/worker/registry.rs:181-242`); the `reap` join-error branch
  explicitly documents that handler panics do not reach it
  (`src/worker/mod.rs:668-677`). The outcome-level claim (a panic settles as
  a failed execution) still holds; the named mechanism does not. No ADR
  records `catch_unwind` or the `PanicPolicy` knob (including its `Dead`
  variant, `src/worker/mod.rs:89-96`); only `docs/design/worker-model.md`
  describes them.

## 4. ADR 22 (minor): "lowers the priority floor" inverts the mechanism

- `docs/adr/0022-...md:33-36` says the rotation "lowers the priority floor
  for a single claim so lower tiers receive guaranteed slots", filtered by
  `priority >= floor`.
- Code: with the 0=High encoding, a reserved claim **raises** the numeric
  floor (0 → 1 or 2) to *exclude* higher tiers (`src/worker/mod.rs:76-78,
  855-869`; `src/postgres/mod.rs:279`). Even read conceptually, "lowering a
  floor" describes admitting lower tiers alongside higher ones, while the
  implementation reserves by excluding higher tiers. One-word-level fix,
  but in the Decision text.

## 5. ADR 14 (minor): jitter is not reduced "modulo the jitter span"

- `docs/adr/0014-...md:29-31` (Decision prose): the mixed bits are reduced
  "modulo the jitter span".
- Code: no modulo; the top 53 bits form a unit sample in `[0,1)` that scales
  the delay into `[delay*(1-f), delay]` (`src/backoff.rs:82-96,130-138`).
  Same observable contract, different reduction step than the Decision
  states (the ADR's code snippet is marked illustrative; this sentence is
  not part of the snippet).

## 6. ADR 25 (minor): the snapshot has no per-(kind, status) breakdown

- `docs/adr/0025-...md:32-35`: "backlog depth and oldest-pending age per
  kind and status, the in-flight (claimed) count, and the dead count".
- Code `Snapshot` (`src/store.rs:536-546`): pending depth/age per kind
  (pending only), a dimensionless claimed count, dead per kind. No "per kind
  and status" shape exists.

Source: ADR sweep against current code, findings independently spot-verified
(quotes and code sites above). A seventh candidate — ADR 20's claim that the
documentation recommends matching an external resource pool and using a
blocking-aware spawn — was checked and rejected:
`docs/design/worker-model.md:100-102` contains exactly that guidance.
