---
title: "Add tests for the backoff override, history time filters and cleanup filters"
kind: chore
component: postgres
origin: review
tags: [testing]
---
# Add tests for the backoff override, history time filters and cleanup filters

Found in the 2026-07-02 review (test-suite audit). Three public behaviors have no test that would catch their breakage: the per-task `Task::backoff()` override, the `finished_since` and `finished_until` bounds of `HistoryFilter`, and the `kind` and `status` restrictions of `CleanupCriteria`.

The audit covered unit tests in `src/` and integration tests in `tests/`. Each numbered item below is a feature whose breakage no current test would catch.

## 1. The per-task `Task::backoff()` override is never exercised

`retry_or_backstop` schedules a retry with
`report.backoff.unwrap_or(self.config.backoff)` (`src/worker/mod.rs:802`),
where `report.backoff` is the task's own `Task::backoff()` captured after
the run (`src/worker/registry.rs:195`). No test, unit or integration,
registers a task that overrides `backoff()`. The closest test,
`jitter_fraction_zero_schedules_the_exact_backoff_delay`
(`src/worker/mod.rs:1668`), configures the **worker-level** backoff via the
builder, so the whole per-task override plumbing (capture in the registry,
precedence over the worker default) is unverified.

Test to add: a task returning a distinctive `Some(Backoff)` fails once with
jitter 0; assert the re-pended `visible_at` matches the task's curve, not
the worker default's.

(The sibling `Task::lease()` override has the same gap; that one is already
recorded in the todo on the lease-override path cleanup.)

## 2. `HistoryFilter::finished_since` / `finished_until` are untested

`tests/operations.rs` covers the `kind` and `status` filters;
`tests/pagination.rs` covers the cursor and limit. Nothing queries with the
two `finished_*` bounds (`src/store.rs:505-508`,
`src/postgres/mod.rs:570-577`). Their interplay with non-terminal rows
(`finished_at IS NULL` rows never match either bound) is exactly the kind
of predicate detail a regression would silently change.

## 3. `CleanupCriteria::kind` / `status` restrictions are untested

`tests/operations.rs` `cleanup_deletes_terminal_jobs_and_cascades_to_journal`
drives only the all-`None` criteria. The kind- and status-restricted delete
paths (`src/postgres/mod.rs:640-647`) have no test asserting that
non-matching terminal rows survive a filtered sweep.

Source: review finding (test-suite audit), `tests/*.rs`,
`src/worker/mod.rs:906-1716` test module.

## Relations

- Relates to: [Clean up the lease-override path (`extend_lease` / `apply_task_lease`)](01KTQ1BD21JAJ99XR9V1NDWYWC-clean-up-the-lease-override-path-extendlease-applytasklease.md), the same gap for the `Task::lease()` override.
