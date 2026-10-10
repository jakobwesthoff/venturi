# Clean up the lease-override path (`extend_lease` / `apply_task_lease`)

One change request over the two functions that implement per-task lease
overrides: `apply_task_lease()` (`src/worker/mod.rs:515-525`) and
`Store::extend_lease` (`src/store.rs`, impl `src/postgres/mod.rs`). Two related
issues, naturally fixed together with one test.

## 1. `extend_lease` is a set, not an extend (name/doc/contract)

`extend_lease` runs `SET claim_expires_at = now() + interval '1 second' * $n`,
which unconditionally *sets* the expiry — it can move *earlier* if a task returns
a shorter lease than the current expiry. The name implies monotonic extension,
and the doc says "Returns whether the lease was extended" when the bool actually
reports whether the ownership guard matched (claimed_by + epoch + status), not
whether the expiry moved later.

- Either rename (`set_lease`/`renew_lease`) or document that it sets the expiry to
  `now() + lease` regardless of direction.
- Fix the bool's doc to "whether the ownership guard matched and the expiry was
  rewritten".
- Decide whether a `Task::lease()` shorter than the worker default is supported
  (the trait doc currently frames overrides as for longer-running tasks). If only
  longer is supported, enforce or document it.

## 2. `apply_task_lease` silently ignores a guard miss

`apply_task_lease` discards an `Ok(false)` from `extend_lease`. `false` means the
ownership guard missed — the claim was lost between `claim_next` and the lease
extension. Effectively impossible at the default 15-minute lease, but reachable
with a very short custom lease, after which the handler runs against a claim it
no longer owns with no signal. Emit a `tracing::warn!` on the `false` branch, and
decide whether the handler should be skipped rather than run.

## Test gap (shared)

`apply_task_lease` and `extend_lease` have no test coverage. Add a test that a
task returning `Some(Duration)` from `lease()` rewrites `claim_expires_at` to the
expected value, and one exercising the guard-miss branch.

Source: review findings, `src/worker/mod.rs:515-525`, `src/store.rs`,
`src/postgres/mod.rs` `extend_lease`.
