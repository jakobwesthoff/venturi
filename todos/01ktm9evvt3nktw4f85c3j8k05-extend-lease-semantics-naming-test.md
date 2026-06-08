# `extend_lease` is a set, not an extend: align name, doc, and tests

## Observations

`apply_task_lease()` (`src/worker/mod.rs:515-525`) calls `extend_lease` whenever
the task's lease differs from the worker default. `Store::extend_lease`
(`src/store.rs:401-410`, impl `src/postgres/mod.rs:469-485`) runs:

```sql
SET claim_expires_at = now() + interval '1 second' * $3
```

This unconditionally *sets* the expiry to the task's requested lease, which can
be earlier than the current expiry if the task returns a shorter lease.

This is not a correctness bug: the lease is the contract for when a claim is
considered abandoned, so a task that declares a shorter lease genuinely wants
earlier reclamation. But the API surface misrepresents this:

- The method is named `extend_lease`, implying monotonic extension.
- Its doc says "Returns whether the lease was extended." The bool actually
  reports whether the ownership guard (`claimed_by = $2 AND status = 'claimed'`)
  matched, not whether the expiry moved later.

## Suggested action

- Either rename to `set_lease`/`renew_lease`, or document clearly that it sets
  the expiry to `now() + lease` regardless of direction.
- Fix the doc comment to describe the bool as "whether the ownership guard
  matched and the expiry was rewritten."
- Decide whether `Task::lease()` shorter than the worker default is a supported
  input (the trait doc currently frames lease override as for *longer*-running
  tasks only). If only longer is supported, enforce or document it.

## Test gap

`apply_task_lease` and `extend_lease` have no test coverage. Add a test that a
task returning `Some(Duration)` from `lease()` rewrites `claim_expires_at` to the
expected value.

Source: review finding, `src/worker/mod.rs:515-525`, `src/store.rs:401-410`,
`src/postgres/mod.rs:469-485`.
