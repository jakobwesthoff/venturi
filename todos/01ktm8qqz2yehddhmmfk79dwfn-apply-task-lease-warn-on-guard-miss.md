# `apply_task_lease()` silently ignores an `extend_lease` guard miss

## Problem

`apply_task_lease()` (`src/worker/mod.rs`, ~lines 516-525) discards an
`Ok(false)` from `extend_lease`. `false` means the ownership guard missed: the
claim was lost between `claim_next` and the lease extension. With the default
15-minute lease this is effectively impossible, but a very short custom lease
makes it reachable, and the handler would then run against a claim it no longer
owns with no signal that anything went wrong.

## Suggested fix

Emit a `tracing::warn!` on the `false` branch so the condition is observable.
Consider whether the handler should be skipped entirely in that case rather than
run.

Source: review finding, `src/worker/mod.rs:516-525`.
