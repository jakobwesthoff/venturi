# Carry serialization failure after a successful run — RESOLVED

## Decision (accepted)

A post-run carry-serialization failure settles the job **dead**, which is the
accepted behavior: the handler ran, but its carry cannot be persisted and
re-running would only fail to serialize again, so dead (terminal, no retry) is
the honest outcome without inventing a new state. We do not introduce a separate
"ran but carry unencodable" lifecycle state.

## What changed

The defect was the *misreporting*, now fixed: such a run was previously routed
through the "job could not be dispatched; marking dead" branch with a generic
note. In `erased_run` (`src/worker/registry.rs`), a failure of
`serde_json::to_value(carry)` after a successful `Ok(result)` is now converted
into a permanent `TaskError` with the message "handler completed but its carry
could not be serialized: <detail>", settled dead through the normal outcome
routing. The pre-run decode failures still report genuine dispatch failures.

Regression test:
`worker::tests::completed_run_with_unencodable_carry_is_dead_with_an_accurate_note`.

## Future consideration (optional)

If a use case ever needs to distinguish "completed-but-carry-lost" from a real
permanent task failure (e.g. for alerting), a dedicated journal outcome could be
added. Not planned.

Source: review finding, `src/worker/registry.rs`, `src/worker/mod.rs` settle.
