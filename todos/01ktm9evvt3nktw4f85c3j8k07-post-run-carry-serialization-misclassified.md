# Carry serialization failure after a successful run is misclassified as dead

## Problem

In `erased_run` (`src/worker/registry.rs:192`), after a handler returns
`Ok(result)`, the carry is serialized with `serde_json::to_value(carry)?`. If
that `?` fires, the spawned task returns `Err(Error::Serialization(..))`. In
`settle()` (`src/worker/mod.rs:630-636`) that `Err` is handled by the
"job could not be dispatched; marking dead" branch.

So a job whose handler ran to completion, but whose resulting carry could not be
serialized, is:

- logged as an undispatchable job (misleading: it dispatched and ran), and
- sent to dead rather than handled as a run outcome.

In practice this is rare: `Carry: Serialize` is a trait bound and
`serde_json::to_value` fails only for exotic cases (non-finite floats, non-string
map keys). But when it happens the diagnosis is confusing and the terminal state
is surprising.

## Decision needed

What is the correct outcome for a post-run carry-serialization failure?

- Retrying re-runs a handler that already produced side effects, and the carry
  will fail to serialize again, so a plain retry is not obviously right.
- A distinct terminal classification ("ran but carry unencodable") with an
  accurate log may be the honest behavior.

This is a behavior decision for the maintainer; do not invent it. Separate the
post-run serialization path from the pre-run dispatch path in `settle()` once the
intended outcome is chosen.

Source: review finding, `src/worker/registry.rs:192`, `src/worker/mod.rs:630-636`.
