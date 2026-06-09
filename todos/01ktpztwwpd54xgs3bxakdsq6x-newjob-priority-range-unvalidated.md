# `NewJob.priority` accepts out-of-range tiers from direct `Store` users

## Problem

`NewJob.priority: i16` (`src/store.rs`) lets a direct `Store` consumer pass a
value outside `0..=2`, learning of it only via the schema CHECK-constraint error
at enqueue. The `Queue` handle always passes valid values, so library users on
the intended path are unaffected.

## Suggested fix

Validate at the `Store::enqueue` boundary, or document the `0..=2` constraint on
`NewJob.priority`.

Source: review finding, `src/store.rs` `NewJob.priority`.
