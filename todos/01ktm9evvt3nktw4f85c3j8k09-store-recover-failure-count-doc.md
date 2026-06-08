# Document that `Store::recover`'s `failure_count` is the new value, not a delta

## Problem

`Store::recover` (`src/store.rs:388-399`) takes `failure_count: i32`. This is the
*new* value to store, not an increment. The current caller in `recover_stale`
passes the pre-incremented `next_failures`, which is correct, but the doc does
not state the contract. A future `Store` implementor (or an alternate caller)
could pass the current count and silently stall the failure counter.

## Suggested fix

Add to the doc comment: "`failure_count` is the value to write, not an
increment; the caller pre-computes `stored_failure_count + 1`."

Source: review finding, `src/store.rs:388-399`.
