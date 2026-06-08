# Document that the merge-race fallback starts a fresh job (carry reset)

## Observation

In `apply_merge` (`src/queue.rs:168-193`), when `merge_into` returns `false`
because the candidate was claimed between `dedup_candidate()` and the merge, the
code falls back to `insert(incoming, ...)`. That inserts a brand-new job: it does
not inherit the now-claimed candidate's `run_count`, `failure_count`, or carry,
so a `Merge::With` that intended to *continue* the in-flight work instead starts
from `Default` carry.

This is intentional ("fall back to a fresh enqueue of the incoming task so no
work is lost") and no work is dropped, but a user relying on `With` to continue
the candidate's carry would be surprised by the reset in the race case.

## Suggested action

Extend `apply_merge`'s doc comment to state that the fallback enqueue is a fresh
job with default carry, not a continuation of the (now-claimed) candidate. Pure
documentation; no behavior change.

Source: review finding, `src/queue.rs:168-193`.
