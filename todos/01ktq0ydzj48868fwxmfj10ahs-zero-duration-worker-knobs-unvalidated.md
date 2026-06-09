# Unvalidated zero-duration worker knobs

## Problem

Same footgun family as the `with_max_pool_size(0)` / `register_capped(0)` todos,
different knobs:

- `poll_max(Duration::ZERO)` (`src/worker/mod.rs:157-160`) makes `wait_duration`
  return zero, busy-spinning the loop with `find_stale` + claim +
  `next_visible_at` queries every iteration.
- `lease(Duration::ZERO)` (`src/worker/mod.rs:164-167`) stamps an
  already-expired lease at claim time, so any worker's `recover_stale` re-pends
  the job mid-run, guaranteeing duplicate execution on every run.

## Suggested fix

Clamp both to a sane minimum (or reject zero with `Error::Config`), and document
the floor. `lease(0)` in particular is a correctness footgun, not just an
efficiency one.

Source: review finding, `src/worker/mod.rs:157-167`.
