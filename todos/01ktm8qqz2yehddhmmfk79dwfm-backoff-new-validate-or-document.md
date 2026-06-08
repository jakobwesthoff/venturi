# `Backoff::new` silently accepts degenerate base/cap

## Problem

`Backoff::new` (`src/backoff.rs:30-33`) performs no validation:

- `Backoff::new(Duration::ZERO, cap)` makes every delay zero, i.e. immediate
  retries until the attempt backstop, a potential tight retry loop.
- `Backoff::new(base, cap)` with `cap < base` silently clamps every delay to
  `cap` (because each delay is `min(cap)`), inverting the caller's intent.

Neither case is documented or guarded.

## Suggested fix

Either document the edge-case behavior on `Backoff::new`, or add precondition
checks (at minimum `debug_assert!` for `base <= cap` and a note on
`base == 0`). Match whichever fits the crate's API-guideline stance on
constructor validation.

Source: review finding, `src/backoff.rs:30-33`.
