---
title: "Resolve a real hostname for `worker_identity()`"
kind: improvement
component: worker
status: needs-discussion
origin: review
tags: [observability]
---
# Resolve a real hostname for `worker_identity()`

`worker_identity()` builds the `host:pid` identity from the `HOSTNAME` environment variable and falls back to `"unknown"`. Found in a review. The diagnostic field cannot tell workers apart when `HOSTNAME` is unset, so the host part should come from a real hostname lookup.

## Problem

`worker_identity()` (`src/worker/mod.rs:801-808`) builds the `host:pid`
diagnostic identity from the `HOSTNAME` environment variable, falling back to
`"unknown"`:

```rust
let host = std::env::var("HOSTNAME")
    .ok()
    .filter(|h| !h.is_empty())
    .unwrap_or_else(|| "unknown".to_owned());
```

`HOSTNAME` is commonly unset on macOS and in containers started without an
explicit `--env HOSTNAME`. Multiple workers then all advertise
`claimed_by = "unknown:<pid>"`, which makes the diagnostic field useless for
telling workers apart.

## Notes

- Correctness no longer depends on this string. The settlement ownership guard
  now matches on the claim epoch (`run_count`), not on `claimed_by`, so two
  workers colliding on `unknown:1` (the common minimal-container case) cannot
  double-settle each other's claims. This supersedes an earlier note here, which
  claimed the pid disambiguates the guard. That reasoning was wrong for
  same-process reclaim and is now moot. This is purely a diagnostics fidelity
  issue: `claimed_by` is the only human-readable "who holds this" field.
- Fix options: pull the `gethostname` crate, or read the platform hostname
  directly. Adding a dependency for a diagnostics-only string is the trade-off
  to weigh.

Source: review finding, `src/worker/mod.rs:826-832`. Related: claim-epoch guard
in `src/store.rs` `settle`/`extend_lease`.
