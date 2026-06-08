# Resolve a real hostname for `worker_identity()`

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

- Correctness is unaffected: the ownership guard compares both host and pid,
  and the pid disambiguates within a single host. This is purely a diagnostics
  fidelity issue.
- Fix options: pull the `gethostname` crate, or read the platform hostname
  directly. Adding a dependency for a diagnostics-only string is the trade-off
  to weigh.

Source: review finding, `src/worker/mod.rs:801-808`.
