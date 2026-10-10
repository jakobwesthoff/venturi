---
title: "rustdoc: unresolved intra-doc link `crate::Store` in `error.rs`"
kind: chore
component: core
origin: review
---
# rustdoc: unresolved intra-doc link `crate::Store` in `error.rs`

Found in the 2026-07-02 review. `cargo doc --no-deps --all-features` (run 2026-07-02, otherwise clean) emits one warning:

```
warning: unresolved link to `crate::Store`
  --> src/error.rs:48:18
```

`Error::InvalidPriority`'s doc references `[crate::Store]`, but `Store` is not re-exported at the crate root (`src/lib.rs:33-39` re-exports `Queue`, `Worker`, etc., not `Store`). The trait lives at `crate::store::Store`. The link renders as dead text on docs.rs.

Fix: change the reference to `[crate::store::Store]` (or re-export `Store` at the root, which is an API-surface decision covered by the public API surface review in the Relations below).

Rest of the mechanical sweep for the record: `just ci` (fmt-check, clippy `-D warnings`, check, fast tests) passes clean; no `TODO`/`FIXME`/`HACK` markers in `src/`, `tests/`, `examples/`, or `migrations/`; no `unsafe` code anywhere in `src/`; the crate contains zero doctests (API examples on public items are part of the API-guidelines pass covered by the public API surface review).

Source: review finding, `cargo doc` output, `src/error.rs:45-53`, `src/lib.rs:33-39`.

## Relations

- Relates to: [Review the public API surface and stability before 1.0](01KTJ9NYM9E8R5X3SQMG76A4SM-review-the-public-api-surface-and-stability-before-10.md), where the re-export decision and the API-guidelines pass are tracked.
