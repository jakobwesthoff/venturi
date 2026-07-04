# rustdoc: unresolved intra-doc link `crate::Store` in `error.rs`

`cargo doc --no-deps --all-features` (run 2026-07-02, otherwise clean)
emits one warning:

```
warning: unresolved link to `crate::Store`
  --> src/error.rs:48:18
```

`Error::InvalidPriority`'s doc references `[crate::Store]`, but `Store` is
not re-exported at the crate root (`src/lib.rs:33-39` re-exports `Queue`,
`Worker`, etc., not `Store`); the trait lives at `crate::store::Store`. The
link renders as dead text on docs.rs.

Fix: change the reference to `[crate::store::Store]` (or re-export `Store`
at the root, which is an API-surface decision — see
`todos/01ktj9nym9e8r5x3sqmg76a4sm-public-api-surface-review.md`).

Rest of the mechanical sweep for the record: `just ci` (fmt-check, clippy
`-D warnings`, check, fast tests) passes clean; no `TODO`/`FIXME`/`HACK`
markers in `src/`, `tests/`, `examples/`, or `migrations/`; no `unsafe`
code anywhere in `src/`; the crate contains zero doctests (API examples on
public items are part of the API-guidelines pass tracked in the todo above).

Source: review finding, `cargo doc` output, `src/error.rs:45-53`,
`src/lib.rs:33-39`.
