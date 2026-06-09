# Unused direct dependency `refinery`

## Problem

`Cargo.toml` declares both `refinery` and `refinery-core`, but only
`refinery_core` is ever imported (`src/postgres/migrations.rs:18,19,63`,
`src/error.rs:29`). `refinery::` is never referenced anywhere in `src/`.

Verified: the crate compiles with `--all-features` after removing the `refinery`
line (the reviewer restored the manifest and lockfile afterward).

## Suggested fix

Drop the `refinery` dependency from `Cargo.toml`, keeping only `refinery-core`.
Re-run `cargo build --all-features` and `cargo test` to confirm. Update the
explanatory comment above the dependencies if it references `refinery`.

Source: review finding, `Cargo.toml`.
