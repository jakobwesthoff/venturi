# Published package ships the `todos/` folder; no `rust-version` declared

Two packaging findings in `Cargo.toml`, verifiable with
`cargo package --list --allow-dirty`.

## 1. The crates.io tarball includes all internal todo notes

`Cargo.toml` has no `exclude`/`include`, so `cargo package` bundles every
tracked file. Verified on 2026-07-02: the package list contains all 36
`todos/**/*.md` files, including this review's findings. These are internal
working notes — open bugs, design doubts, deferred decisions — published
verbatim to crates.io with every release, and they also bloat the tarball.

Fix: add to `[package]` e.g.

```toml
exclude = ["/todos", "/.github", "/Justfile", "/.adr-dir"]
```

(decide the exact set; `migrations/` must stay in — the SQL is embedded via
`include_str!`, `src/postgres/migrations.rs:34-47` — and `docs/` is
referenced from the crate docs, so keep or drop it deliberately. An
`include` allowlist is the tighter alternative.)

## 2. No MSRV (`rust-version`) is declared

The crate uses `edition = "2024"` (requires rustc ≥ 1.85) and
`u64::is_multiple_of` (`src/worker/mod.rs:859-861`, stabilized in 1.87), so
the de-facto MSRV is at least 1.87, but `Cargo.toml` does not declare
`rust-version`. A consumer on an older toolchain gets an opaque compile
error instead of cargo's clear "package requires rustc X" refusal, and MSRV
bumps are invisible in diffs/changelogs. Declaring it is also on the Rust
API guidelines checklist the project already uses for API review
(`todos/01ktj9nym9e8r5x3sqmg76a4sm-public-api-surface-review.md`).

Fix: determine the real MSRV (try builds or `cargo msrv`), declare
`rust-version = "..."`, and note the policy in the changelog on future bumps.

Source: review finding, `Cargo.toml`, `cargo package --list` output
(2026-07-02), `src/postgres/migrations.rs:34-47`, `src/worker/mod.rs:859-861`.
