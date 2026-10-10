---
title: "Declare the MSRV with `rust-version`"
kind: chore
component: project
origin: review
---
# Declare the MSRV with `rust-version`

Found in the 2026-07-02 review: `Cargo.toml` does not declare a minimum supported Rust version. The review's other packaging finding, internal files in the published crate, is fixed by the `exclude` list in `Cargo.toml`.

## Scope

The crate uses `edition = "2024"` (requires rustc ≥ 1.85) and `u64::is_multiple_of` (`src/worker/mod.rs:859-861`, stabilized in 1.87), so the de-facto MSRV is at least 1.87, but `Cargo.toml` does not declare `rust-version`. A consumer on an older toolchain gets an opaque compile error instead of cargo's clear "package requires rustc X" refusal, and MSRV bumps are invisible in diffs/changelogs. Declaring it is also on the Rust API guidelines checklist the project already uses for API review, covered by the public API surface review in the Relations below.

Fix: determine the real MSRV (try builds or `cargo msrv`), declare `rust-version = "..."`, and note the policy in the changelog on future bumps.

Source: review finding, `Cargo.toml`, `src/worker/mod.rs:859-861`.

## Relations

- Relates to: [Review the public API surface and stability before 1.0](01KTJ9NYM9E8R5X3SQMG76A4SM-review-the-public-api-surface-and-stability-before-10.md), the API-guidelines review that also covers the MSRV declaration.
