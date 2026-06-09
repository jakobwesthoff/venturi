# `tokio` pulls `features = ["full"]` in a library

## Problem

`Cargo.toml` enables `tokio`'s `full` feature, forcing fs/net/process/signal/
io-std onto every consumer; the crate needs roughly `rt`, `sync`, `time`,
`macros`.

Fits the existing `postgres`-feature dependency-hygiene todo's theme, but is a
distinct dependency.

## Suggested fix

Trim `tokio` to the features actually used, verifying the build and tests still
compile (tests may need additional features such as `macros`/`rt-multi-thread`,
which can go under `dev-dependencies`).

Source: review finding R5, `Cargo.toml`.
