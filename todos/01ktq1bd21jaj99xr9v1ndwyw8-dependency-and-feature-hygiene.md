# Dependency and feature hygiene (Cargo.toml)

One pass over the manifest's dependency surface. Three related cleanups that
would naturally be addressed together when touching `Cargo.toml`.

## 1. Remove the unused `refinery` direct dependency

`Cargo.toml` declares both `refinery` and `refinery-core`, but only
`refinery_core` is imported (`src/postgres/migrations.rs:18,19,63`,
`src/error.rs:29`); `refinery::` is referenced nowhere in `src/`. Verified the
crate compiles `--all-features` without it. Drop the `refinery` line.

## 2. Trim `tokio` to the features actually used

`tokio` is pulled with `features = ["full"]`, forcing fs/net/process/signal/
io-std onto every consumer; the library needs roughly `rt`, `sync`, `time`,
`macros`. Trim to those, moving any test-only features (e.g.
`rt-multi-thread`, `macros`) under `dev-dependencies` as needed.

## 3. Make the `postgres` feature actually gate its dependencies

The `postgres` feature only gates `pub mod postgres` in `lib.rs`; the heavy
deps it implies (`tokio-postgres`, `deadpool-postgres`, `refinery(-core)`,
`time`) are unconditional. A consumer implementing a custom `Store` still
compiles the full PostgreSQL stack, and `Error::Database`/`Pool`/`PoolBuild`
live in the public enum even when the adapter is excluded.

- Mark the postgres-only deps `optional = true` and list them under the
  `postgres` feature.
- Gate the postgres-specific `Error` variants under `#[cfg(feature = "postgres")]`
  (`src/error.rs:17-25`).

**Open question (item 3 only):** the crate is "PostgreSQL-backed", so
postgres-as-default is intentional. Decide whether decoupling is worth the added
`cfg` surface, or whether the abstract `Store` trait is meant only for
in-process test fakes (in which case document that and drop item 3). Items 1 and
2 are unconditional and independent of this decision.

Source: review findings, `Cargo.toml`, `src/error.rs:17-25`, `src/lib.rs`,
`src/postgres/migrations.rs`.
