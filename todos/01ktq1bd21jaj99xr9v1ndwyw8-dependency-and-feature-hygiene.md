# Make the `postgres` feature actually gate its dependencies

The `postgres` feature only gates `pub mod postgres` in `lib.rs`; the heavy deps
it implies (`tokio-postgres`, `deadpool-postgres`, `refinery-core`, `time`) are
unconditional. A consumer implementing a custom `Store` still compiles the full
PostgreSQL stack, and `Error::Database`/`Pool`/`PoolBuild` live in the public
enum even when the adapter is excluded.

- Mark the postgres-only deps `optional = true` and list them under the
  `postgres` feature.
- Gate the postgres-specific `Error` variants under `#[cfg(feature = "postgres")]`
  (`src/error.rs:17-25`).

**Open question:** the crate is "PostgreSQL-backed", so postgres-as-default is
intentional. Decide whether decoupling is worth the added `cfg` surface, or
whether the abstract `Store` trait is meant only for in-process test fakes (in
which case document that and drop this item).

Source: review findings, `Cargo.toml`, `src/error.rs:17-25`, `src/lib.rs`.

## Done (split out)

Items 1 (drop the unused direct `refinery` dependency) and 2 (trim `tokio` to
the features actually used) of the original finding are resolved; only the
feature-gating decision above remains.
