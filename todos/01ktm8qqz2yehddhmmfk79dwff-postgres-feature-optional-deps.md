# Make the `postgres` feature actually gate its dependencies

## Problem

The `postgres` feature is documented as the "default PostgreSQL storage
adapter", but it only gates `pub mod postgres` in `lib.rs`. The heavy
dependencies it implies are unconditional:

- `tokio-postgres`, `deadpool-postgres`, `refinery`, `refinery-core`, `time`
  are all non-optional in `Cargo.toml`.

Consequences:
- A consumer implementing a custom `Store` (the trait is storage-agnostic)
  still compiles the full PostgreSQL stack.
- `Error::Database`, `Error::Pool`, `Error::PoolBuild` live in the public
  error enum even when the adapter is excluded.

## Suggested fix

- Mark the postgres-only deps `optional = true` and add them to the
  `postgres` feature's dependency list.
- Gate the postgres-specific `Error` variants under
  `#[cfg(feature = "postgres")]`.

## Open question

The crate description is "PostgreSQL-backed job queue", so postgres-as-default
is intentional. Decide whether decoupling is worth the added `cfg` surface, or
whether the abstract `Store` trait is meant only for in-process test fakes (in
which case document that and close this).

Source: review finding, `Cargo.toml:13,20-21,28`, `src/error.rs:17-25`,
`src/lib.rs`.
