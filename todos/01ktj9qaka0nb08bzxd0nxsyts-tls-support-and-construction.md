# Revisit TLS support and adapter construction

**Status:** decided-as-interim during implementation; user thinks the restriction
may be too harsh and wants to reconsider.
**Related:** [[wakeup-notification-architecture]] (the listener is one TLS-bound
piece, but this is the broader concern).

## Current state

- **The pool is built by the caller.** `PostgresStore::new(pool, prefix)` takes an
  already-constructed `deadpool_postgres::Pool`, so the adapter is TLS-agnostic:
  the caller picks `NoTls` or a rustls `MakeRustlsConnect` when building the pool.
  This keeps the adapter from depending on a specific TLS stack.
- **The convenience constructor is `NoTls`-only.** `PostgresStore::connect(dsn,
  prefix)` builds the pool internally with `NoTls` (so consumers need no direct
  `deadpool`/`tokio_postgres` dependency for the common local case). There is no
  TLS equivalent — TLS users must drop down to `new` and build the pool
  themselves with the rustls connector.
- **The `LISTEN` listener is `NoTls`-only.** `PgNotifier` (`src/postgres/notify.rs`)
  does `tokio_postgres::connect(dsn, NoTls)`. A TLS deployment therefore gets no
  push wakeups unless it can point `with_listen` at a plaintext endpoint.

## Why it ended up this way

- `tokio-postgres-rustls` + `rustls` (aws-lc-rs backend, kept per user decision;
  `native-tls` was eliminated) are already dependencies, so TLS is *available*;
  it is just not wired into the convenience paths.
- A TLS connector is generic over `MakeTlsConnect`. Storing/erasing one in the
  non-generic `PostgresStore` (needed for both `connect`-with-TLS and the
  listener) is more work than the initial build took on, so it was deferred to
  keep `connect` and the listener simple.

## Things to reconsider

- **Is "caller builds the pool" the right primary API**, or should the adapter
  optionally own connection params + a connector so it can build the pool *and*
  the listener consistently (and expose them)? Owning params is what unblocks a
  TLS-capable listener cleanly.
- **A TLS-aware convenience constructor**, e.g. `connect_tls(dsn, prefix,
  connector)` or `connect_rustls(dsn, prefix, client_config)`, so TLS users get
  the same one-call ergonomics as `connect`.
- **First-class rustls support out of the box** (a helper that builds a sensible
  `MakeRustlsConnect` from system/webpki roots) vs. leaving the connector entirely
  to the caller.
- **Whether the listener should reuse the same TLS path** as the pool so a TLS
  deployment gets push wakeups without a second plaintext endpoint (ties into
  [[wakeup-notification-architecture]]).
- **Feature flags:** currently rustls/tokio-postgres-rustls are unconditional
  deps. If TLS becomes first-class, consider gating the TLS stack behind a feature
  so `NoTls`-only users don't pay for it.

## Acceptance criteria for the revisit

- TLS deployments have an ergonomic construction path (no manual deadpool
  plumbing required just to get TLS).
- Push wakeups are available under TLS (or a clear, documented reason they are
  not).
- The TLS dependency surface is intentional (feature-gated if appropriate).
