# Front-door docs are stale: removed `postgres` feature still advertised, version pins at 0.1

One doc-fix change over the three entry-point documents (crate docs, README,
guide). All findings verified against `Cargo.toml` (version `0.4.0`, features
`metrics` and `rustls` only, `Cargo.toml:3,41-47`) and ADR 26
(`docs/adr/0026-keep-postgresql-as-the-only-backend-and-remove-the-postgres-feature.md`;
feature removed in commit `92ad42e`).

## 1. Crate docs claim the adapter is behind a `postgres` feature

`src/lib.rs:10-12`:

> The default storage adapter (behind the `postgres` feature) is built on
> `tokio-postgres`, `deadpool`, and `refinery`.

This is the front page of docs.rs. A user following it would try
`features = ["postgres"]` and get a cargo error, or wrongly conclude the
adapter is optional. Reword to state the adapter is always compiled.

## 2. README repeats the removed-feature claim

`README.md:47-48`:

> The default `postgres` feature enables the PostgreSQL adapter.

Same fix: the adapter is always compiled; only `metrics` and `rustls` are
features.

## 3. README and guide pin `venturi = "0.1"`

The crate is at `0.4.0` with several breaking changes since 0.1 (run-number
types, `TaskError::cause`, removed feature — see CHANGELOG). Stale pins:

- `README.md:42` — `venturi = "0.1"`
- `docs/guide.md:54` — `venturi = "0.1"`
- `docs/guide.md:458` — `venturi = { version = "0.1", features = ["metrics"] }`

A new user copying the snippet gets 0.1 semantics and API, then hits breaking
differences against current docs. Either bump the pins on each release
(consider a release-checklist note or just a `"0"`-major-style policy
decision) or pin the current minor.

Note the CHANGELOG's 0.1.0 entry mentioning the `postgres` feature
(`CHANGELOG.md:180`) is historically accurate and must stay as is.

Source: review finding, `src/lib.rs:10-12`, `README.md:38-49`,
`docs/guide.md:52-58,452-461`, `Cargo.toml:3,41-47`, ADR 26.
