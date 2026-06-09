# 26. Keep PostgreSQL as the only backend and remove the postgres feature

Date: 2026-06-10

## Status

Accepted

Amends [8. Isolate storage behind a backend trait](0008-isolate-storage-behind-a-backend-trait.md)

Amends [24. Build the default storage adapter on tokio-postgres, deadpool, and refinery](0024-build-the-default-storage-adapter-on-tokio-postgres-deadpool-and-refinery.md)

Relates to [7. Provide a layered architecture with independently usable layers](0007-provide-a-layered-architecture-with-independently-usable-layers.md)

## Context

Storage sits behind a backend trait (ADR 8), and the PostgreSQL adapter built on
tokio-postgres, deadpool, and refinery is the default and only implementation
(ADR 24). ADR 8 framed the trait as also letting "a test or alternative backend"
implement it without PostgreSQL, and ADR 24 noted a different database "could be
provided later as another adapter".

A `postgres` cargo feature existed, on by default, but it gated only the
`postgres` module; the adapter's dependencies (tokio-postgres, deadpool-postgres,
refinery-core, and the `time` crate) were declared unconditionally. Building with
`--no-default-features` therefore removed the module while still compiling its
entire dependency stack, producing a crate with no usable storage rather than a
slimmer one.

Making the feature meaningful would mean marking those dependencies optional
under the feature and gating the PostgreSQL-only `Error` variants (`Database`,
`Pool`, `PoolBuild`, `Migration`, `RunNumberOutOfRange`) behind
`#[cfg(feature = "postgres")]`, with the `rustls` feature made to imply
`postgres`. That is mechanically clean: outside the `postgres` module the driver
crates are referenced only in `lib.rs` and `error.rs`, and the `time` crate only
in the migration bridge. But it adds a permanent `cfg` surface and a
`--no-default-features` build lane to keep from bitrotting, and the only build it
enables is one that drops the adapter and supplies a custom `Store`. No such
consumer or alternative backend is in view.

## Decision

PostgreSQL is the only supported storage backend. The `Store` trait stays, but
its purpose is stated as keeping the upper layers (the queue handle, the worker
loop, the registry) free of any direct driver dependency and backing the crate's
in-memory test fake. It is not a supported extension point for alternative
production backends, and operations are added to it as the queue gains
capabilities without regard to external implementors.

The `postgres` cargo feature is removed. The PostgreSQL adapter and its
dependencies are always compiled. The `metrics` and `rustls` features remain as
independent opt-ins.

Decoupling the PostgreSQL stack behind the feature was considered and rejected:
the `cfg` and CI maintenance cost is permanent, while the only build
configuration it would enable is one nobody runs.

## Consequences

Removing the feature is a breaking change to the crate's feature surface: a
consumer that listed `features = ["postgres"]` must drop it. `--no-default-features`
now yields the same crate as the default build, since the adapter is
unconditional and only `metrics` and `rustls` remain opt-in. The PostgreSQL-only
`Error` variants are part of the public enum in every build.

The layered architecture (ADR 7) and the trait boundary (ADR 8) are unchanged:
nothing above storage names a driver or a SQL type. What changes is the stated
intent. The boundary exists for internal decoupling and testing, so the earlier
notes in ADR 8 and ADR 24 that an alternative production backend could be added
later no longer reflect the project's direction.
