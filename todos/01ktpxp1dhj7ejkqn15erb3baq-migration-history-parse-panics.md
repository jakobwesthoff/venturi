# Malformed refinery history rows panic instead of erroring

## Problem

`PgMigrationClient::query` parses refinery's history rows with `expect()`
(`src/postgres/migrations.rs:121-126`):

```rust
let applied_on = OffsetDateTime::parse(&applied_on, &Rfc3339)
    .expect("refinery records applied_on in RFC 3339");
...
let checksum = checksum
    .parse::<u64>()
    .expect("refinery records checksum as a decimal u64");
```

A history row corrupted by a manual edit, a partial restore, or a future
refinery format change panics the migration runner instead of surfacing a
recoverable error. Everywhere else malformed DB content maps to `Error::Config`
(`src/postgres/rows.rs:16-21`), so this is inconsistent with the crate's own
error-handling convention.

## Suggested fix

Map both parse failures to `Self::Error` (the `tokio_postgres::Error` type the
trait returns) or a wrapped error, rather than `expect`. The trait's associated
`Error` type constrains the options; check whether refinery's `AsyncQuery`
error channel can carry a non-`PgError` variant, otherwise convert.

Source: review finding, `src/postgres/migrations.rs:121-126`.
