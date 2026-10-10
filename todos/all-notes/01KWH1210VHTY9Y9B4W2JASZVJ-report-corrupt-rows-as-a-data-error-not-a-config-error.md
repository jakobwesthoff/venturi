---
title: "Report corrupt rows as a data error, not a config error"
kind: bug
component: postgres
status: needs-discussion
origin: review
tags: [api-design]
---
# Report corrupt rows as a data error, not a config error

Found in the 2026-07-02 review. The row-mapping layer reports undecodable stored data as `Error::Config`, so a consumer hitting a corrupt row is told that their configuration is wrong.

## Problem

The row-mapping layer reports undecodable stored data with the `Config`
error variant (`src/postgres/rows.rs:17,21,53,57`):

- a stored job `id` that is not a valid ULID
- a stored `status` outside the four known states
- a journal `job_id` that is not a valid ULID
- a journal `outcome` outside the known set

`Error::Config`'s contract is "A configuration value was invalid (for
example an empty table prefix)" (`src/error.rs:42-44`), and its `Display`
prefix is "invalid configuration: ...". A consumer that hits a corrupted or
manually edited row (or a row written by an incompatible future version)
therefore gets an error that claims their *configuration* is wrong, and a
consumer matching on variants to route recovery logic has no way to
distinguish "my setup is wrong" (fail fast at startup) from "one stored row
is bad" (skip/alert/repair). All legitimate uses of `Config` are in
`validate_prefix` (`src/postgres/mod.rs:797-823`); the rows.rs uses are the
only outliers.

## Fix (decision needed)

Add a dedicated variant for data-integrity failures, e.g.

```rust
/// A stored row could not be decoded into the crate's types.
#[error("stored data is corrupt: {0}")]
Corrupt(String),
```

The variant name is open (`Corrupt`, `Decode`, `Integrity`). Use it in
`job_from_row`/`journal_from_row`. `Error` is `#[non_exhaustive]`
(`src/error.rs:13`), so adding a variant is not a breaking change. The
`MalformedHistory` case in the migration bridge
(`src/postgres/migrations.rs:89-102`) already models this kind of failure
separately and can stay as is (it surfaces through the refinery error).

Source: review finding, `src/postgres/rows.rs:14-70`, `src/error.rs:42-44`.
