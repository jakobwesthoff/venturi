# `DedupKey` accepts unbounded and empty strings; limits surface as opaque DB errors

## Problem

`DedupKey` (`src/task.rs:74-121`) is a thin wrapper over `String` with no
validation and `From` impls for arbitrary strings. The value lands in the
`dedup_key text` column (`migrations/V1__initial.sql:27`), which is covered by
the btree index `{{prefix}}_jobs_dedup (kind, dedup_key, created_at)`
(`migrations/V3__index_tuning.sql:44-46`).

Two consequences, neither documented on `DedupKey` or `Task::dedup_key`:

1. **Oversized keys fail at the index layer.** PostgreSQL rejects btree index
   entries larger than roughly one third of a page (~2704 bytes for the
   default 8 KiB page). `dedup_key` is the only *arbitrary user-supplied*
   string in an indexed column (`kind` is a code constant, `id` a ULID), so a
   task that derives its key from unbounded input (a URL, a user-provided
   name) makes `enqueue` fail with `Error::Database` wrapping
   "index row size ... exceeds btree version 4 maximum ...", far from the
   code that built the key.

2. **The empty string is a valid key, distinct from `None`.** `Some(DedupKey
   ::new(""))` coalesces all empty-keyed jobs of that kind with each other,
   while `None` opts out of coalescing. A task that builds its key from an
   optional field via `unwrap_or_default()` silently merges unrelated jobs.

## Suggested fix (decision needed)

Pick one or both:

- Document the constraints on `DedupKey`/`Task::dedup_key`: keys should be
  short (well under the ~2.7 KiB btree ceiling; hash long inputs), and the
  empty string is a real key, not "no key".
- Add validation: reject or hash oversized keys in `DedupKey::new`, and/or
  reject the empty string (or treat it as `None` at the enqueue boundary).
  Validation is a behavior change and needs a decision on the failure mode
  (panic, error variant, silent hash).

Source: review finding, `src/task.rs:74-121`, `migrations/V1__initial.sql:27`,
`migrations/V3__index_tuning.sql:44-46`.
