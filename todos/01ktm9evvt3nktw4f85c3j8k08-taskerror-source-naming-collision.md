# `TaskError::source()` collides with `std::error::Error::source`

## Problem

`TaskError` (`src/outcome.rs:125-132`) deliberately does not implement
`std::error::Error`, yet exposes an inherent
`pub fn source(&self) -> Option<&(dyn Error + 'static)>`. The name shadows the
well-known trait method. A user reaching for `source()` to walk an error chain
finds this method, calls it, gets no compiler error, but it is not reachable
through trait dispatch and does not participate in `anyhow::Error::chain()` or
similar.

## Suggested fix

Rename the inherent method (e.g. `cause()` or `inner()`), or add a doc note
clarifying it is not `std::error::Error::source` and explaining why `TaskError`
does not implement the trait.

Source: review finding, `src/outcome.rs:125-132`.
