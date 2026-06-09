# `apply()` migration comment overstates the substitution

## Problem

The comment in `apply()` (`src/postgres/migrations.rs:50-58`) says the prefix is
substituted "into each migration's name and body" and that the name's prefix
keeps refinery's recorded identities distinct. The code substitutes only the
body (`sql.replace("{{prefix}}", prefix)`); the name passed to
`Migration::unapplied` is the literal `V1__initial` etc. Isolation actually comes
from the per-prefix history table (`set_migration_table_name`).

The comment is wrong; the code is correct.

## Suggested fix

Rewrite the comment to describe the body-only substitution and the history-table
isolation.

Source: review finding, `src/postgres/migrations.rs:50-58`.
