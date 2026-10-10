---
title: "Keep merge churns a dead row version"
kind: improvement
component: postgres
impact: low
origin: review
tags: [performance]
---
# Keep merge churns a dead row version

Found in review. The Keep branch of `merge_into` writes a new row version on every Keep merge, only to test whether the candidate is still pending. A read-only check avoids that write.

## Problem

The Keep branch of `merge_into` issues `SET dedup_key = dedup_key` (`src/postgres/mod.rs`, lines 689-696) purely to test whether the candidate is still pending. That writes a new row version (plus WAL) on every Keep merge.

## Suggested fix

Use an `EXISTS` or `SELECT ... FOR UPDATE` inside the transaction to report pendingness without rewriting the row.

Impact is low unless Keep merges are very frequent.

Source: review finding, `src/postgres/mod.rs` lines 689-696.
