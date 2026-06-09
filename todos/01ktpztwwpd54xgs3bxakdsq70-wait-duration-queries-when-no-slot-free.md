# `wait_duration` queries even when no slot is free

## Problem

`Worker::run` calls `wait_duration` every loop spin, including spins where
`running.len() == concurrency` and no claim can happen regardless of the answer
(`src/worker/mod.rs` run loop → `wait_duration` → `next_visible_at`). That is one
extra `next_visible_at` query per settled job on a busy worker.

Same family as the `recover_stale` cadence todo, but a distinct query, so noted
separately.

## Suggested fix

Skip the `next_visible_at` probe when every slot is occupied (the wait then only
needs to resolve on a handler finishing, a notification, or shutdown).

Source: review finding R2, `src/worker/mod.rs` run loop / wait_duration.
