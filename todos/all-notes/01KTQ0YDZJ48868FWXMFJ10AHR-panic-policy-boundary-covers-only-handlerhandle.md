---
title: "Panic-policy boundary covers only `Handler::handle`"
kind: bug
component: worker
status: needs-discussion
origin: review
---
# Panic-policy boundary covers only `Handler::handle`

Found in review. The `catch_unwind` in `erased_run` wraps only the handler call, so a panic in carry default, backoff or carry serialization escapes `PanicPolicy` and waits for lease recovery. Whether to widen the boundary or fix the comment is a decision for the maintainers.

## Problem

The `catch_unwind` in `erased_run` wraps only the `payload.handle(...)` call (`src/worker/registry.rs`, lines 180-182). User code that panics elsewhere on the run path escapes it:

- `T::Carry::default()` (`registry.rs`, line 165)
- `payload.backoff()` (`registry.rs`, line 187)
- a panicking custom `Serialize` inside `serde_json::to_value(carry)` (`registry.rs`, line 192)

Such a panic kills the JoinSet task and lands in `reap`'s `Err(join_error)` branch (`src/worker/mod.rs`, lines 620-629). That branch's comment asserts that handler panics cannot reach it, which these paths now contradict. The job stays `claimed` until lease recovery (default 15 minutes) instead of settling per `PanicPolicy`. The backstop heals it, but slowly and around the policy.

## Repro

Register a task whose `Carry`'s `Default` impl panics. The job stalls one full lease per attempt rather than retrying promptly.

## Suggested fix

Either:

- widen the `catch_unwind` to cover the whole run closure (carry decode, handler, carry encode, backoff read) so every panic settles per `PanicPolicy`, or
- accept the current narrow boundary and correct the `reap` comment to state that a panic outside `handle` falls to lease recovery.

The first changes behavior. The second is doc-only.

Source: review findings, `src/worker/registry.rs` lines 165-192, `src/worker/mod.rs` lines 620-629.
