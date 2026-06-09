# Panic-policy boundary covers only `Handler::handle`

## Problem

The `catch_unwind` in `erased_run` wraps only the `payload.handle(...)` call
(`src/worker/registry.rs:180-182`). User code that panics elsewhere on the run
path escapes it:

- `T::Carry::default()` (`registry.rs:165`)
- `payload.backoff()` (`registry.rs:187`)
- a panicking custom `Serialize` inside `serde_json::to_value(carry)`
  (`registry.rs:192`)

Such a panic kills the JoinSet task and lands in `reap`'s `Err(join_error)`
branch (`src/worker/mod.rs:620-629`) — whose comment asserts that handler panics
cannot reach it, which these paths now contradict. The job stays `claimed` until
lease recovery (default 15 min) instead of settling per `PanicPolicy`.
Self-healing via the backstop, but slow and policy-bypassing.

## Repro

Register a task whose `Carry`'s `Default` impl panics; the job stalls one full
lease per attempt rather than retrying promptly.

## Suggested fix (decision needed)

Either widen the `catch_unwind` to cover the whole run closure (carry decode,
handler, carry encode, backoff read) so every panic settles per `PanicPolicy`,
or accept the current narrow boundary and correct the `reap` comment to state
that a panic outside `handle` falls to lease recovery. The first changes
behavior; the second is doc-only.

Source: review finding, `src/worker/registry.rs:165-192`, `src/worker/mod.rs:620-629`.
