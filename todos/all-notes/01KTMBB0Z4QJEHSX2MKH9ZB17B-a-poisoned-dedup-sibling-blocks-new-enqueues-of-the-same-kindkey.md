---
title: "A poisoned dedup sibling blocks new enqueues of the same kind+key"
kind: bug
component: core
status: needs-discussion
origin: review
tags: [dedup]
---
# A poisoned dedup sibling blocks new enqueues of the same kind+key

Found in review. An existing pending dedup row that cannot be decoded into the current task types makes `Queue::submit` fail, so a new, valid task of the same kind and dedup key cannot be enqueued. The fix needs a decision on merge semantics before code changes.

## Problem

`Queue::submit` (`src/queue.rs`, lines 100-107) reconstructs the existing dedup candidate's typed payload and carry so it can call `task.merge(&pending)`:

```rust
let existing_payload: T = serde_json::from_value(candidate.payload.clone())?;
let existing_carry: T::Carry = if candidate.carry.is_null() {
    T::Carry::default()
} else {
    serde_json::from_value(candidate.carry.clone())?
};
```

If either deserialize fails, the `?` propagates and the whole `enqueue` fails.

Trigger: a pending row written under an older release whose `T`/`T::Carry` shape has since changed incompatibly (or whose stored JSON is otherwise undecodable into the current types). A producer enqueuing a brand-new, valid task of that kind+dedup_key then gets `Error::Serialization` and cannot enqueue at all, blocked by an unrelated poisoned sibling. The non-dedup `insert` path has no such coupling.

Source: review finding, `src/queue.rs` lines 100-107.

## Open questions

The merge decision needs the existing payload to construct `Pending<Self>` and call `task.merge(&pending)`. When the existing row cannot be deserialized, no merge decision can be computed. The alternatives each change semantics:

- **Fail (current):** surfaces the corruption, but denies all new enqueues of the key until the row is cleared.
- **Fall back to a fresh `insert`:** no work lost, but creates a sibling and, since `dedup_candidate` would keep returning the corrupt row, risks unbounded siblings.
- **Overwrite (Replace-like):** discards the corrupt row's state without consulting `merge`, which the task author never authorized.

Pick the intended behavior deliberately before changing code. A reasonable direction: skip undecodable candidates when selecting a merge target (so a corrupt row neither blocks nor accumulates siblings), but that needs maintainer sign-off.
