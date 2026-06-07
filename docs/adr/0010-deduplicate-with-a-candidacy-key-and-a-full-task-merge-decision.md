# 10. Deduplicate with a candidacy key and a full-task merge decision

Date: 2026-06-07

## Status

Accepted

## Context

Real workloads need to coalesce redundant work, but a bare last-write-wins key
is too weak: the decision often depends on the full content of the colliding
tasks, and once a job can pause and accumulate progress (ADR 11, ADR 15) it
depends on the existing job's carried state and history too. Deciding that against
every pending task of a kind would require scanning and deserialising the whole
pending set on each enqueue, which does not scale. An indexed `dedup_key` paired
with a separate merge step keeps candidate selection cheap, but a merge that sees
only the colliding payloads cannot make a decision informed by the existing job's
carried state and history.

## Decision

Deduplication is two layers on the `Task` trait (ADR 17):

1. `dedup_key()` returns an optional key. `None` means the task is never
   coalesced. Two pending tasks with the same `(KIND, key)` are collision
   candidates, found through an index.
2. `merge(&self, existing: &Pending<Self>)` is called only when a pending
   candidate exists. The candidate may be a paused, already-run job; it is treated
   like any other pending job. `Pending<Self>` gives merge the existing job's full
   state, so the decision is informed by content and history and can continue
   in-progress work:

```rust
struct Pending<T: Task> {
    payload: T,
    carry: T::Carry,
    run_count: u32,
    journal: Vec<JournalEntry>,
}

enum Merge<T: Task> {
    Keep,                              // incoming is redundant; existing untouched
    Replace,                           // existing payload <- incoming; carry reset to Default
    With { task: T, carry: T::Carry }, // existing <- computed payload + carry (continue the work)
    Independent,                       // not a duplicate; enqueue incoming as a new row
}
```

`Keep`, `Replace`, and `With` act on the existing row (same job id), so its
journal is preserved and the job stays trackable across the merge; each appends a
`merged` journal entry (ADR 16) recording the decision. `Independent` is a plain
new enqueue.

## Consequences

Candidate selection stays an indexed lookup, so enqueue cost does not grow with
backlog size, while the merge has the full content, carry, and history of both
colliding jobs. `With { task, carry }` expresses content-aware coalescing that
continues a paused job's work with a modified carry; `Replace` starts the
surviving job's payload fresh with a default carry; `Keep` and `Independent` cover
the trivial cases. Because merge reads and writes the typed carry, `Carry` is a
`Task` associated type (ADR 17). The candidacy key, the stored carry, and the
`merged` event are storage concerns settled with the schema and the journal.
