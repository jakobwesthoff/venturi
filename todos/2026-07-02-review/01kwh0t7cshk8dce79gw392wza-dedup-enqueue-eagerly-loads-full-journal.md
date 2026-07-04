# Dedup-colliding enqueue eagerly loads the candidate's full journal

## Problem

When an enqueue collides with a pending sibling, `Queue::submit`
(`src/queue.rs:134-146`) loads the candidate's **entire** journal to build the
`Pending<T>` view before calling `task.merge(&pending)`:

```rust
let journal = self
    .store
    .journal(candidate.id)      // SELECT ... WHERE job_id = $1 ORDER BY id  (no LIMIT)
    .await?
    .into_iter()
    .map(JournalEntry::from_record)
    .collect();
```

This cost is paid on **every** colliding enqueue even when the merge decision
never reads it. The default `Task::merge` is `Merge::Replace` and ignores
`existing` completely (`src/task.rs:238-241`), so the common case (a task
that sets `dedup_key` but keeps the default merge) pays for a full journal
fetch, including every `attachment` jsonb, and throws it away.

Because the journal grows by one row per run and per merge, a hot dedup key
(exactly the kind of job dedup exists for, e.g. a repeatedly re-enqueued
refresh task) makes producer-side enqueue latency grow with the age of the
surviving job. Producers, unlike workers, are typically on a request path.

The payload/carry deserialization on the same lines has the same
always-eager shape but is bounded in size; the journal is the unbounded part.

## Constraint that shapes the fix

`Task::merge` is a synchronous method (`src/task.rs:238`), so `Pending`
cannot lazily await the journal inside `merge`. Options:

- Load the journal only for tasks that declare they need it (an opt-in, e.g.
  a `const MERGE_READS_JOURNAL: bool` or a builder-style knob on the task).
- Cap the loaded history (most-recent N entries) and document the cap on
  `Pending::journal`.
- Drop the journal from `Pending` entirely (breaking change; check whether
  any consumer actually uses `Pending::journal`).

## Related

- [[01kwh0rmbt948ff5xhyxeytxdw-context-history-unbounded-and-silently-empty]]
  covers the same unbounded `journal(id)` load on the worker/run side; a fix
  that caps or lazies the store read could serve both call sites.

Source: review finding, `src/queue.rs:134-146`, `src/task.rs:238-241`,
`src/postgres/mod.rs:424-432`.
