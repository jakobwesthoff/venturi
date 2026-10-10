---
title: "`Context::history` is loaded unbounded per run"
kind: bug
component: worker
status: needs-discussion
origin: review
---
# `Context::history` is loaded unbounded per run

Found in the 2026-07-02 review. Before each handler run, the worker loads the
whole journal for the job with no limit, so the cost of every run grows with the
number of earlier runs, whether or not the handler reads history. The seam is
`Worker::history_for` (`src/worker/mod.rs:519-529`) and `Store::journal`
(`src/postgres/mod.rs:424-432`).

## 1. The whole journal is loaded on every run, with no limit

Before each handler run, `history_for` calls `store.journal(id)`, which runs
`SELECT {columns} FROM {prefix}_journal WHERE job_id = $1 ORDER BY id` with no
`LIMIT`, and materializes every row, including every `attachment` jsonb, into
`Vec<JournalEntry>` for `Context::history`.

The journal is append-only and grows by one row per run (ADR 16). A job that
runs many times accumulates an ever-longer history, and the per-run cost
(query, transfer, jsonb deserialization, memory) grows linearly with prior
run count. Two realistic ways to get there:

- A pause-looping job: `Outcome::Pause` is the documented way to yield and
  become eligible again (`src/outcome.rs:25-32`), and pauses do not consume
  the failure backstop, so a long-lived recurring job can accumulate
  thousands of entries. Every one of its future runs pays for all of them.
- A high failure backstop combined with attachments: attachments are recorded
  for any outcome including failures (`src/context.rs:139-141`), so a
  retry-heavy job with per-run evidence drags all of it into memory each run.

The handler may never even call `ctx.history()`; the cost is paid regardless.

Options (decision needed): load history lazily (only when the handler asks),
cap the number of entries loaded per run (most-recent N, document the cap),
or exclude attachments from the default load and fetch them on demand.

Source: review finding, `src/worker/mod.rs:519-529`,
`src/postgres/mod.rs:424-432`, `src/context.rs:139-141`,
`src/outcome.rs:25-32`.
