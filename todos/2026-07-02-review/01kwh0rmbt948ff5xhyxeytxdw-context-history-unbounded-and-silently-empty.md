# `Context::history` is loaded unbounded per run, and silently empty on a read failure

Two related weaknesses in the journal-to-handler seam
(`Worker::history_for`, `src/worker/mod.rs:519-529`; `Store::journal`,
`src/postgres/mod.rs:424-432`).

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

## 2. A failed journal read silently hands the handler an empty history

`history_for` maps any store error to `Vec::new()` with only a worker-side
`tracing::warn!`. The handler-facing docs (`Context::history`,
`src/context.rs:120-124`) present history as "prior outcomes for this job"
with no mention that it can be empty because of a transient read failure. A
handler using history to make decisions (e.g. inspecting prior notes or
attachments to resume work) will silently act as if the job were fresh.

Fix: at minimum document the degraded mode on `Context::history`. Consider
whether failing the run (letting the claim settle as retryable) is the more
honest behavior than proceeding with wrong history; that is a behavior
change and needs a decision.

Source: review finding, `src/worker/mod.rs:519-529`,
`src/postgres/mod.rs:424-432`, `src/context.rs:120-141`,
`src/outcome.rs:25-32`.
