---
title: "`Context::history` is silently empty on a journal read failure"
kind: bug
component: worker
status: needs-discussion
origin: review
---
# `Context::history` is silently empty on a journal read failure

Found in the 2026-07-02 review. The journal-to-handler seam
(`Worker::history_for`, `src/worker/mod.rs:519-529`; `Store::journal`,
`src/postgres/mod.rs:424-432`) hides a failed read: the handler gets an empty
history and cannot tell it from a fresh job.

## Problem

`history_for` maps any store error to `Vec::new()` with only a worker-side
`tracing::warn!`. The handler-facing docs (`Context::history`,
`src/context.rs:120-124`) present history as "prior outcomes for this job"
with no mention that it can be empty because of a transient read failure. A
handler using history to make decisions (e.g. inspecting prior notes or
attachments to resume work) will silently act as if the job were fresh.

## Suggested fix (decision needed)

At minimum document the degraded mode on `Context::history`. Consider whether
failing the run (letting the claim settle as retryable) is the more honest
behavior than proceeding with wrong history. That is a behavior change and
needs a decision.

Source: review finding, `src/worker/mod.rs:519-529`,
`src/postgres/mod.rs:424-432`, `src/context.rs:120-141`.
