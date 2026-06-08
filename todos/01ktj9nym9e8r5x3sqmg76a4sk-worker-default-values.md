# Confirm chosen worker default values and exposure

**Status:** decided-as-interim; the plan left several concrete values "to pick at
implementation".

## Values chosen (all in `src/worker/mod.rs` / `src/backoff.rs`)

- **Failure backstop:** `DEFAULT_BACKSTOP = 20` failed executions before a job is
  forced to dead. The plan said "enabled by default at a high value"; 20 was
  picked. Configurable via `backstop(Option<u32>)`.
- **Backoff base/cap:** `Backoff::default()` = base `1s`, cap `5m`. The plan said
  "pick sensible defaults (e.g. base ~1s, cap a few minutes)". Configurable via
  `backoff(Backoff)` and per-task `Task::backoff`.
- **Proportional jitter fraction:** `DEFAULT_JITTER_FRACTION = 0.5`. The plan made
  this worker-level. **It is currently NOT exposed on the builder** — only the
  internal constant. Decide whether to add a `jitter_fraction(f64)` knob.
- **Priority ratio:** `DEFAULT_PRIORITY_RATIO = 4` (see
  [[priority-rotation-algorithm]]).
- **Concurrency:** `max(1, min(8, cores/2))` (from the plan, not invented).
- **poll_max `30s`, lease `15m`, shutdown_timeout `30s`** (from the plan/ADRs).

## To confirm / decide

- Is `backstop = 20` the right "high" value, or should it be much higher (e.g.
  hundreds) so it almost never trips before the task's own
  `TaskError::permanent`?
- Are base `1s` / cap `5m` the right defaults for the target workloads?
- Should the **jitter fraction be configurable** (builder method), or is a fixed
  `0.5` fine?
- Document all of these in the guide's configuration table once confirmed (the
  table currently lists them but not the jitter fraction, since it is not a knob).
