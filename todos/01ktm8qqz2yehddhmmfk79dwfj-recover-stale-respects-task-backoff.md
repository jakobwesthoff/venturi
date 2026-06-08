# Stale-claim recovery ignores a task's `backoff()` override

## Problem

`recover_stale()` (`src/worker/mod.rs`, ~lines 482-488) reschedules a recovered
job using `self.config.backoff` unconditionally. A task type that overrides
`Task::backoff()` gets the worker default for its recovery delay rather than its
own configured backoff.

The reason it currently uses the default: recovery does not deserialize the
payload, and reading the per-task override would require knowing the concrete
`Task` type.

## Options

1. Deserialize the payload in `recover_stale()` to read `Task::backoff()`
   (adds a deserialize on the recovery path, and needs the registry to map kind
   -> backoff).
2. Document the limitation explicitly: recovered stale claims always use the
   worker-default backoff. Currently this behavior is undocumented.

## Decision needed

Whether per-task backoff fidelity on recovery is worth the deserialize, or
documenting the limitation is sufficient.

Source: review finding, `src/worker/mod.rs:482-488`.
