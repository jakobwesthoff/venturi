# 20. Run a bounded-concurrency claim and dispatch loop

Date: 2026-06-07

## Status

Accepted

Relates to [3. Claim jobs with `FOR UPDATE SKIP LOCKED`](0003-claim-jobs-with-for-update-skip-locked.md)

Relates to [4. Wake workers with LISTEN/NOTIFY and a polling fallback](0004-wake-workers-with-listen-notify-and-a-polling-fallback.md)

Relates to [11. Signal task outcome through Completed/Pause results, notes, and retryable-by-default errors](0011-signal-task-outcome-through-completed-pause-results-notes-and-retryable-by-default-errors.md)

Relates to [19. Recover stale claims by lease expiry](0019-recover-stale-claims-by-lease-expiry.md)

Relates to [21. Shut down gracefully by draining cooperatively, then releasing](0021-shut-down-gracefully-by-draining-cooperatively-then-releasing.md)

Relates to [23. Cap per-kind concurrency locally at claim time](0023-cap-per-kind-concurrency-locally-at-claim-time.md)

Relates to [24. Build the default storage adapter on tokio-postgres, deadpool, and refinery](0024-build-the-default-storage-adapter-on-tokio-postgres-deadpool-and-refinery.md)

## Context

A worker must turn claimable jobs into running handlers, bound how many run at
once, react promptly to new and newly-eligible work, and record each outcome. The
wakeup primitives (a notification channel plus a polling fallback, ADR 4), the
single-row claim (ADR 3), and stale-claim recovery (ADR 19) are already decided;
this records the loop that drives them and the concurrency model around it.

Worker concurrency is a property of the workload, not the host. Handlers that are
I/O-bound spend their time awaiting and can run many at once, bounded by
downstream and connection-pool capacity. Handlers that are CPU-bound, or that
block a thread, are bounded by the async runtime's worker-thread budget, beyond
which more concurrency starves the runtime. Handlers gated by an external resource
are bounded by that resource. The library cannot know which case applies, so any
default concurrency is a safe starting point to be tuned, not an optimum.

## Decision

**Bounded single loop.** A worker runs one claim-and-dispatch loop that maintains
an in-flight set of at most `N` concurrently running handlers. Each iteration runs
stale-claim recovery (ADR 19); then, while a slot is free, claims one job and
spawns it as a handler task, stopping when nothing is claimable. Each claim is
restricted to the worker's registered task kinds: the claim filter is exactly the
set of registered tasks, so a worker never claims a kind it cannot handle.
Claiming is one row per free slot rather than a batch, so each job's lease
(ADR 19) begins when its work begins. `N = 1` yields strictly serial processing.
Horizontal scale comes from running more worker processes, not from nesting loops.

**Default concurrency.** `N` defaults to `max(1, min(8, available_parallelism() /
2))` and is configurable. This default is a safety floor: it stays under the
runtime's worker-thread budget so that handlers which block a thread cannot starve
the runtime on a small host, and the cap keeps it modest on a large one. It errs
low deliberately, because too little concurrency costs visible throughput a user
can raise, whereas too much can silently overwhelm a downstream or the runtime.
The documentation states that I/O-bound workers should raise it, that it should
match the size of any external resource pool a handler depends on, and that
blocking work belongs in a blocking-aware spawn rather than a higher `N`.

**Waiting.** When the loop cannot make progress by claiming, it waits for the
soonest of: a handler finishing (which frees a slot), a notification on the
channel (new work enqueued), or a timeout. The timeout is `min(time until the next
future visible_at, poll_max)`, where the first term comes from a cheap indexed
lookup of the nearest not-yet-eligible job among the worker's kinds. This wakes
the loop exactly when a delayed job becomes eligible (a retry's backoff, a paused
job's resume, or a scheduled enqueue), since those produce no notification, while
`poll_max` bounds the wait when nothing is scheduled.

**Settlement.** The loop reaps each finished handler and performs settlement in
one place: a normal return settles by its `Outcome` (ADR 11), and a task that
ended in a panic settles as a failed execution. Both cases go through the same
single settlement path. Panic isolation relies on the runtime catching a panicking
task at its boundary and surfacing it as a join error rather than crashing the
process, which holds when the binary unwinds on panic. When the binary aborts on
panic instead, a handler panic terminates the process, which is the worker-death
case already covered by lease recovery (ADR 19). Correctness therefore never
depends on the consuming binary's panic configuration, only on how quickly and
gracefully a panic is absorbed.

## Consequences

A worker scales from strictly serial (`N = 1`) to bounded-parallel with one knob,
and out of the box it cannot starve the runtime even with naive blocking handlers,
at the cost of being conservative for I/O-bound work that should be tuned up.
Delayed and paused jobs are picked up close to their eligibility time without
tight polling. Settling on the loop serializes settlement; under a healthy
database its per-job cost is negligible, and if extreme throughput ever makes it a
bottleneck, settlement can be moved off the loop without changing the model, since
where settlement runs is an internal detail. Per-kind concurrency limits are not
part of this loop: a worker applies one limit across its registered kinds, and
isolating a kind is done by running a separate worker over a different registered
subset.
