# 18. Expose history query and cleanup APIs

Date: 2026-06-07

## Status

Accepted

## Context

Because all jobs and their journal entries are retained (ADR 5, ADR 16),
consumers need to read that history from outside the worker, and operators need to
reclaim the space it occupies. Leaving consumers to query the tables directly and
relegating purging to an out-of-band reconciler pass makes both second-class;
venturi makes history query and cleanup first-class library surface.

## Decision

- A **query API** reads the retained record: filter jobs by kind, status, and time
  window (for example, completed since a timestamp) returning job records, and
  fetch a single job's full journal timeline. The jobs table answers listings; the
  journal answers per-run detail.
- A **cleanup API** removes history in bulk by age and criteria (status, kind),
  index-efficiently. Cleanup is unified: removing a job removes its journal
  entries. Cleanup is explicit, never automatic.

Exact method signatures are left to the design document; this ADR fixes the
capabilities and that they are first-class library surface sitting behind the
backend trait (ADR 8).

## Consequences

Consumers can answer questions such as "completed jobs of kind K in the last 24
hours, with their state and detail" without writing bespoke SQL. Operators control
retention by age and criteria rather than the library imposing a fixed policy.
Because both sit behind the backend trait, an alternative backend implements the
same query and cleanup contract.
