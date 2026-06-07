# 9. Define tasks as a trait dispatched through a type-erased registry

Date: 2026-06-07

## Status

Accepted

## Context

A consuming project has several kinds of work, each with its own payload shape
and handler. The straightforward modelling, one hand-written `JobKind` enum
matched in a central dispatcher, has two problems: nothing ties an enqueued
payload's shape to the handler that will run it, and adding a kind means editing
the enum and the match in lockstep. A reusable queue library also cannot own a
domain enum, because the set of kinds belongs to the consuming project, not the
library.

Payloads cross a JSON boundary in storage, so the storage layer only ever sees a
`kind` string and an opaque payload value. Type safety can only be recovered at
enqueue and at dispatch.

## Decision

A task is a type that implements a `Task` trait, implemented directly on the
payload struct. The trait carries a stable `const KIND` discriminator, an async
`handle` method, and the deduplication hooks (ADR 10). The struct being the
payload makes enqueue typed and lets the dedup hooks read the task's own fields.

Handlers receive their dependencies through a single shared state value: the
worker is generic over a consumer-defined state `S`, and `handle` is passed
`&S`. Tasks ignore the parts of `S` they do not use.

Tasks are registered against the worker by type. Internally this is a
type-erased registry keyed by the `KIND` string: each entry deserializes the
stored payload into the concrete task type and invokes its `handle` with `&S`.
Because every handler takes the same `&S`, the registry is homogeneous.

The return type of `handle` (how success, retryable failure, and permanent
failure are distinguished) is deferred to the failure-handling decision.

## Consequences

The `KIND` string, the payload type, and the handler are one unit the compiler
ties together; an enqueued task cannot reach a handler expecting a different
payload. Adding a kind is implementing a trait and registering the type, not
editing a central enum. The library owns no domain enum. Shared dependencies
live in `S` once rather than being repeated per task. The registry boundary is
where typed tasks meet the type-erased storage layer.
