# 7. Provide a layered architecture with independently usable layers

Date: 2026-06-07

## Status

Accepted

## Context

venturi is meant to be dropped into projects with different needs. Some want a
batteries-included queue they configure and run; others need to drive part of
the machine themselves (their own worker loop, their own claim cadence) while
still reusing the durable storage operations.

## Decision

venturi is built in layers, and each layer is public and usable on its own:

- the storage operations (enqueue, claim, complete, fail, and so on),
- the worker loop that drives those operations,
- the task registry and dispatch on top of the loop.

The default, top-level entry point wires all layers together into a
batteries-included queue. A consumer who needs to can drop down a layer and
provide their own implementation of the part above it, building against the
layer below.

## Consequences

Each layer has a public, documented seam rather than being an internal detail of
the layer above it. The library cannot assume the consumer always uses the
top-level engine, so lower layers must be coherent and usable in isolation. This
constrains later API decisions: a layer may not reach past its neighbour.
