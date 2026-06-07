# 14. Source retry jitter deterministically from the job identifier

Date: 2026-06-07

## Status

Accepted

## Context

Retry jitter (ADR 12) needs a cheap, uniformly distributed value in a bounded
range to decorrelate retries across a batch of jobs that failed together. Rust's
`rand` crate and its dependency tree (`rand_core`, `rand_chacha`, `getrandom`,
`ppv-lite86`) are far more than this needs, and pull cryptographic-grade
machinery and platform entropy access into a library that wants neither for a
non-cryptographic purpose.

## Decision

The jitter offset is a pure function of the job's ULID and its attempt number,
with no random-number generator and no crate dependency.

The 128-bit ULID already carries 80 bits of randomness (ADR 2), so distinct jobs
already have distinct, well-spread identifiers. The function mixes the identifier
with the attempt through a SplitMix64-style finalizer — a short sequence of
wrapping multiplications and xor-shifts — and reduces the result modulo the
jitter span. Illustratively:

```rust
fn jitter_offset(id: u128, attempt: u32, span: u64) -> u64 {
    // Fold the 128-bit id into 64 bits and stir in the attempt so successive
    // retries of the same job land on different offsets.
    let mut z = (id as u64) ^ ((id >> 64) as u64) ^ (attempt as u64).wrapping_mul(GOLDEN);
    // SplitMix64 finalizer: avalanches every input bit across the whole word.
    z = (z ^ (z >> 30)).wrapping_mul(M1);
    z = (z ^ (z >> 27)).wrapping_mul(M2);
    z ^= z >> 31;
    z % span
}
```

How it works: the finalizer avalanches the input bits, so nearby identifiers and
consecutive attempts produce outputs uncorrelated across the whole range; the
modulo maps that into the jitter span. Decorrelation of a failed batch comes from
the jobs' identifiers differing, not from a seeded generator. Including the
attempt means the same job draws a different offset on each successive retry.

## Consequences

No random-number dependency. The jitter is deterministic: a given
`(id, attempt)` always yields the same offset, which makes the backoff schedule
reproducible and trivial to test. The offset feeds `visible_at` exactly as in
ADR 12. The implementation site documents the finalizer constants and the
bit-mixing so the choice is legible to future readers.
