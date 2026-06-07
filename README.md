# venturi

> Controlled flow from backlog to worker.

**venturi** is a generic, durable job queue for Rust, backed by PostgreSQL.

A venturi is the narrowed section of a pipe that turns built-up pressure into
controlled, measurable flow. This library does the same for work: jobs
accumulate safely in your database and are released to workers at a rate you
control, with the durability and transactional guarantees Postgres already
gives you.

It is built to be shared across projects rather than reimplemented per
codebase. The goal is a single, well-tested queue you can drop into any Rust
service that needs reliable background job processing.

## Status

Early development. The design is still being worked out, so the public API and
feature set are not yet stable. This README will grow as the design solidifies.

## License

Licensed under the [MIT License](LICENSE).
