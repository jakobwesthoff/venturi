# venturi task runner.
#
# Database-backed integration tests are marked `#[ignore]`, so they are skipped
# by the fast `test` recipe and run explicitly via `integration-test` (they spin
# up an ephemeral PostgreSQL through Docker). `--all-features` keeps the optional
# `metrics` feature in the compile and test path.

alias lint := clippy
alias format := fmt

# Show the available recipes.
default:
    @just --list

# Format all code.
[group('format')]
fmt:
    cargo fmt --all

# Check formatting without modifying files.
[group('format')]
fmt-check:
    cargo fmt --all -- --check

# Type-check every target with all features.
[group('check')]
check:
    cargo check --all-targets --all-features

# Lint with clippy; treat warnings as errors.
[group('check')]
clippy:
    cargo clippy --all-targets --all-features -- -D warnings

# Run the fast tests (unit and in-process), skipping database-backed ones.
[group('test')]
test:
    cargo test --all-features

# Run only the database-backed integration tests (requires Docker).
[group('test')]
integration-test:
    cargo test --all-features -- --ignored

# Run every test, fast and database-backed (requires Docker).
[group('test')]
test-all:
    cargo test --all-features -- --include-ignored

# Build the library with all features.
[group('build')]
build:
    cargo build --all-features

# Build optimized with all features.
[group('build')]
build-release:
    cargo build --release --all-features

# Build the API documentation.
[group('build')]
doc:
    cargo doc --no-deps --all-features

# Remove build artifacts.
[group('build')]
clean:
    cargo clean

# Local gate: formatting, lints, type-check, and the fast tests.
ci: fmt-check clippy check test
