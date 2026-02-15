# Release Checklist

Related docs: [Operations](OPERATIONS.md) · [Architecture](ARCHITECTURE.md) · [Security](SECURITY_ENTERPRISE.md) · [Docs Index](index.md)

## Pre-release
- [ ] Update `CHANGELOG.md` for the release version.
- [ ] Verify `README.md` reflects current behavior and limitations.
- [ ] Run formatting: `cargo fmt --all -- --check`.
- [ ] Run linting: `cargo clippy --all-targets --all-features -- -D warnings`.
- [ ] Run tests: `cargo test`.
- [ ] Smoke benchmark build: `cargo bench --no-run`.

## Validation
- [ ] Single-node produce/fetch smoke test.
- [ ] Cluster failover smoke test (3 nodes).
- [ ] Kafka compatibility smoke test.

## Publication
- [ ] Tag release in git.
- [ ] Publish GitHub release notes from `CHANGELOG.md`.
- [ ] Attach benchmark summary and known limitations.
