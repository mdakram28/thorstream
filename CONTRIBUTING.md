# Contributing to Thorstream

## Development setup
- Install Rust stable (`rustup default stable`)
- Clone repo and run:
  - `cargo test`
  - `cargo fmt --all`
  - `cargo clippy --all-targets --all-features -- -D warnings`

## Pull request checklist
- Keep changes focused and atomic.
- Add or update tests for behavior changes.
- Update docs (`README.md` and architecture/ops docs) when behavior or APIs change.
- Ensure all checks pass locally before opening PR.

## Commit guidance
- Use descriptive commit messages.
- Mention user-facing behavior changes clearly.

## Reporting bugs
Please include:
- Reproduction steps
- Expected vs actual behavior
- Rust version (`rustc --version`)
- Platform info
- Relevant logs
