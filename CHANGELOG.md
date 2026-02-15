# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
- Multi-node control-plane hooks with leader election and quorum replication path.
- Cluster fault-tolerance tests.
- Benchmark harness (`criterion`) for produce/fetch throughput.
- Publishing and governance docs (`CONTRIBUTING`, `SECURITY`, `CODE_OF_CONDUCT`, `LICENSE`).

### Changed
- Leader-aware produce path for custom protocol and Kafka protocol handlers.
- Extended broker cluster metadata and runtime configuration.
