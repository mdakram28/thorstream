# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
- Multi-node control-plane hooks with leader election and quorum replication path.
- Cluster fault-tolerance tests.
- Benchmark harness (`criterion`) for produce/fetch throughput.
- Publishing and governance docs (`CONTRIBUTING`, `SECURITY`, `CODE_OF_CONDUCT`, `LICENSE`).
- Kafka Connect-compatible HTTP API surface (`/connectors`, `/connector-plugins`, status/pause/resume).
- Schema Registry-compatible HTTP API surface (`/subjects`, `/schemas/ids`, config and compatibility endpoints).
- Embedded Streams shim module (`streams_shim`) for stateless filter/map/to pipelines.
- Object-store-backed segment mirroring and startup restore path.
- Kubernetes-first deployment manifests (Deployment, Service, PDB, HPA).
- Kubernetes operations guide for near-stateless autoscaled brokers.
- Advanced log semantics: retention (time/size), compaction, idempotent producer sequencing, transactional EOS scaffolding, and ISR/HW consistency behavior.

### Changed
- Leader-aware produce path for custom protocol and Kafka protocol handlers.
- Extended broker cluster metadata and runtime configuration.
