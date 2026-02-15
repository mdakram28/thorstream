# Operations Guide

Related docs: [Architecture](ARCHITECTURE.md) · [Deployment TLS](DEPLOYMENT_TLS.md) · [Kubernetes](KUBERNETES.md) · [Release Checklist](RELEASE_CHECKLIST.md) · [Docs Index](README.md)

## Environment variables
- `THORSTREAM_ADDR` (default `0.0.0.0:9092`)
- `THORSTREAM_KAFKA_ADDR` (optional Kafka wire protocol listener)
- `THORSTREAM_KAFKA_PORT` (metadata broker port hint)
- `THORSTREAM_NODE_ID` (cluster node id)
- `THORSTREAM_CLUSTER_PEERS` format: `1=127.0.0.1:9101,2=127.0.0.1:9102`
- `THORSTREAM_COMPAT_API_ADDR` (optional Connect + Schema Registry compatibility API, e.g. `127.0.0.1:8083`)

## Single-node run
```bash
cargo run --bin thorstream
```

## 3-node local cluster example
Node 0:
```bash
THORSTREAM_NODE_ID=0 \
THORSTREAM_ADDR=127.0.0.1:9100 \
THORSTREAM_CLUSTER_PEERS="1=127.0.0.1:9101,2=127.0.0.1:9102" \
cargo run --bin thorstream
```
Node 1:
```bash
THORSTREAM_NODE_ID=1 \
THORSTREAM_ADDR=127.0.0.1:9101 \
THORSTREAM_CLUSTER_PEERS="0=127.0.0.1:9100,2=127.0.0.1:9102" \
cargo run --bin thorstream
```
Node 2:
```bash
THORSTREAM_NODE_ID=2 \
THORSTREAM_ADDR=127.0.0.1:9102 \
THORSTREAM_CLUSTER_PEERS="0=127.0.0.1:9100,1=127.0.0.1:9101" \
cargo run --bin thorstream
```

## Health checks
- Verify logs for leader transitions.
- Run integration tests and cluster tests before deploy:
  - `cargo test --test cluster_fault_tolerance_test`
  - `cargo test`

## Backup and restore
- Backup `data/` atomically (filesystem snapshot preferred).
- Restore by replacing `data/` and restarting nodes.

## Deployment
- For production reverse-proxy and TLS setup of compatibility APIs, see `docs/DEPLOYMENT_TLS.md`.
