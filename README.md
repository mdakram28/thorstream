# Thorstream

Thorstream is a Rust event streaming broker with Kafka-like APIs, append-only durable storage, and a lightweight multi-node replication/control plane.

## Highlights

- Append-only segmented storage with startup recovery from disk.
- Topics, partitions, offsets, consumer groups, commit/seek/poll semantics.
- TCP custom protocol server and Kafka wire protocol endpoint.
- Leader-aware produce path with peer replication and quorum acknowledgement.
- Deterministic heartbeat-based leader election for static cluster membership.

## Quick start

Run a single node:

```bash
cargo run --bin thorstream
```

Enable Kafka protocol listener:

```bash
THORSTREAM_KAFKA_ADDR=0.0.0.0:9093 cargo run --bin thorstream
```

## 3-node local cluster

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

## Environment variables

- `THORSTREAM_ADDR`: custom protocol listener (default `0.0.0.0:9092`)
- `THORSTREAM_KAFKA_ADDR`: Kafka protocol listener (optional)
- `THORSTREAM_NODE_ID`: integer node id for cluster mode
- `THORSTREAM_CLUSTER_PEERS`: static peers, format `id=host:port,id=host:port`
- `THORSTREAM_COMPAT_API_ADDR`: enables HTTP compatibility APIs for Kafka Connect and Schema Registry
- `THORSTREAM_SASL_PLAIN_USERS`: comma-separated `user:password` pairs
- `THORSTREAM_SASL_SCRAM_USERS`: comma-separated `user:password` pairs for SCRAM validation
- `THORSTREAM_SASL_OAUTH_TOKENS`: comma-separated bearer tokens
- `THORSTREAM_DEFAULT_PRINCIPAL`: default runtime principal if protocol handshake is not present
- `THORSTREAM_ACL_RULES`: semicolon-separated ACL rules `principal|operation|resource_type|resource_pattern|permission`
- `THORSTREAM_ACL_DEFAULT_ALLOW`: `true/false` fallback if no ACL match
- `THORSTREAM_RBAC_BINDINGS`: role bindings, e.g. `alice=admin;bob=viewer`
- `THORSTREAM_AUDIT_LOG_PATH`: JSONL audit log file path

## Development

Run checks:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Run benchmarks:

```bash
cargo bench --bench throughput
```

Kafka compatibility tests (Python):

```bash
./tests/kafka_client_compat/setup_venv.sh
pytest tests/kafka_client_compat/test_thorstream.py -v
```

## Ecosystem compatibility

Thorstream now includes compatibility surfaces for Kafka ecosystem integrations:

- **Kafka Connect API compatibility** (HTTP):
	- `GET /connector-plugins`
	- `GET/POST /connectors`
	- `GET/DELETE /connectors/{name}`
	- `GET /connectors/{name}/status`
	- `PUT /connectors/{name}/pause` and `PUT /connectors/{name}/resume`
	- Built-in plugin descriptors for S3 sink/source, JDBC sink/source, and Debezium Postgres CDC.
	- Current scope: API/control-plane compatibility surface for connector management and discovery.

- **Schema Registry compatibility** (HTTP):
	- `GET /subjects`
	- `GET/POST /subjects/{subject}/versions`
	- `GET /subjects/{subject}/versions/{version|latest}`
	- `GET /schemas/ids/{id}`
	- `GET/PUT /config` and `GET/PUT /config/{subject}`
	- `POST /compatibility/subjects/{subject}/versions/{version|latest}`
	- Supports schema type markers for `AVRO`, `PROTOBUF`, and `JSON`.

- **Streams compatibility shim**:
	- Embedded Rust shim is available via `thorstream::streams_shim` (`StreamsBuilder`, `KStream`, `StreamTask`).
	- Supports stateless `filter_values`, `map_values`, and sink `to(...)` with `run_once(...)` execution.

Enable API server:

```bash
THORSTREAM_COMPAT_API_ADDR=127.0.0.1:8083 cargo run --bin thorstream
```

## Documentation

- Architecture: `docs/ARCHITECTURE.md`
- Operations: `docs/OPERATIONS.md`
- Deployment (TLS + reverse proxy): `docs/DEPLOYMENT_TLS.md`
- Security (SASL/ACL/RBAC/audit): `docs/SECURITY_ENTERPRISE.md`
- Release checklist: `docs/RELEASE_CHECKLIST.md`
- Security policy: `SECURITY.md`
- Contribution guide: `CONTRIBUTING.md`

## Current limitations

- Leader election is heartbeat-based and deterministic, not full Raft log consensus.
- Cluster membership is static at startup.
- Snapshot shipping and log compaction are not implemented.

## License

MIT License. See `LICENSE`.
