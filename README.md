<p align="center"><img src="https://raw.githubusercontent.com/mdakram28/thorstream/refs/heads/main/documentation/assets/logo.svg" width="400px" /></p>

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
- `THORSTREAM_OBJECT_STORE_DIR`: object-store-backed mirror root for segment durability
- `THORSTREAM_OBJECT_STORE_REQUIRED`: fail writes if object-store mirror fails (`true/false`)
- `THORSTREAM_SASL_PLAIN_USERS`: comma-separated `user:password` pairs
- `THORSTREAM_SASL_SCRAM_USERS`: comma-separated `user:password` pairs for SCRAM validation
- `THORSTREAM_SASL_OAUTH_TOKENS`: comma-separated bearer tokens
- `THORSTREAM_DEFAULT_PRINCIPAL`: default runtime principal if protocol handshake is not present
- `THORSTREAM_ACL_RULES`: semicolon-separated ACL rules `principal|operation|resource_type|resource_pattern|permission`
- `THORSTREAM_ACL_DEFAULT_ALLOW`: `true/false` fallback if no ACL match
- `THORSTREAM_RBAC_BINDINGS`: role bindings, e.g. `alice=admin;bob=viewer`
- `THORSTREAM_AUDIT_LOG_PATH`: JSONL audit log file path
- `THORSTREAM_LOG_FORMAT`: set `json` for structured JSON logs

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

## Web UI

Thorstream now includes a simple UI project at `ui/` for operations visibility and basic API actions.

- Dashboard metrics from `/metrics` (lag, partition size, throughput, under-replicated partitions, p99 latency)
- Basic Kafka Connect operations (list/create connectors)
- Basic Schema Registry operations (list subjects/register schema)

Run:

```bash
cd ui
npm install
npm run dev
```

The UI defaults to Vite proxy `/api -> http://127.0.0.1:8083`.

See `ui/README.md` for full setup and build instructions.

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

## Advanced log semantics

- Partitioned log ordering guarantees per partition.
- Retention policies:
	- Time-based retention (`TopicConfig.retention_ms`)
	- Size-based retention (`TopicConfig.retention_bytes`)
	- Log compaction by key (`TopicConfig.cleanup_policy = Compact`)
- Idempotent producer semantics:
	- Producer ID + sequence number checks (`Producer::send_idempotent`)
	- Duplicate sequence dedupe and gap rejection
- Exactly-once scaffolding (EOS):
	- Transaction begin/commit/abort (`Producer::begin_transaction`, `send_transactional`, `commit_transaction`, `abort_transaction`)
	- Producer ID/sequence metadata persisted in records
- High watermark / ISR semantics:
	- Topic `min_insync_replicas` support
	- Partition high watermark computed from ISR minimum
	- Replica consistency checks on append path

## Documentation
- Website: [https://mdakram.com/thorstream/](https://mdakram.com/thorstream/)
- Documentation home: [documentation/index.md](documentation/index.md)
- Documentation index map: [documentation/DOCS_INDEX.md](documentation/DOCS_INDEX.md)
- Architecture: [documentation/ARCHITECTURE.md](documentation/ARCHITECTURE.md)
- Operations: [documentation/OPERATIONS.md](documentation/OPERATIONS.md)
- Deployment (TLS + reverse proxy): [documentation/DEPLOYMENT_TLS.md](documentation/DEPLOYMENT_TLS.md)
- Kubernetes-first deployment: [documentation/KUBERNETES.md](documentation/KUBERNETES.md)
- Security (SASL/ACL/RBAC/audit): [documentation/SECURITY_ENTERPRISE.md](documentation/SECURITY_ENTERPRISE.md)
- Release checklist: [documentation/RELEASE_CHECKLIST.md](documentation/RELEASE_CHECKLIST.md)
- UI guide: [ui/README.md](ui/README.md)

### MkDocs Material site

A full documentation website is included via MkDocs Material.

Run locally:

```bash
/home/akram/workspace/thorstream/.venv/bin/python -m pip install -r documentation/requirements-mkdocs.txt
/home/akram/workspace/thorstream/.venv/bin/python -m mkdocs serve
```

Build static site:

```bash
/home/akram/workspace/thorstream/.venv/bin/python -m mkdocs build
```

Configuration is in [mkdocs.yml](mkdocs.yml) and source pages are in [documentation/index.md](documentation/index.md).

## Observability & Ops

- Prometheus-compatible metrics endpoint: `GET /metrics` on the compatibility API listener.
- Exposed metrics include:
	- consumer lag per group/topic/partition
	- partition size bytes
	- throughput counters (produce/fetch records and bytes)
	- under-replicated partitions
	- request latency p99 (ms)
- Structured logs:
	- set `THORSTREAM_LOG_FORMAT=json`
- OpenTelemetry hooks:
	- request paths emit `tracing` spans (`thorstream.request`) for custom and Kafka transports
	- these spans can be forwarded by an OTEL-enabled subscriber/collector pipeline
- Security policy: `SECURITY.md`
- Contribution guide: `CONTRIBUTING.md`

## Current limitations

- Leader election is heartbeat-based and deterministic, not full Raft log consensus.
- Cluster membership is static at startup.
- Snapshot shipping and log compaction are not implemented.

## License

MIT License. See `LICENSE`.
