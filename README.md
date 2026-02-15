# Thorstream

A fast, Kafka-like append-only event streaming platform in Rust with **Kafka consumer–compatible semantics**: topics, partitions, offsets, consumer groups, commit/seek, and poll-based consumption.

## Features

- **Append-only log storage** — Segment files with length-prefixed records; designed for fast sequential writes and reads.
- **Kafka-style model** — Topics, partitions, offsets, high water mark, and consumer group offset commit/fetch.
- **Producer API** — Send single or batch records; optional key and partition.
- **Consumer API** — Subscribe or assign topic/partitions, poll, seek, and commit offsets (same concepts as Kafka).
- **Wire protocol** — Length-prefixed binary protocol (Metadata, Produce, Fetch, OffsetCommit, OffsetFetch) for a remote server.
- **Extensible layout** — Storage, broker, protocol, and server are split into modules for easier evolution (e.g. segment rollover, replication).

## Quick start

**Run the server** (default: `0.0.0.0:9092`):

```bash
cargo run --bin thorstream
# or
THORSTREAM_ADDR=0.0.0.0:9093 cargo run --bin thorstream
```

**Use in-process (embedded)**:

```rust
use std::sync::Arc;
use thorstream::{Broker, BrokerConfig, Producer, Consumer, Record};

let config = BrokerConfig::default();
let broker = Arc::new(Broker::new(config)?);
broker.create_topic("events", None)?;

let producer = Producer::new(Arc::clone(&broker));
producer.send("events", Record::new(b"hello".to_vec()), None)?;

let mut consumer = Consumer::new(Arc::clone(&broker));
consumer.set_group_id("my-group");
consumer.subscribe(vec!["events".to_string()])?;
let records = consumer.poll(None)?;
consumer.commit()?;
```

## Project layout

```
src/
├── lib.rs           # Re-exports and public API
├── error.rs          # Error type
├── types.rs          # Record, StoredRecord (Kafka-like)
├── storage/          # Append-only log
│   ├── segment.rs   # Single segment file (length-prefixed, bincode)
│   └── log.rs        # Partition log (per-topic/partition)
├── broker/           # Topic/partition management
│   └── topic.rs      # Broker, create_topic, produce, fetch, offset commit/fetch
├── protocol/         # Wire format
│   └── codec.rs      # Request/response encode/decode
├── producer.rs       # Producer client
├── consumer.rs       # Kafka-compatible consumer (assign, subscribe, poll, commit, seek)
└── server/           # TCP server
    └── handler.rs    # Connection handler, dispatch to broker
```

## Consumer compatibility

The consumer API mirrors Kafka’s:

- **Subscribe** — Subscribe to topic(s); all partitions are assigned.
- **Assign** — Manually assign topic/partition(s).
- **Poll** — Fetch records for assigned partitions; offsets advance automatically.
- **Commit** — Commit current offsets for the consumer group (requires `set_group_id`).
- **Seek** — Set position for a topic/partition.
- **Position** — Current offset for a topic/partition.

Committed offsets are stored per (group_id, topic, partition) and used on the next subscribe/assign so each consumer in a group resumes from the last committed position.

## Configuration

- **BrokerConfig** — `data_dir`, `default_topic_config` (e.g. `num_partitions`).
- **TopicConfig** — `num_partitions`, `replication_factor` (replication not implemented; for API compatibility).
- **PartitionLogConfig** — `max_segment_size_bytes` (for future segment rollover), `data_dir`.

## Testing with standard Kafka client libraries

Thorstream speaks the Kafka wire protocol so **standard Kafka clients** (e.g. kafka-python) can connect. Tests are in Python (pytest) and **start the Thorstream Kafka broker automatically** for the session.

1. **Build the binary** (so the test can start it): `cargo build --bin thorstream`
2. **Set up a venv and run tests** (from repo root):
   ```bash
   ./tests/kafka_client_compat/setup_venv.sh
   pytest tests/kafka_client_compat/test_thorstream.py -v
   ```
   The tests start the broker on `127.0.0.1:19093`, run, then stop it. No need to run the server manually.

## License

MIT
