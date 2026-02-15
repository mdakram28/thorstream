use std::sync::Arc;

use tempfile::TempDir;
use thorstream::{Broker, BrokerConfig, CleanupPolicy, Producer, Record, TopicConfig};

fn broker_with_temp_dir() -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let broker = Arc::new(
        Broker::new(BrokerConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        })
        .unwrap(),
    );
    (broker, dir)
}

#[test]
fn partition_ordering_guarantees_per_partition() {
    let (broker, _dir) = broker_with_temp_dir();
    broker
        .create_topic(
            "ordered",
            Some(TopicConfig {
                num_partitions: 2,
                ..TopicConfig::default()
            }),
        )
        .unwrap();

    for i in 0..5 {
        broker
            .produce(
                "ordered",
                Some(1),
                Record::new(format!("v{}", i).into_bytes()),
            )
            .unwrap();
    }

    let rows = broker.fetch("ordered", 1, 0, 1024 * 1024, 100).unwrap();
    let offsets: Vec<i64> = rows.iter().map(|r| r.offset).collect();
    assert_eq!(offsets, vec![0, 1, 2, 3, 4]);
}

#[test]
fn retention_time_and_compaction() {
    let (broker, _dir) = broker_with_temp_dir();
    broker
        .create_topic(
            "compact-retain",
            Some(TopicConfig {
                retention_ms: Some(1),
                cleanup_policy: CleanupPolicy::Compact,
                ..TopicConfig::default()
            }),
        )
        .unwrap();

    let old = Record {
        key: Some(b"k1".to_vec()),
        value: b"old".to_vec(),
        headers: vec![],
        timestamp: Some(1),
        producer_id: None,
        sequence: None,
        transaction_id: None,
    };
    broker.produce("compact-retain", Some(0), old).unwrap();

    let new = Record {
        key: Some(b"k1".to_vec()),
        value: b"new".to_vec(),
        headers: vec![],
        timestamp: Some(i64::MAX / 4),
        producer_id: None,
        sequence: None,
        transaction_id: None,
    };
    broker.produce("compact-retain", Some(0), new).unwrap();

    let rows = broker
        .fetch("compact-retain", 0, 0, 1024 * 1024, 100)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].record.value.as_slice(), b"new");
}

#[test]
fn retention_size_based() {
    let (broker, _dir) = broker_with_temp_dir();
    broker
        .create_topic(
            "size-retain",
            Some(TopicConfig {
                retention_bytes: Some(120),
                ..TopicConfig::default()
            }),
        )
        .unwrap();

    for i in 0..10 {
        broker
            .produce("size-retain", Some(0), Record::new(vec![i as u8; 32]))
            .unwrap();
    }

    let rows = broker.fetch("size-retain", 0, 0, 1024 * 1024, 100).unwrap();
    assert!(rows.len() < 10);
}

#[test]
fn idempotent_producer_sequences() {
    let (broker, _dir) = broker_with_temp_dir();
    let producer = Producer::new(Arc::clone(&broker));

    let r0 = producer
        .send_idempotent("idem", Record::new(b"a".to_vec()), Some(0), 42, 0)
        .unwrap();
    let r0_dup = producer
        .send_idempotent("idem", Record::new(b"a-dup".to_vec()), Some(0), 42, 0)
        .unwrap();
    let r1 = producer
        .send_idempotent("idem", Record::new(b"b".to_vec()), Some(0), 42, 1)
        .unwrap();

    assert_eq!(r0.offset, r0_dup.offset);
    assert_eq!(r1.offset, r0.offset + 1);
}

#[test]
fn eos_transactions_commit_and_abort() {
    let (broker, _dir) = broker_with_temp_dir();
    let producer = Producer::new(Arc::clone(&broker));

    producer.begin_transaction("tx-1", 7);
    producer
        .send_transactional("tx-1", "tx-topic", Record::new(b"c1".to_vec()), Some(0))
        .unwrap();
    producer
        .send_transactional("tx-1", "tx-topic", Record::new(b"c2".to_vec()), Some(0))
        .unwrap();
    producer.commit_transaction("tx-1").unwrap();

    producer.begin_transaction("tx-2", 7);
    producer
        .send_transactional("tx-2", "tx-topic", Record::new(b"a1".to_vec()), Some(0))
        .unwrap();
    producer.abort_transaction("tx-2");

    let rows = broker.fetch("tx-topic", 0, 0, 1024 * 1024, 100).unwrap();
    let values: Vec<Vec<u8>> = rows.into_iter().map(|r| r.record.value).collect();
    assert_eq!(values, vec![b"c1".to_vec(), b"c2".to_vec()]);
}

#[test]
fn high_watermark_and_isr_consistency() {
    let (broker, _dir) = broker_with_temp_dir();
    broker
        .create_topic(
            "rf-topic",
            Some(TopicConfig {
                replication_factor: 3,
                min_insync_replicas: 2,
                ..TopicConfig::default()
            }),
        )
        .unwrap();

    for i in 0..4 {
        broker
            .produce("rf-topic", Some(0), Record::new(vec![i as u8]))
            .unwrap();
    }

    let isr = broker.in_sync_replicas("rf-topic", 0).unwrap();
    assert!(isr.len() >= 2);
    assert_eq!(broker.high_water_mark("rf-topic", 0).unwrap(), 4);
}
