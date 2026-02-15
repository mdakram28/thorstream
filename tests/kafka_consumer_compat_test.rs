//! Kafka consumer library compatibility tests.
//!
//! These tests verify the **semantic contract** that Kafka consumer libraries
//! rely on: offset ordering, commit/resume, subscribe/assign, poll ordering,
//! seek, position, and high water mark. Passing these tests ensures that code
//! written for a Kafka consumer (or a thin adapter) will behave correctly
//! when backed by Thorstream.

use std::sync::Arc;
use thorstream::{
    Broker, BrokerConfig, Consumer, Producer, Record, TopicConfig, TopicPartition,
};
use tempfile::TempDir;

fn broker_with_temp_dir() -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let config = BrokerConfig {
        data_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let broker = Arc::new(Broker::new(config).unwrap());
    (broker, dir)
}

// ---- Offset ordering (Kafka: offsets are strictly increasing per partition) ----

#[test]
fn kafka_compat_offsets_strictly_increasing_per_partition() {
    let (broker, _dir) = broker_with_temp_dir();
    broker.create_topic("t", None).unwrap();
    let producer = Producer::new(Arc::clone(&broker));
    for i in 0u8..5 {
        let res = producer.send("t", Record::new(vec![i]), None).unwrap();
        assert_eq!(res.offset, i as i64, "offset must be 0,1,2,...");
    }
    let records = broker.fetch("t", 0, 0, 1024, 10).unwrap();
    let offsets: Vec<i64> = records.iter().map(|r| r.offset).collect();
    assert_eq!(offsets, [0, 1, 2, 3, 4], "fetched offsets must be strictly increasing");
}

#[test]
fn kafka_compat_fetch_from_offset_returns_from_that_offset_inclusive() {
    let (broker, _dir) = broker_with_temp_dir();
    broker.create_topic("t", None).unwrap();
    for i in 0..5 {
        broker
            .produce("t", Some(0), Record::new(vec![i]))
            .unwrap();
    }
    let from_2 = broker.fetch("t", 0, 2, 1024, 10).unwrap();
    assert_eq!(from_2.len(), 3);
    assert_eq!(from_2[0].offset, 2);
    assert_eq!(from_2[1].offset, 3);
    assert_eq!(from_2[2].offset, 4);
}

// ---- Consumer group: commit and resume (no duplicates, no gap) ----

#[test]
fn kafka_compat_consumer_group_resume_after_commit_no_duplicates() {
    let (broker, _dir) = broker_with_temp_dir();
    broker.create_topic("events", None).unwrap();
    let producer = Producer::new(Arc::clone(&broker));
    producer.send("events", Record::new(b"a".to_vec()), None).unwrap();
    producer.send("events", Record::new(b"b".to_vec()), None).unwrap();
    producer.send("events", Record::new(b"c".to_vec()), None).unwrap();

    let mut c1 = Consumer::new(Arc::clone(&broker));
    c1.set_group_id("g1");
    c1.subscribe(vec!["events".to_string()]).unwrap();
    let batch1 = c1.poll(None).unwrap();
    assert_eq!(batch1.len(), 3);
    c1.commit().unwrap();

    // Simulate restart: new consumer, same group → must resume from committed offset
    let mut c2 = Consumer::new(Arc::clone(&broker));
    c2.set_group_id("g1");
    c2.subscribe(vec!["events".to_string()]).unwrap();
    let batch2 = c2.poll(None).unwrap();
    assert_eq!(batch2.len(), 0, "resumed consumer must not see already-committed records");
}

#[test]
fn kafka_compat_consumer_group_partial_commit_resume_from_committed() {
    let (broker, _dir) = broker_with_temp_dir();
    broker.create_topic("t", None).unwrap();
    let producer = Producer::new(Arc::clone(&broker));
    for i in 0..5 {
        producer.send("t", Record::new(vec![i]), None).unwrap();
    }

    let mut c1 = Consumer::new(Arc::clone(&broker));
    c1.set_group_id("g1");
    c1.subscribe(vec!["t".to_string()]).unwrap();
    let _ = c1.poll(None).unwrap(); // consume all 5
    c1.seek("t", 0, 2).unwrap();    // rewind to offset 2
    c1.commit().unwrap();           // commit offset 2 (next read from 2)

    let mut c2 = Consumer::new(Arc::clone(&broker));
    c2.set_group_id("g1");
    c2.subscribe(vec!["t".to_string()]).unwrap();
    let batch = c2.poll(None).unwrap();
    assert_eq!(batch.len(), 3, "must see offsets 2,3,4");
    assert_eq!(batch[0].offset, 2);
    assert_eq!(batch[1].offset, 3);
    assert_eq!(batch[2].offset, 4);
}

// ---- Multiple partitions (Kafka: subscribe = all partitions; offsets per partition) ----

#[test]
fn kafka_compat_subscribe_multiple_partitions_poll_returns_all() {
    let (broker, _dir) = broker_with_temp_dir();
    broker
        .create_topic(
            "multi",
            Some(TopicConfig {
                num_partitions: 2,
                ..Default::default()
            }),
        )
        .unwrap();
    let producer = Producer::new(Arc::clone(&broker));
    producer.send("multi", Record::new(b"p0-a".to_vec()), Some(0)).unwrap();
    producer.send("multi", Record::new(b"p0-b".to_vec()), Some(0)).unwrap();
    producer.send("multi", Record::new(b"p1-a".to_vec()), Some(1)).unwrap();

    let mut consumer = Consumer::new(Arc::clone(&broker));
    consumer.subscribe(vec!["multi".to_string()]).unwrap();
    let batch = consumer.poll(None).unwrap();
    assert_eq!(batch.len(), 3);
    let by_partition: std::collections::HashMap<i32, Vec<_>> =
        batch.iter().map(|r| (r.partition, r.offset)).fold(
            std::collections::HashMap::new(),
            |mut m, (p, o)| {
                m.entry(p).or_default().push(o);
                m
            },
        );
    assert_eq!(by_partition.get(&0).map(|v| v.as_slice()), Some([0, 1].as_slice()));
    assert_eq!(by_partition.get(&1).map(|v| v.as_slice()), Some([0].as_slice()));
}

#[test]
fn kafka_compat_assign_single_partition_only_sees_that_partition() {
    let (broker, _dir) = broker_with_temp_dir();
    broker
        .create_topic(
            "t",
            Some(TopicConfig {
                num_partitions: 2,
                ..Default::default()
            }),
        )
        .unwrap();
    let producer = Producer::new(Arc::clone(&broker));
    producer.send("t", Record::new(b"p0".to_vec()), Some(0)).unwrap();
    producer.send("t", Record::new(b"p1".to_vec()), Some(1)).unwrap();

    let mut consumer = Consumer::new(Arc::clone(&broker));
    consumer
        .assign(vec![TopicPartition {
            topic: "t".to_string(),
            partition: 1,
        }])
        .unwrap();
    let batch = consumer.poll(None).unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].partition, 1);
    assert_eq!(batch[0].record.value.as_slice(), b"p1");
}

// ---- Seek and position (Kafka: seek sets next fetch offset; position returns it) ----

#[test]
fn kafka_compat_seek_then_position_returns_seek_offset() {
    let (broker, _dir) = broker_with_temp_dir();
    broker.create_topic("t", None).unwrap();
    for i in 0..5 {
        broker.produce("t", Some(0), Record::new(vec![i])).unwrap();
    }
    let mut consumer = Consumer::new(Arc::clone(&broker));
    consumer.assign(vec![TopicPartition { topic: "t".to_string(), partition: 0 }]).unwrap();
    assert_eq!(consumer.position("t", 0).unwrap(), 0);
    consumer.seek("t", 0, 3).unwrap();
    assert_eq!(consumer.position("t", 0).unwrap(), 3);
    let batch = consumer.poll(None).unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].offset, 3);
    assert_eq!(batch[1].offset, 4);
}

#[test]
fn kafka_compat_seek_to_beginning() {
    let (broker, _dir) = broker_with_temp_dir();
    broker.create_topic("t", None).unwrap();
    broker.produce("t", Some(0), Record::new(b"first".to_vec())).unwrap();
    broker.produce("t", Some(0), Record::new(b"second".to_vec())).unwrap();

    let mut consumer = Consumer::new(Arc::clone(&broker));
    consumer.assign(vec![TopicPartition { topic: "t".to_string(), partition: 0 }]).unwrap();
    let _ = consumer.poll(None).unwrap();
    consumer.seek("t", 0, 0).unwrap();
    let batch = consumer.poll(None).unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].record.value.as_slice(), b"first");
    assert_eq!(batch[1].record.value.as_slice(), b"second");
}

// ---- Empty poll (Kafka: poll returns empty when no new data, not error) ----

#[test]
fn kafka_compat_poll_empty_when_no_data() {
    let (broker, _dir) = broker_with_temp_dir();
    broker.create_topic("t", None).unwrap();
    let mut consumer = Consumer::new(Arc::clone(&broker));
    consumer.subscribe(vec!["t".to_string()]).unwrap();
    let batch = consumer.poll(None).unwrap();
    assert!(batch.is_empty());
}

#[test]
fn kafka_compat_poll_empty_after_consuming_all() {
    let (broker, _dir) = broker_with_temp_dir();
    broker.create_topic("t", None).unwrap();
    broker.produce("t", Some(0), Record::new(b"only".to_vec())).unwrap();
    let mut consumer = Consumer::new(Arc::clone(&broker));
    consumer.subscribe(vec!["t".to_string()]).unwrap();
    let one = consumer.poll(None).unwrap();
    assert_eq!(one.len(), 1);
    let empty = consumer.poll(None).unwrap();
    assert!(empty.is_empty());
}

// ---- High water mark (Kafka: next offset to be assigned; fetch up to HWM-1) ----

#[test]
fn kafka_compat_high_water_mark_is_next_assignable_offset() {
    let (broker, _dir) = broker_with_temp_dir();
    broker.create_topic("t", None).unwrap();
    assert_eq!(broker.high_water_mark("t", 0).unwrap(), 0);
    broker.produce("t", Some(0), Record::new(b"a".to_vec())).unwrap();
    assert_eq!(broker.high_water_mark("t", 0).unwrap(), 1);
    broker.produce("t", Some(0), Record::new(b"b".to_vec())).unwrap();
    assert_eq!(broker.high_water_mark("t", 0).unwrap(), 2);
}

// ---- Start offset (Kafka: earliest offset is 0 for new log) ----

#[test]
fn kafka_compat_start_offset_zero() {
    let (broker, _dir) = broker_with_temp_dir();
    broker.create_topic("t", None).unwrap();
    assert_eq!(broker.start_offset("t", 0).unwrap(), 0);
}

// ---- StoredRecord has partition and offset (Kafka ConsumerRecord semantics) ----

#[test]
fn kafka_compat_stored_record_has_partition_and_offset() {
    let (broker, _dir) = broker_with_temp_dir();
    broker
        .create_topic(
            "t",
            Some(TopicConfig {
                num_partitions: 2,
                ..Default::default()
            }),
        )
        .unwrap();
    broker.produce("t", Some(1), Record::new(b"v".to_vec())).unwrap();
    let records = broker.fetch("t", 1, 0, 1024, 10).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].partition, 1);
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[0].record.value.as_slice(), b"v");
}
