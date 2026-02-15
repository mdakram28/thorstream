//! Integration tests: broker, producer, consumer.

use std::sync::Arc;
use tempfile::TempDir;
use thorstream::{Broker, BrokerConfig, Consumer, Producer, Record, TopicConfig, TopicPartition};

fn broker_with_temp_dir() -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let config = BrokerConfig {
        data_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let broker = Arc::new(Broker::new(config).unwrap());
    (broker, dir)
}

#[test]
fn produce_and_fetch() {
    let (broker, _dir) = broker_with_temp_dir();
    broker.create_topic("t1", None).unwrap();
    let (p, off) = broker
        .produce("t1", None, Record::new(b"hello".to_vec()))
        .unwrap();
    assert_eq!(p, 0);
    assert_eq!(off, 0);
    let records = broker.fetch("t1", 0, 0, 1024, 10).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[0].record.value.as_slice(), b"hello");
}

#[test]
fn producer_consumer_flow() {
    let (broker, _dir) = broker_with_temp_dir();
    let producer = Producer::new(Arc::clone(&broker));
    producer
        .send("events", Record::new(b"e1".to_vec()), None)
        .unwrap();
    producer
        .send("events", Record::new(b"e2".to_vec()), None)
        .unwrap();

    let mut consumer = Consumer::new(Arc::clone(&broker));
    consumer.subscribe(vec!["events".to_string()]).unwrap();
    let batch = consumer.poll(None).unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].record.value.as_slice(), b"e1");
    assert_eq!(batch[1].record.value.as_slice(), b"e2");

    consumer.commit().unwrap_err(); // no group_id
    consumer.set_group_id("g1");
    consumer.commit().unwrap();

    let mut consumer2 = Consumer::new(Arc::clone(&broker));
    consumer2.set_group_id("g1");
    consumer2.subscribe(vec!["events".to_string()]).unwrap();
    let batch2 = consumer2.poll(None).unwrap();
    assert_eq!(batch2.len(), 0); // already consumed, offset committed
}

#[test]
fn assign_and_seek() {
    let (broker, _dir) = broker_with_temp_dir();
    broker.create_topic("t2", None).unwrap();
    broker
        .produce("t2", Some(0), Record::new(b"a".to_vec()))
        .unwrap();
    broker
        .produce("t2", Some(0), Record::new(b"b".to_vec()))
        .unwrap();

    let mut consumer = Consumer::new(Arc::clone(&broker));
    consumer
        .assign(vec![TopicPartition {
            topic: "t2".to_string(),
            partition: 0,
        }])
        .unwrap();
    consumer.seek("t2", 0, 1).unwrap();
    let batch = consumer.poll(None).unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].record.value.as_slice(), b"b");
}

#[test]
fn crash_recovery_reloads_topics_and_data() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let broker = Broker::new(BrokerConfig {
            data_dir: data_dir.clone(),
            ..Default::default()
        })
        .unwrap();
        broker.create_topic("recovery", None).unwrap();
        broker
            .produce("recovery", Some(0), Record::new(b"r1".to_vec()))
            .unwrap();
        broker
            .produce("recovery", Some(0), Record::new(b"r2".to_vec()))
            .unwrap();
    }

    let recovered = Broker::new(BrokerConfig {
        data_dir,
        ..Default::default()
    })
    .unwrap();

    let records = recovered.fetch("recovery", 0, 0, 1024, 10).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].record.value.as_slice(), b"r1");
    assert_eq!(records[1].record.value.as_slice(), b"r2");
}

#[test]
fn replication_writes_all_replicas_consistently() {
    let (broker, _dir) = broker_with_temp_dir();
    broker
        .create_topic(
            "replicated",
            Some(TopicConfig {
                num_partitions: 1,
                replication_factor: 3,
            }),
        )
        .unwrap();

    for payload in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
        broker
            .produce("replicated", Some(0), Record::new(payload.to_vec()))
            .unwrap();
    }

    let hwms = broker.replica_high_water_marks("replicated", 0).unwrap();
    assert_eq!(hwms, vec![3, 3, 3]);

    let fetched = broker.fetch("replicated", 0, 0, 1024, 10).unwrap();
    assert_eq!(fetched.len(), 3);
    assert_eq!(fetched[0].record.value.as_slice(), b"a");
    assert_eq!(fetched[1].record.value.as_slice(), b"b");
    assert_eq!(fetched[2].record.value.as_slice(), b"c");
}
