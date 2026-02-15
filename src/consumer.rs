//! Kafka-compatible consumer: subscribe, poll, commit offsets.

use crate::broker::Broker;
use crate::error::{Result, ThorstreamError};
use crate::types::StoredRecord;
use std::sync::Arc;
use std::time::Duration;

/// Topic partition assignment (Kafka-compatible).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

/// Kafka-compatible consumer: assign topic/partitions, poll, commit.
pub struct Consumer {
    broker: Arc<Broker>,
    /// Consumer group (for offset commit/fetch).
    group_id: Option<String>,
    /// Assigned topic-partitions.
    assignment: Vec<TopicPartition>,
    /// Current offset per topic/partition (for fetch next).
    offsets: std::collections::HashMap<TopicPartition, i64>,
    /// Default fetch sizes.
    max_bytes: usize,
    max_records: usize,
}

impl Consumer {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self {
            broker,
            group_id: None,
            assignment: Vec::new(),
            offsets: std::collections::HashMap::new(),
            max_bytes: 1024 * 1024,
            max_records: 500,
        }
    }

    /// Set consumer group (enables offset commit/fetch).
    pub fn set_group_id(&mut self, group_id: impl Into<String>) {
        self.group_id = Some(group_id.into());
    }

    /// Assign specific topic-partitions (Kafka assign API).
    pub fn assign(&mut self, topic_partitions: Vec<TopicPartition>) -> Result<()> {
        for tp in &topic_partitions {
            self.broker.ensure_topic(&tp.topic)?;
            let n = self.broker.num_partitions(&tp.topic)?;
            if tp.partition < 0 || tp.partition >= n {
                return Err(ThorstreamError::PartitionNotFound {
                    topic: tp.topic.clone(),
                    partition: tp.partition,
                });
            }
            if !self.offsets.contains_key(tp) {
                let start = self
                    .group_id
                    .as_ref()
                    .and_then(|g| self.fetch_committed_offset(g, &tp.topic, tp.partition).ok())
                    .flatten()
                    .unwrap_or(0);
                self.offsets.insert(tp.clone(), start);
            }
        }
        self.assignment = topic_partitions;
        Ok(())
    }

    /// Subscribe to topic(s) — assigns all partitions of each topic.
    pub fn subscribe(&mut self, topics: Vec<String>) -> Result<()> {
        let mut assignment = Vec::new();
        for topic in &topics {
            self.broker.ensure_topic(topic)?;
            let n = self.broker.num_partitions(topic)?;
            for p in 0..n {
                let tp = TopicPartition {
                    topic: topic.clone(),
                    partition: p,
                };
                if !self.offsets.contains_key(&tp) {
                    let start = self
                        .group_id
                        .as_ref()
                        .and_then(|g| self.fetch_committed_offset(g, topic, p).ok())
                        .flatten()
                        .unwrap_or(0);
                    self.offsets.insert(tp.clone(), start);
                }
                assignment.push(tp);
            }
        }
        self.assignment = assignment;
        Ok(())
    }

    fn fetch_committed_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>> {
        // Delegate to broker; broker stores committed offsets (in-memory for now).
        self.broker
            .offset_fetch(group_id, topic, partition)
            .map(Some)
            .or(Ok(None))
    }

    /// Poll for records (Kafka-compatible: returns when data is available or timeout).
    pub fn poll(&mut self, timeout: Option<Duration>) -> Result<Vec<StoredRecord>> {
        let mut all = Vec::new();
        for tp in &self.assignment.clone() {
            let start = *self.offsets.get(tp).unwrap_or(&0);
            let records = self.broker.fetch(
                &tp.topic,
                tp.partition,
                start,
                self.max_bytes,
                self.max_records,
            )?;
            if !records.is_empty() {
                let next = records.last().map(|r| r.offset + 1).unwrap_or(start);
                self.offsets.insert(tp.clone(), next);
                all.extend(records);
            }
        }
        if all.is_empty() && timeout.map(|t| t.as_millis() > 0).unwrap_or(false) {
            std::thread::sleep(timeout.unwrap());
            return self.poll(None);
        }
        Ok(all)
    }

    /// Seek to offset for a topic partition.
    pub fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        let tp = TopicPartition {
            topic: topic.to_string(),
            partition,
        };
        if self.assignment.contains(&tp) {
            self.offsets.insert(tp, offset);
            Ok(())
        } else {
            Err(ThorstreamError::PartitionNotFound {
                topic: topic.to_string(),
                partition,
            })
        }
    }

    /// Commit current offsets for assigned partitions (Kafka commitSync semantics).
    pub fn commit(&mut self) -> Result<()> {
        let group_id = self
            .group_id
            .as_ref()
            .ok_or_else(|| ThorstreamError::ConsumerGroup("No group_id set".into()))?;
        for tp in &self.assignment {
            let offset = *self.offsets.get(tp).unwrap_or(&0);
            self.broker
                .offset_commit(group_id, &tp.topic, tp.partition, offset)?;
        }
        Ok(())
    }

    /// Current position (offset) for a topic partition.
    pub fn position(&self, topic: &str, partition: i32) -> Result<i64> {
        let tp = TopicPartition {
            topic: topic.to_string(),
            partition,
        };
        self.offsets
            .get(&tp)
            .copied()
            .ok_or_else(|| ThorstreamError::PartitionNotFound {
                topic: topic.to_string(),
                partition,
            })
    }

    pub fn set_max_bytes(&mut self, max_bytes: usize) {
        self.max_bytes = max_bytes;
    }
    pub fn set_max_records(&mut self, max_records: usize) {
        self.max_records = max_records;
    }
}
