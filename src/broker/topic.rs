//! Broker and topic management.

use crate::error::{Result, ThorstreamError};
use crate::storage::{PartitionLog, PartitionLogConfig};
use crate::types::{Record, StoredRecord};
use dashmap::DashMap;
use std::sync::Arc;

/// Per-topic configuration (Kafka-compatible: partitions, replication, etc.).
#[derive(Clone, Debug)]
pub struct TopicConfig {
    /// Number of partitions.
    pub num_partitions: i32,
    /// Replication factor (ignored in single-node; for API compatibility).
    pub replication_factor: i16,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            num_partitions: 1,
            replication_factor: 1,
        }
    }
}

/// Broker-wide configuration.
#[derive(Clone, Debug)]
pub struct BrokerConfig {
    pub data_dir: std::path::PathBuf,
    pub default_topic_config: TopicConfig,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            data_dir: std::path::PathBuf::from("data"),
            default_topic_config: TopicConfig::default(),
        }
    }
}

/// Central broker: creates topics, routes produce/fetch to partition logs.
pub struct Broker {
    config: BrokerConfig,
    /// topic_name -> (partition_id -> PartitionLog)
    topics: DashMap<String, Vec<Arc<PartitionLog>>>,
    /// Committed offsets: (group_id, topic, partition) -> offset.
    offsets: DashMap<(String, String, i32), i64>,
}

impl Broker {
    pub fn new(config: BrokerConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)
            .map_err(|e| ThorstreamError::Storage(e.to_string()))?;
        Ok(Self {
            config,
            topics: DashMap::new(),
            offsets: DashMap::new(),
        })
    }

    /// Create a topic with given config. Idempotent: if topic exists, ensure partition count.
    pub fn create_topic(
        &self,
        name: impl AsRef<str>,
        config: Option<TopicConfig>,
    ) -> Result<()> {
        let name = name.as_ref().to_string();
        let config = config.unwrap_or_else(|| self.config.default_topic_config.clone());
        if self.topics.contains_key(&name) {
            let existing = self.topics.get(&name).unwrap();
            if existing.len() != config.num_partitions as usize {
                return Err(ThorstreamError::Storage(format!(
                    "Topic {} exists with {} partitions, cannot change to {}",
                    name,
                    existing.len(),
                    config.num_partitions
                )));
            }
            return Ok(());
        }
        let log_config = PartitionLogConfig {
            data_dir: self.config.data_dir.clone(),
            max_segment_size_bytes: 1024 * 1024 * 1024,
        };
        let mut logs = Vec::with_capacity(config.num_partitions as usize);
        for p in 0..config.num_partitions {
            let log = PartitionLog::open(&name, p, log_config.clone())?;
            logs.push(Arc::new(log));
        }
        self.topics.insert(name, logs);
        Ok(())
    }

    /// Ensure topic exists (create with default config if not).
    pub fn ensure_topic(&self, name: impl AsRef<str>) -> Result<()> {
        let name = name.as_ref();
        if !self.topics.contains_key(name) {
            self.create_topic(name, None)?;
        }
        Ok(())
    }

    fn partition_log(&self, topic: &str, partition: i32) -> Result<Arc<PartitionLog>> {
        let logs = self
            .topics
            .get(topic)
            .ok_or_else(|| ThorstreamError::TopicNotFound(topic.to_string()))?;
        let idx = partition as usize;
        if idx >= logs.len() {
            return Err(ThorstreamError::PartitionNotFound {
                topic: topic.to_string(),
                partition,
            });
        }
        Ok(Arc::clone(&logs[idx]))
    }

    /// Produce: append record to partition, return (partition, offset).
    pub fn produce(
        &self,
        topic: impl AsRef<str>,
        partition: Option<i32>,
        record: Record,
    ) -> Result<(i32, i64)> {
        let topic = topic.as_ref();
        self.ensure_topic(topic)?;
        let logs = self.topics.get(topic).unwrap();
        let partition_id = partition
            .filter(|&p| p >= 0 && (p as usize) < logs.len())
            .unwrap_or(0);
        let log = &logs[partition_id as usize];
        let offset = log.append(record)?;
        Ok((partition_id, offset))
    }

    /// Fetch: read from topic/partition from start_offset, up to max_bytes and max_records.
    pub fn fetch(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        max_bytes: usize,
        max_records: usize,
    ) -> Result<Vec<StoredRecord>> {
        let log = self.partition_log(topic, partition)?;
        let (pairs, _) = log.read_range(start_offset, max_records, max_bytes)?;
        Ok(pairs
            .into_iter()
            .map(|(offset, record)| StoredRecord::new(offset, partition, record))
            .collect())
    }

    /// Get high water mark for a partition (next offset to be assigned).
    pub fn high_water_mark(&self, topic: &str, partition: i32) -> Result<i64> {
        let log = self.partition_log(topic, partition)?;
        Ok(log.high_water_mark())
    }

    /// Get start offset (0 for our implementation).
    pub fn start_offset(&self, topic: &str, partition: i32) -> Result<i64> {
        let _ = self.partition_log(topic, partition)?;
        Ok(0)
    }

    /// List topic names.
    pub fn list_topics(&self) -> Vec<String> {
        self.topics.iter().map(|r| r.key().clone()).collect()
    }

    /// Number of partitions for a topic.
    pub fn num_partitions(&self, topic: &str) -> Result<i32> {
        let logs = self
            .topics
            .get(topic)
            .ok_or_else(|| ThorstreamError::TopicNotFound(topic.to_string()))?;
        Ok(logs.len() as i32)
    }

    /// Commit offset for consumer group (Kafka OffsetCommit).
    pub fn offset_commit(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        let _ = self.partition_log(topic, partition)?;
        self.offsets
            .insert((group_id.to_string(), topic.to_string(), partition), offset);
        Ok(())
    }

    /// Fetch committed offset (Kafka OffsetFetch).
    pub fn offset_fetch(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
    ) -> Result<i64> {
        let _ = self.partition_log(topic, partition)?;
        self.offsets
            .get(&(group_id.to_string(), topic.to_string(), partition))
            .map(|r| *r)
            .ok_or_else(|| ThorstreamError::ConsumerGroup("No committed offset".into()))
    }
}
