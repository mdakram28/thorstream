//! Broker and topic management.

use crate::error::{Result, ThorstreamError};
use crate::storage::{PartitionLog, PartitionLogConfig};
use crate::types::{Record, StoredRecord};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CleanupPolicy {
    Delete,
    Compact,
}

/// Per-topic configuration (Kafka-compatible: partitions, replication, etc.).
#[derive(Clone, Debug)]
pub struct TopicConfig {
    /// Number of partitions.
    pub num_partitions: i32,
    /// Replication factor (ignored in single-node; for API compatibility).
    pub replication_factor: i16,
    /// Minimum in-sync replicas required for acked produce.
    pub min_insync_replicas: i16,
    /// Time retention (ms).
    pub retention_ms: Option<u64>,
    /// Size retention (bytes).
    pub retention_bytes: Option<u64>,
    /// Cleanup policy.
    pub cleanup_policy: CleanupPolicy,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            num_partitions: 1,
            replication_factor: 1,
            min_insync_replicas: 1,
            retention_ms: None,
            retention_bytes: None,
            cleanup_policy: CleanupPolicy::Delete,
        }
    }
}

#[derive(Clone, Debug)]
struct TxnBufferRecord {
    topic: String,
    partition: Option<i32>,
    record: Record,
}

/// Broker-wide configuration.
#[derive(Clone, Debug)]
pub struct BrokerConfig {
    pub data_dir: std::path::PathBuf,
    pub default_topic_config: TopicConfig,
    pub node_id: i32,
    pub peers: HashMap<i32, String>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            data_dir: std::path::PathBuf::from("data"),
            default_topic_config: TopicConfig::default(),
            node_id: 0,
            peers: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClusterStatus {
    pub node_id: i32,
    pub leader_id: Option<i32>,
    pub term: i64,
    pub peers: HashMap<i32, String>,
}

/// Central broker: creates topics, routes produce/fetch to partition logs.
pub struct Broker {
    config: BrokerConfig,
    /// topic_name -> (partition_id -> PartitionLog)
    topics: DashMap<String, Vec<Arc<PartitionLog>>>,
    /// (topic, partition) -> all replicas logs (leader first).
    replicas: DashMap<(String, i32), Vec<Arc<PartitionLog>>>,
    /// topic_name -> topic config (including replication_factor).
    topic_configs: DashMap<String, TopicConfig>,
    /// Committed offsets: (group_id, topic, partition) -> offset.
    offsets: DashMap<(String, String, i32), i64>,
    producer_seq: DashMap<(String, i32, i64), i32>,
    producer_last_offset: DashMap<(String, i32, i64), i64>,
    transactions: DashMap<String, Vec<TxnBufferRecord>>,
    tx_producer: DashMap<String, i64>,
    leader_id: RwLock<Option<i32>>,
    term: RwLock<i64>,
}

impl Broker {
    pub fn new(config: BrokerConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)
            .map_err(|e| ThorstreamError::Storage(e.to_string()))?;
        let node_id = config.node_id;
        let broker = Self {
            config,
            topics: DashMap::new(),
            replicas: DashMap::new(),
            topic_configs: DashMap::new(),
            offsets: DashMap::new(),
            producer_seq: DashMap::new(),
            producer_last_offset: DashMap::new(),
            transactions: DashMap::new(),
            tx_producer: DashMap::new(),
            leader_id: RwLock::new(Some(node_id)),
            term: RwLock::new(0),
        };
        broker.recover_topics_from_disk()?;
        Ok(broker)
    }

    fn recover_topics_from_disk(&self) -> Result<()> {
        let root = self.config.data_dir.clone();
        let entries = match std::fs::read_dir(&root) {
            Ok(x) => x,
            Err(_) => return Ok(()),
        };

        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let file_type = match entry.file_type() {
                Ok(ft) => ft,
                Err(_) => continue,
            };
            if !file_type.is_dir() {
                continue;
            }

            let topic = entry.file_name().to_string_lossy().to_string();
            if topic.contains("__replica") {
                continue;
            }

            let mut max_partition: i32 = -1;
            let part_entries = match std::fs::read_dir(entry.path()) {
                Ok(x) => x,
                Err(_) => continue,
            };
            for part_entry in part_entries {
                let part_entry = match part_entry {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                let pft = match part_entry.file_type() {
                    Ok(ft) => ft,
                    Err(_) => continue,
                };
                if !pft.is_dir() {
                    continue;
                }
                if let Ok(partition) = part_entry.file_name().to_string_lossy().parse::<i32>() {
                    if partition > max_partition {
                        max_partition = partition;
                    }
                }
            }

            if max_partition < 0 {
                continue;
            }

            let mut rf: i16 = 1;
            loop {
                let replica_topic = format!("{}__replica{}", topic, rf);
                if root.join(&replica_topic).is_dir() {
                    rf += 1;
                } else {
                    break;
                }
            }

            self.create_topic(
                &topic,
                Some(TopicConfig {
                    num_partitions: max_partition + 1,
                    replication_factor: rf,
                    ..TopicConfig::default()
                }),
            )?;
        }
        Ok(())
    }

    /// Create a topic with given config. Idempotent: if topic exists, ensure partition count.
    pub fn create_topic(&self, name: impl AsRef<str>, config: Option<TopicConfig>) -> Result<()> {
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
            if let Some(existing_cfg) = self.topic_configs.get(&name) {
                if existing_cfg.replication_factor != config.replication_factor {
                    return Err(ThorstreamError::Storage(format!(
                        "Topic {} exists with replication_factor {}, cannot change to {}",
                        name, existing_cfg.replication_factor, config.replication_factor
                    )));
                }
            }
            return Ok(());
        }
        let log_config = PartitionLogConfig {
            data_dir: self.config.data_dir.clone(),
            max_segment_size_bytes: 1024 * 1024 * 1024,
            retention_ms: config.retention_ms,
            retention_bytes: config.retention_bytes,
            compact: matches!(config.cleanup_policy, CleanupPolicy::Compact),
        };
        let mut logs = Vec::with_capacity(config.num_partitions as usize);
        for p in 0..config.num_partitions {
            let log = PartitionLog::open(&name, p, log_config.clone())?;
            let leader = Arc::new(log);
            logs.push(Arc::clone(&leader));

            let mut partition_replicas =
                Vec::with_capacity(config.replication_factor.max(1) as usize);
            partition_replicas.push(Arc::clone(&leader));
            for replica_id in 1..config.replication_factor.max(1) {
                let replica_topic = format!("{}__replica{}", name, replica_id);
                let replica_log = PartitionLog::open(&replica_topic, p, log_config.clone())?;
                partition_replicas.push(Arc::new(replica_log));
            }
            self.replicas.insert((name.clone(), p), partition_replicas);
        }
        self.topics.insert(name.clone(), logs);
        self.topic_configs.insert(name, config);
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

        let replicas = self
            .replicas
            .get(&(topic.to_string(), partition_id))
            .ok_or_else(|| {
                ThorstreamError::Storage(format!(
                    "Replica state missing for topic={}, partition={}",
                    topic, partition_id
                ))
            })?;

        let isr = self.in_sync_replicas(topic, partition_id)?;
        let min_isr = self
            .topic_configs
            .get(topic)
            .map(|c| c.min_insync_replicas)
            .unwrap_or(1)
            .max(1) as usize;
        if isr.len() < min_isr {
            return Err(ThorstreamError::Cluster(format!(
                "insufficient ISR for topic={}, partition={}, isr={}, min_isr={}",
                topic,
                partition_id,
                isr.len(),
                min_isr
            )));
        }

        let mut offset: Option<i64> = None;
        for replica_log in replicas.iter() {
            let assigned = replica_log.append(record.clone())?;
            if let Some(expected) = offset {
                if expected != assigned {
                    return Err(ThorstreamError::Storage(format!(
                        "Replica offset mismatch for topic={}, partition={}: leader={}, replica={}",
                        topic, partition_id, expected, assigned
                    )));
                }
            } else {
                offset = Some(assigned);
            }
        }

        Ok((partition_id, offset.unwrap_or(0)))
    }

    pub fn begin_transaction(&self, transaction_id: impl Into<String>, producer_id: i64) {
        let tx = transaction_id.into();
        self.transactions.entry(tx.clone()).or_default();
        self.tx_producer.insert(tx, producer_id);
    }

    pub fn produce_transactional(
        &self,
        transaction_id: &str,
        topic: impl AsRef<str>,
        partition: Option<i32>,
        record: Record,
    ) -> Result<()> {
        if !self.transactions.contains_key(transaction_id) {
            return Err(ThorstreamError::Protocol(format!(
                "transaction {} not started",
                transaction_id
            )));
        }
        if let Some(mut rows) = self.transactions.get_mut(transaction_id) {
            rows.push(TxnBufferRecord {
                topic: topic.as_ref().to_string(),
                partition,
                record,
            });
        }
        Ok(())
    }

    pub fn commit_transaction(&self, transaction_id: &str) -> Result<Vec<(String, i32, i64)>> {
        let rows = self
            .transactions
            .remove(transaction_id)
            .map(|(_, v)| v)
            .unwrap_or_default();
        self.tx_producer.remove(transaction_id);
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let topic = row.topic.clone();
            let (partition, offset) = self.produce(&topic, row.partition, row.record)?;
            out.push((topic, partition, offset));
        }
        Ok(out)
    }

    pub fn abort_transaction(&self, transaction_id: &str) {
        self.transactions.remove(transaction_id);
        self.tx_producer.remove(transaction_id);
    }

    pub fn produce_idempotent(
        &self,
        topic: impl AsRef<str>,
        partition: Option<i32>,
        mut record: Record,
        producer_id: i64,
        sequence: i32,
    ) -> Result<(i32, i64)> {
        let topic_name = topic.as_ref().to_string();
        self.ensure_topic(&topic_name)?;
        let logs = self.topics.get(&topic_name).unwrap();
        let partition_id = partition
            .filter(|&p| p >= 0 && (p as usize) < logs.len())
            .unwrap_or(0);
        let key = (topic_name.clone(), partition_id, producer_id);

        if let Some(last_seq) = self.producer_seq.get(&key) {
            if sequence < *last_seq {
                return Err(ThorstreamError::Protocol(format!(
                    "out of order sequence: got {} expected >= {}",
                    sequence, *last_seq
                )));
            }
            if sequence == *last_seq {
                let off = self.producer_last_offset.get(&key).map(|v| *v).unwrap_or(0);
                return Ok((partition_id, off));
            }
            if sequence != *last_seq + 1 {
                return Err(ThorstreamError::Protocol(format!(
                    "sequence gap: got {} expected {}",
                    sequence,
                    *last_seq + 1
                )));
            }
        }

        record.producer_id = Some(producer_id);
        record.sequence = Some(sequence);
        let (p, off) = self.produce(&topic_name, Some(partition_id), record)?;
        self.producer_seq.insert(key.clone(), sequence);
        self.producer_last_offset.insert(key, off);
        Ok((p, off))
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
        let isr = self.in_sync_replicas(topic, partition)?;
        if isr.is_empty() {
            return Ok(0);
        }
        Ok(*isr.iter().min().unwrap_or(&0))
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

    /// Replication factor for a topic.
    pub fn replication_factor(&self, topic: &str) -> Result<i16> {
        self.topic_configs
            .get(topic)
            .map(|c| c.replication_factor)
            .ok_or_else(|| ThorstreamError::TopicNotFound(topic.to_string()))
    }

    /// Replica HWMs for a partition (leader first).
    pub fn replica_high_water_marks(&self, topic: &str, partition: i32) -> Result<Vec<i64>> {
        let replicas = self
            .replicas
            .get(&(topic.to_string(), partition))
            .ok_or_else(|| ThorstreamError::PartitionNotFound {
                topic: topic.to_string(),
                partition,
            })?;
        Ok(replicas.iter().map(|log| log.high_water_mark()).collect())
    }

    pub fn in_sync_replicas(&self, topic: &str, partition: i32) -> Result<Vec<i64>> {
        let replica_hwms = self.replica_high_water_marks(topic, partition)?;
        if replica_hwms.is_empty() {
            return Ok(Vec::new());
        }
        let leader_hwm = replica_hwms[0];
        Ok(replica_hwms
            .into_iter()
            .filter(|hwm| *hwm >= leader_hwm)
            .collect())
    }

    pub fn under_replicated_partitions(&self) -> usize {
        self.topics
            .iter()
            .map(|entry| {
                let topic = entry.key().clone();
                let cfg = self
                    .topic_configs
                    .get(&topic)
                    .map(|c| c.replication_factor)
                    .unwrap_or(1)
                    .max(1) as usize;
                let mut urp = 0usize;
                for partition in 0..entry.value().len() as i32 {
                    let isr = self.in_sync_replicas(&topic, partition).unwrap_or_default();
                    if isr.len() < cfg {
                        urp += 1;
                    }
                }
                urp
            })
            .sum()
    }

    pub fn partition_sizes(&self) -> Vec<(String, i32, u64)> {
        let mut out = Vec::new();
        for entry in self.topics.iter() {
            let topic = entry.key().clone();
            for (partition, log) in entry.value().iter().enumerate() {
                let size = log.size_bytes().unwrap_or(0);
                out.push((topic.clone(), partition as i32, size));
            }
        }
        out
    }

    pub fn consumer_lag_metrics(&self) -> Vec<(String, String, i32, i64)> {
        let mut out = Vec::new();
        for item in self.offsets.iter() {
            let (group, topic, partition) = item.key();
            let committed = *item.value();
            let hw = self.high_water_mark(topic, *partition).unwrap_or(0);
            let lag = (hw - committed).max(0);
            out.push((group.clone(), topic.clone(), *partition, lag));
        }
        out
    }

    pub fn cluster_status(&self) -> ClusterStatus {
        ClusterStatus {
            node_id: self.config.node_id,
            leader_id: *self.leader_id.read(),
            term: *self.term.read(),
            peers: self.config.peers.clone(),
        }
    }

    pub fn set_leader(&self, leader_id: Option<i32>, term: i64) {
        let mut l = self.leader_id.write();
        *l = leader_id;
        let mut t = self.term.write();
        if term >= *t {
            *t = term;
        }
    }

    pub fn leader_id(&self) -> Option<i32> {
        *self.leader_id.read()
    }

    pub fn is_leader(&self) -> bool {
        self.leader_id() == Some(self.config.node_id)
    }

    pub fn leader_addr(&self) -> Option<String> {
        let leader_id = self.leader_id()?;
        if leader_id == self.config.node_id {
            return None;
        }
        self.config.peers.get(&leader_id).cloned()
    }

    pub fn apply_replication(
        &self,
        topic: &str,
        partition: i32,
        record: Record,
        expected_offset: i64,
    ) -> Result<i64> {
        let (_, offset) = self.produce(topic, Some(partition), record)?;
        if offset != expected_offset {
            return Err(ThorstreamError::Cluster(format!(
                "replication offset mismatch expected={} got={}",
                expected_offset, offset
            )));
        }
        Ok(offset)
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
    pub fn offset_fetch(&self, group_id: &str, topic: &str, partition: i32) -> Result<i64> {
        let _ = self.partition_log(topic, partition)?;
        self.offsets
            .get(&(group_id.to_string(), topic.to_string(), partition))
            .map(|r| *r)
            .ok_or_else(|| ThorstreamError::ConsumerGroup("No committed offset".into()))
    }
}
