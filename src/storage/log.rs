//! Partition log: append-only log over one or more segments.

use super::object_store;
use super::Segment;
use crate::error::{Result, ThorstreamError};
use crate::types::Record;
use std::collections::HashMap;
use std::sync::Arc;

/// Config for partition log (segment rollover, etc.).
#[derive(Clone, Debug)]
pub struct PartitionLogConfig {
    /// Max segment size in bytes before rolling to a new segment (0 = no limit).
    pub max_segment_size_bytes: u64,
    /// Data directory for segment files.
    pub data_dir: std::path::PathBuf,
    /// Retention time in milliseconds (delete policy).
    pub retention_ms: Option<u64>,
    /// Retention size in bytes (delete policy).
    pub retention_bytes: Option<u64>,
    /// Enable log compaction by key.
    pub compact: bool,
}

impl Default for PartitionLogConfig {
    fn default() -> Self {
        Self {
            max_segment_size_bytes: 1024 * 1024 * 1024, // 1GB
            data_dir: std::path::PathBuf::from("data"),
            retention_ms: None,
            retention_bytes: None,
            compact: false,
        }
    }
}

/// Log for a single partition: currently one active segment, append and read by offset.
#[allow(dead_code)]
pub struct PartitionLog {
    topic: String,
    partition: i32,
    config: PartitionLogConfig,
    /// Active segment (append + read).
    active_segment: parking_lot::RwLock<Arc<Segment>>,
    /// Loaded segments for read (base_offset -> segment); active is also here.
    segments: dashmap::DashMap<i64, Arc<Segment>>,
}

impl PartitionLog {
    pub fn open(
        topic: impl Into<String>,
        partition: i32,
        config: PartitionLogConfig,
    ) -> Result<Self> {
        let topic = topic.into();
        let segment_dir = config.data_dir.join(&topic).join(partition.to_string());
        let segment_path = segment_dir.join("00000000000000000000.log");
        object_store::restore_segment_if_needed(&topic, partition, &segment_path)?;
        let segment = Segment::open(segment_path, 0)?;
        let segment = Arc::new(segment);
        let base = segment.base_offset();
        let segments = dashmap::DashMap::new();
        segments.insert(base, Arc::clone(&segment));
        Ok(Self {
            topic: topic.clone(),
            partition,
            config,
            active_segment: parking_lot::RwLock::new(segment),
            segments,
        })
    }

    /// Append a record; returns assigned offset.
    pub fn append(&self, mut record: Record) -> Result<i64> {
        record.ensure_timestamp();
        let seg = self.active_segment.read();
        let offset = seg.append(&record)?;
        object_store::mirror_append(&self.topic, self.partition, &record)?;
        self.apply_retention_and_compaction()?;
        Ok(offset)
    }

    /// Read a single record at offset.
    pub fn read_at(&self, offset: i64) -> Result<Option<Record>> {
        let seg = self.segment_for_offset(offset)?;
        seg.read_at(offset)
    }

    /// Read from start_offset (inclusive), up to max_records and max_bytes.
    /// Returns (stored records as (offset, record), next_offset).
    pub fn read_range(
        &self,
        start_offset: i64,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<(Vec<(i64, Record)>, i64)> {
        let seg = self.segment_for_offset(start_offset)?;
        seg.read_range(start_offset, max_records, max_bytes)
    }

    /// High water mark: next offset to be assigned (equal to number of records).
    pub fn high_water_mark(&self) -> i64 {
        self.active_segment.read().next_offset()
    }

    /// Start offset (0 for new log).
    pub fn start_offset(&self) -> i64 {
        0
    }

    fn segment_for_offset(&self, offset: i64) -> Result<Arc<Segment>> {
        // For single-segment implementation, always use active.
        let seg = self.active_segment.read();
        if offset < seg.base_offset() {
            return Err(ThorstreamError::InvalidOffset(offset));
        }
        // offset == next_offset is valid (no new records); only strictly past is invalid
        if offset > seg.next_offset() {
            return Err(ThorstreamError::InvalidOffset(offset));
        }
        Ok(Arc::clone(&seg))
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn size_bytes(&self) -> Result<u64> {
        self.active_segment.read().file_size_bytes()
    }

    fn apply_retention_and_compaction(&self) -> Result<()> {
        let seg = self.active_segment.read();
        let mut rows = seg.read_all()?;
        if rows.is_empty() {
            return Ok(());
        }

        if let Some(ms) = self.config.retention_ms {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let min_ts = now - ms as i64;
            rows.retain(|(_, record)| record.timestamp.unwrap_or(now) >= min_ts);
        }

        if let Some(max_bytes) = self.config.retention_bytes {
            while seg.file_size_bytes()? > max_bytes && rows.len() > 1 {
                rows.remove(0);
                seg.rewrite_all(&rows)?;
            }
        }

        if self.config.compact {
            let mut latest_by_key: HashMap<Vec<u8>, usize> = HashMap::new();
            for (idx, (_, record)) in rows.iter().enumerate() {
                if let Some(key) = &record.key {
                    latest_by_key.insert(key.clone(), idx);
                }
            }
            let compacted: Vec<(i64, Record)> = rows
                .into_iter()
                .enumerate()
                .filter_map(|(idx, row)| {
                    let keep = if let Some(key) = &row.1.key {
                        latest_by_key.get(key).copied() == Some(idx)
                    } else {
                        true
                    };
                    if keep {
                        Some(row)
                    } else {
                        None
                    }
                })
                .collect();
            seg.rewrite_all(&compacted)?;
            return Ok(());
        }

        seg.rewrite_all(&rows)?;
        Ok(())
    }
}
