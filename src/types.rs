//! Core types shared across the streaming platform.

use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Record sent to a topic (Kafka-compatible concept).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    /// Optional key for partitioning.
    pub key: Option<Vec<u8>>,
    /// Payload.
    pub value: Vec<u8>,
    /// Optional headers (Kafka-style).
    #[serde(default)]
    pub headers: Vec<RecordHeader>,
    /// Timestamp (millis since epoch); set by broker if None.
    pub timestamp: Option<i64>,
    /// Producer id for idempotent / transactional producers.
    #[serde(default)]
    pub producer_id: Option<i64>,
    /// Monotonic producer sequence number (per partition).
    #[serde(default)]
    pub sequence: Option<i32>,
    /// Transaction identifier if record belongs to an open transaction.
    #[serde(default)]
    pub transaction_id: Option<String>,
}

/// Kafka-style record header.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RecordHeader {
    pub key: String,
    pub value: Vec<u8>,
}

impl Record {
    pub fn new(value: Vec<u8>) -> Self {
        Self {
            key: None,
            value,
            headers: Vec::new(),
            timestamp: None,
            producer_id: None,
            sequence: None,
            transaction_id: None,
        }
    }

    pub fn with_key(mut self, key: Vec<u8>) -> Self {
        self.key = Some(key);
        self
    }

    pub fn with_headers(mut self, headers: Vec<RecordHeader>) -> Self {
        self.headers = headers;
        self
    }

    pub fn with_producer(mut self, producer_id: i64, sequence: i32) -> Self {
        self.producer_id = Some(producer_id);
        self.sequence = Some(sequence);
        self
    }

    pub fn with_transaction(mut self, transaction_id: impl Into<String>) -> Self {
        self.transaction_id = Some(transaction_id.into());
        self
    }

    /// Set timestamp to now if not set (used by broker).
    pub fn ensure_timestamp(&mut self) {
        if self.timestamp.is_none() {
            self.timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .ok();
        }
    }
}

/// Stored record with assigned offset and partition (Kafka-compatible).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredRecord {
    pub offset: i64,
    pub partition: i32,
    pub record: Record,
}

impl StoredRecord {
    pub fn new(offset: i64, partition: i32, record: Record) -> Self {
        Self {
            offset,
            partition,
            record,
        }
    }
}
