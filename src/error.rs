//! Error types for the streaming platform.

use thiserror::Error;

/// Result alias for broker operations.
pub type Result<T> = std::result::Result<T, ThorstreamError>;

/// Errors that can occur in the streaming platform.
#[derive(Error, Debug)]
pub enum ThorstreamError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    #[error("Partition not found: {topic}/{partition}")]
    PartitionNotFound { topic: String, partition: i32 },

    #[error("Invalid offset: {0}")]
    InvalidOffset(i64),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Consumer group error: {0}")]
    ConsumerGroup(String),
}
