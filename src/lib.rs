//! Thorstream: fast Kafka-like append-only event streaming with Kafka consumer compatibility.

pub mod broker;
pub mod cluster;
pub mod compat;
pub mod consumer;
pub mod error;
pub mod observability;
pub mod producer;
pub mod protocol;
pub mod security;
pub mod server;
pub mod storage;
pub mod streams_shim;
pub mod types;

pub use broker::{Broker, BrokerConfig, CleanupPolicy, TopicConfig};
pub use consumer::{Consumer, TopicPartition};
pub use error::{Result, ThorstreamError};
pub use producer::{ProduceResult, Producer};
pub use protocol::{Request, Response};
pub use types::{Record, RecordHeader, StoredRecord};
