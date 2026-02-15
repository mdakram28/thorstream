//! Wire protocol for producer/consumer (Kafka-semantic compatible).
//!
//! Custom format: length (4 bytes BE) | api_key (2 bytes BE) | payload (bytes).
//! Kafka format: see kafka.rs for standard Kafka binary protocol.

mod codec;
mod kafka;

pub use codec::{
    decode_request, encode_request, encode_response, FetchResponse, MetadataResponse,
    PartitionMetadata, ProduceResponse, Request, Response, TopicMetadata,
};
pub use kafka::{
    build_minimal_error_response, decode_kafka_request, handle_kafka_request, kafka_frame_response,
};
