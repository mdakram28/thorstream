//! Request/response codec (length-prefixed, api_key, bincode payload).

use crate::error::{Result, ThorstreamError};
use crate::types::{Record, StoredRecord};
use bytes::BytesMut;
use serde::Serialize;
use std::io::Cursor;

const API_METADATA: u16 = 0;
const API_PRODUCE: u16 = 1;
const API_FETCH: u16 = 2;
const API_OFFSET_COMMIT: u16 = 3;
const API_OFFSET_FETCH: u16 = 4;

/// Client request (Kafka-semantic).
#[derive(Debug, Clone)]
pub enum Request {
    Metadata { topics: Vec<String> },
    Produce {
        topic: String,
        partition: Option<i32>,
        records: Vec<Record>,
    },
    Fetch {
        topic: String,
        partition: i32,
        offset: i64,
        max_bytes: usize,
        max_records: usize,
    },
    OffsetCommit {
        group_id: String,
        topic: String,
        partition: i32,
        offset: i64,
    },
    OffsetFetch {
        group_id: String,
        topic: String,
        partition: i32,
    },
}

/// Server response.
#[derive(Debug, Clone)]
pub enum Response {
    Metadata(MetadataResponse),
    Produce(ProduceResponse),
    Fetch(FetchResponse),
    OffsetCommit,
    OffsetFetch(Option<i64>),
    Error(String),
}

#[derive(Debug, Clone, Serialize)]
pub struct MetadataResponse {
    pub topics: Vec<TopicMetadata>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicMetadata {
    pub name: String,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PartitionMetadata {
    pub partition_id: i32,
    pub leader_id: i32,
    pub replicas: Vec<i32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProduceResponse {
    pub topic: String,
    pub partition: i32,
    pub base_offset: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FetchResponse {
    pub topic: String,
    pub partition: i32,
    pub records: Vec<StoredRecord>,
    pub high_water_mark: i64,
}

/// Decode a single request from buffer. Consumes the frame; returns (Request, bytes_consumed).
pub fn decode_request(src: &mut BytesMut) -> Result<Option<Request>> {
    if src.len() < 6 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
    if src.len() < 4 + len {
        return Ok(None);
    }
    let api_key = u16::from_be_bytes([src[4], src[5]]);
    let frame = src.split_to(4 + len);
    let payload = &frame[6..];
    let request = match api_key {
        API_METADATA => {
            let topics: Vec<String> = bincode::deserialize_from(Cursor::new(payload))
                .map_err(|e| ThorstreamError::Protocol(e.to_string()))?;
            Request::Metadata { topics }
        }
        API_PRODUCE => {
            let (topic, partition, records): (String, Option<i32>, Vec<Record>) =
                bincode::deserialize_from(Cursor::new(payload))
                    .map_err(|e| ThorstreamError::Protocol(e.to_string()))?;
            Request::Produce {
                topic,
                partition,
                records,
            }
        }
        API_FETCH => {
            let (topic, partition, offset, max_bytes, max_records): (
                String,
                i32,
                i64,
                usize,
                usize,
            ) = bincode::deserialize_from(Cursor::new(payload))
                .map_err(|e| ThorstreamError::Protocol(e.to_string()))?;
            Request::Fetch {
                topic,
                partition,
                offset,
                max_bytes,
                max_records,
            }
        }
        API_OFFSET_COMMIT => {
            let (group_id, topic, partition, offset): (String, String, i32, i64) =
                bincode::deserialize_from(Cursor::new(payload))
                    .map_err(|e| ThorstreamError::Protocol(e.to_string()))?;
            Request::OffsetCommit {
                group_id,
                topic,
                partition,
                offset,
            }
        }
        API_OFFSET_FETCH => {
            let (group_id, topic, partition): (String, String, i32) =
                bincode::deserialize_from(Cursor::new(payload))
                    .map_err(|e| ThorstreamError::Protocol(e.to_string()))?;
            Request::OffsetFetch {
                group_id,
                topic,
                partition,
            }
        }
        _ => return Err(ThorstreamError::Protocol(format!("Unknown api_key {}", api_key))),
    };
    Ok(Some(request))
}

/// Encode response into dst.
pub fn encode_response(resp: &Response, dst: &mut BytesMut) -> Result<()> {
    let (api_key, payload): (u16, Vec<u8>) = match resp {
        Response::Metadata(m) => {
            let payload = bincode::serialize(m).map_err(|e| ThorstreamError::Protocol(e.to_string()))?;
            (API_METADATA, payload)
        }
        Response::Produce(p) => {
            let payload = bincode::serialize(p).map_err(|e| ThorstreamError::Protocol(e.to_string()))?;
            (API_PRODUCE, payload)
        }
        Response::Fetch(f) => {
            let payload = bincode::serialize(f).map_err(|e| ThorstreamError::Protocol(e.to_string()))?;
            (API_FETCH, payload)
        }
        Response::OffsetCommit => (API_OFFSET_COMMIT, vec![]),
        Response::OffsetFetch(opt) => {
            let payload =
                bincode::serialize(opt).map_err(|e| ThorstreamError::Protocol(e.to_string()))?;
            (API_OFFSET_FETCH, payload)
        }
        Response::Error(msg) => {
            let payload = bincode::serialize(msg).map_err(|e| ThorstreamError::Protocol(e.to_string()))?;
            (u16::MAX, payload) // error sentinel
        }
    };
    let frame_len = 2 + payload.len();
    dst.reserve(4 + frame_len);
    // length (4 BE) + api_key (2 BE) + payload
    dst.extend_from_slice(&(frame_len as u32).to_be_bytes());
    dst.extend_from_slice(&api_key.to_be_bytes());
    dst.extend_from_slice(&payload);
    Ok(())
}
