//! Minimal Kafka binary wire protocol for standard client compatibility.
//!
//! Frame: int32 (BE) length + body.
//! Request: header (api_key, api_version, correlation_id, client_id) + body.
//! Supports: ApiVersions (18), Metadata (3), Produce (0), Fetch (1).

use crate::broker::Broker;
use crate::cluster;
use crate::error::{Result, ThorstreamError};
use crate::types::Record;
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

const API_PRODUCE: i16 = 0;
const API_FETCH: i16 = 1;
const API_LIST_OFFSETS: i16 = 2;
const API_METADATA: i16 = 3;
const API_API_VERSIONS: i16 = 18;

const RECORD_BATCH_MAGIC: i8 = 2;

type DecodedKafkaRequest = (i16, i16, i32, Cursor<Vec<u8>>);
type FetchResponseData = (String, i32, Vec<(i64, Record)>, i64);

/// Read Kafka request header (after frame length): api_key, api_version, correlation_id, client_id.
fn read_request_header(src: &mut Cursor<Vec<u8>>) -> Result<(i16, i16, i32, Option<String>)> {
    if src.remaining() < 8 {
        return Err(ThorstreamError::Protocol("short request header".into()));
    }
    let api_key = src.get_i16();
    let api_version = src.get_i16();
    let correlation_id = src.get_i32();
    let client_id_len = src.get_i16();
    let client_id = if client_id_len < 0 {
        None
    } else {
        let len = client_id_len as usize;
        if src.remaining() < len {
            return Err(ThorstreamError::Protocol("short client_id".into()));
        }
        let mut buf = vec![0u8; len];
        src.copy_to_slice(&mut buf);
        Some(String::from_utf8(buf).unwrap_or_else(|_| "?".into()))
    };
    Ok((api_key, api_version, correlation_id, client_id))
}

/// Write response header (Kafka v0): correlation_id, no tagged fields.
fn write_response_header(dst: &mut BytesMut, correlation_id: i32) {
    dst.put_i32(correlation_id);
}

/// Read nullable Kafka string: int16 length (-1 = null), then bytes.
fn read_string(src: &mut Cursor<Vec<u8>>) -> Result<Option<String>> {
    if src.remaining() < 2 {
        return Err(ThorstreamError::Protocol("short string len".into()));
    }
    let len = src.get_i16();
    if len < 0 {
        return Ok(None);
    }
    let n = len as usize;
    if src.remaining() < n {
        return Err(ThorstreamError::Protocol("short string".into()));
    }
    let mut b = vec![0u8; n];
    src.copy_to_slice(&mut b);
    Ok(Some(String::from_utf8(b).unwrap_or_else(|_| "?".into())))
}

fn write_string(dst: &mut BytesMut, s: &str) {
    let b = s.as_bytes();
    dst.put_i16(b.len() as i16);
    dst.extend_from_slice(b);
}

/// Parse RecordBatch (magic 2), extract key/value records. Returns (base_offset, records).
fn parse_record_batch(mut payload: &[u8]) -> Result<(i64, Vec<Record>)> {
    if payload.len() < 61 {
        return Ok((0, Vec::new()));
    }
    let base_offset = payload.get_i64();
    let _batch_len = payload.get_i32();
    let _partition_leader_epoch = payload.get_i32();
    let magic = payload.get_i8();
    if magic != RECORD_BATCH_MAGIC {
        return Err(ThorstreamError::Protocol(format!(
            "unsupported batch magic {}",
            magic
        )));
    }
    let _crc = payload.get_u32();
    let _attributes = payload.get_i16();
    let _last_offset_delta = payload.get_i32();
    let _base_timestamp = payload.get_i64();
    let _max_timestamp = payload.get_i64();
    let _producer_id = payload.get_i64();
    let _producer_epoch = payload.get_i16();
    let _base_sequence = payload.get_i32();
    let record_count = payload.get_i32();
    let mut records = Vec::with_capacity(record_count.max(0) as usize);
    for _ in 0..record_count {
        if payload.len() < 5 {
            break;
        }
        let rec = parse_record(&mut payload)?;
        records.push(rec);
    }
    Ok((base_offset, records))
}

/// Parse one record in RecordBatch (length-prefixed, then attributes, timestampDelta, offsetDelta, key, value, headers).
fn parse_record(payload: &mut &[u8]) -> Result<Record> {
    let len = read_varint(payload)?;
    let start = payload.len();
    let _attributes = payload.get_i8();
    let _ts_delta = read_varlong(payload)?;
    let _offset_delta = read_varint(payload)?;
    let key = read_varint_bytes(payload)?;
    let value = read_varint_bytes(payload)?;
    let _num_headers = read_varint(payload)?;
    let consumed = start - payload.len();
    if consumed > len as usize {
        return Err(ThorstreamError::Protocol("record overflow".into()));
    }
    Ok(Record {
        key: if key.is_empty() { None } else { Some(key) },
        value,
        headers: Vec::new(),
        timestamp: None,
    })
}

fn read_varint(src: &mut &[u8]) -> Result<i32> {
    let mut v: i64 = 0;
    let mut shift = 0;
    loop {
        if src.is_empty() {
            return Err(ThorstreamError::Protocol("varint eof".into()));
        }
        let b = src.get_u8();
        v |= ((b & 0x7f) as i64) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 35 {
            return Err(ThorstreamError::Protocol("varint too long".into()));
        }
    }
    Ok(((v >> 1) ^ -(v & 1)) as i32)
}

fn read_varlong(src: &mut &[u8]) -> Result<i64> {
    let mut v: u64 = 0;
    let mut shift = 0u32;
    loop {
        if src.is_empty() {
            return Err(ThorstreamError::Protocol("varlong eof".into()));
        }
        let b = src.get_u8();
        v |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return Err(ThorstreamError::Protocol("varlong too long".into()));
        }
    }
    Ok(((v >> 1) as i64) ^ (-(v as i64 & 1)))
}

fn read_varint_bytes(src: &mut &[u8]) -> Result<Vec<u8>> {
    let len = read_varint(src)?;
    if len <= 0 {
        return Ok(Vec::new());
    }
    let n = len as usize;
    if src.len() < n {
        return Err(ThorstreamError::Protocol("short bytes".into()));
    }
    let out = src[..n].to_vec();
    src.advance(n);
    Ok(out)
}

/// Build a minimal RecordBatch (magic 2) from stored records for Fetch response.
fn build_record_batch(base_offset: i64, records: &[(i64, &Record)]) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_i64(base_offset);
    let batch_body_start = buf.len() + 4;
    buf.put_i32(0); // placeholder batch length
    buf.put_i32(0); // partition_leader_epoch
    buf.put_i8(RECORD_BATCH_MAGIC);
    buf.put_u32(0); // crc (we skip verification for simplicity)
    buf.put_i16(0); // attributes
    let last_delta = records.last().map(|(o, _)| *o - base_offset).unwrap_or(0);
    buf.put_i32(last_delta as i32);
    buf.put_i64(0);
    buf.put_i64(0);
    buf.put_i64(-1);
    buf.put_i16(0);
    buf.put_i32(-1);
    buf.put_i32(records.len() as i32);
    for (offset, record) in records {
        let rec_buf = build_record(*offset - base_offset, record);
        put_varint(&mut buf, rec_buf.len() as i32);
        buf.extend_from_slice(&rec_buf);
    }
    let batch_len = buf.len() - batch_body_start;
    buf[batch_body_start - 4..batch_body_start].copy_from_slice(&(batch_len as i32).to_be_bytes());
    buf.to_vec()
}

fn build_record(offset_delta: i64, record: &Record) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_i8(0);
    put_varlong(&mut buf, 0);
    put_varint(&mut buf, offset_delta as i32);
    let key = record.key.as_deref().unwrap_or(&[]);
    put_varint_bytes(&mut buf, key);
    put_varint_bytes(&mut buf, &record.value);
    put_varint(&mut buf, 0);
    buf.to_vec()
}

fn put_varint(dst: &mut BytesMut, v: i32) {
    let u = ((v << 1) ^ (v >> 31)) as u32;
    put_uvariant(dst, u as u64);
}

fn put_varlong(dst: &mut BytesMut, v: i64) {
    let u = ((v << 1) ^ (v >> 63)) as u64;
    put_uvariant(dst, u);
}

fn put_uvariant(dst: &mut BytesMut, mut u: u64) {
    while u > 0x7f {
        dst.put_u8((u as u8) | 0x80);
        u >>= 7;
    }
    dst.put_u8(u as u8);
}

fn put_varint_bytes(dst: &mut BytesMut, b: &[u8]) {
    put_varint(dst, b.len() as i32);
    dst.extend_from_slice(b);
}

/// Kafka unsigned varint (for CompactArray length, TaggedFields count).
fn put_unsigned_varint(dst: &mut BytesMut, mut u: u32) {
    while u > 0x7f {
        dst.put_u8((u as u8) | 0x80);
        u >>= 7;
    }
    dst.put_u8(u as u8);
}

/// Write compact string (varint length+1, then utf8 bytes). Null is 0.
fn put_compact_string(dst: &mut BytesMut, s: &str) {
    let bytes = s.as_bytes();
    put_unsigned_varint(dst, (bytes.len() + 1) as u32);
    dst.extend_from_slice(bytes);
}

/// Decode one Kafka request from buffer. Returns (api_key, api_version, correlation_id, body_cursor) or None if incomplete.
pub fn decode_kafka_request(src: &mut BytesMut) -> Result<Option<DecodedKafkaRequest>> {
    if src.len() < 4 {
        return Ok(None);
    }
    let len = i32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
    if len == 0 || len > 100_000_000 {
        return Err(ThorstreamError::Protocol("invalid frame size".into()));
    }
    if src.len() < 4 + len {
        return Ok(None);
    }
    src.advance(4);
    let body = src.split_to(len).to_vec();
    let mut cur = Cursor::new(body.clone());
    let (api_key, api_version, correlation_id, _client_id) = read_request_header(&mut cur)?;
    let body_start = cur.position() as usize;
    let body_cursor = Cursor::new(body[body_start..].to_vec());
    Ok(Some((api_key, api_version, correlation_id, body_cursor)))
}

/// Handle Kafka request and write response into dst.
pub fn handle_kafka_request(
    broker: &Broker,
    api_key: i16,
    version: i16,
    correlation_id: i32,
    mut body: Cursor<Vec<u8>>,
) -> Result<BytesMut> {
    let mut dst = BytesMut::new();
    match api_key {
        API_API_VERSIONS => {
            write_response_header(&mut dst, correlation_id);
            dst.put_i16(0); // error_code
            let apis: &[(i16, i16, i16)] = &[
                (API_PRODUCE, 0, 5),
                (API_FETCH, 0, 5),
                (API_LIST_OFFSETS, 0, 2),
                (API_METADATA, 0, 8),
                (API_API_VERSIONS, 0, 3),
            ];
            if version >= 3 {
                // v3/v4: CompactArray (length as unsigned varint: N+1), then each entry + TaggedFields(0), then throttle_time_ms, then TaggedFields(0)
                put_unsigned_varint(&mut dst, (apis.len() + 1) as u32);
                for (key, min_v, max_v) in apis {
                    dst.put_i16(*key);
                    dst.put_i16(*min_v);
                    dst.put_i16(*max_v);
                    put_unsigned_varint(&mut dst, 0); // tagged fields count
                }
                dst.put_i32(0); // throttle_time_ms
                put_unsigned_varint(&mut dst, 0); // tagged fields
            } else {
                // v0: int32 count, entries. v1/v2: + throttle_time_ms
                dst.put_i32(apis.len() as i32);
                for (key, min_v, max_v) in apis {
                    dst.put_i16(*key);
                    dst.put_i16(*min_v);
                    dst.put_i16(*max_v);
                }
                if version >= 1 {
                    dst.put_i32(0); // throttle_time_ms
                }
            }
        }
        API_METADATA => {
            let requested_topics = read_metadata_request(&mut body, version)?;
            // If no topics requested (or null), return all topics
            let topics: Vec<String> = if requested_topics.is_empty() {
                broker.list_topics()
            } else {
                // Auto-create requested topics that don't exist (Kafka default behavior)
                for topic in &requested_topics {
                    let _ = broker.ensure_topic(topic);
                }
                requested_topics
            };
            write_response_header(&mut dst, correlation_id);

            // Version 9+ uses compact format
            let compact = version >= 9;

            if version >= 3 {
                // v3+: throttle_time_ms at start
                dst.put_i32(0);
            }

            let port: i32 = std::env::var("THORSTREAM_KAFKA_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(9093);

            // Brokers array
            if compact {
                put_unsigned_varint(&mut dst, 2); // compact array: count+1
            } else {
                dst.put_i32(1); // broker count
            }
            dst.put_i32(0); // node_id
            if compact {
                put_compact_string(&mut dst, "127.0.0.1");
            } else {
                write_string(&mut dst, "127.0.0.1");
            }
            dst.put_i32(port);
            if version >= 1 {
                // v1+: rack (nullable string)
                if compact {
                    put_unsigned_varint(&mut dst, 0); // null = length 0
                } else {
                    dst.put_i16(-1); // null rack
                }
            }
            if compact {
                put_unsigned_varint(&mut dst, 0); // tagged fields
            }

            if version >= 2 {
                // v2+: cluster_id (nullable string)
                if compact {
                    put_unsigned_varint(&mut dst, 0); // null = length 0
                } else {
                    dst.put_i16(-1); // null cluster_id
                }
            }
            if version >= 1 {
                // v1+: controller_id
                dst.put_i32(0);
            }

            // Topics array
            if compact {
                put_unsigned_varint(&mut dst, (topics.len() + 1) as u32);
            } else {
                dst.put_i32(topics.len() as i32);
            }

            for name in topics {
                let n = broker.num_partitions(&name).unwrap_or(0);
                dst.put_i16(0); // error_code
                if compact {
                    put_compact_string(&mut dst, &name);
                } else {
                    write_string(&mut dst, &name);
                }
                if version >= 1 {
                    // v1+: is_internal
                    dst.put_i8(0);
                }

                // Partitions array
                if compact {
                    put_unsigned_varint(&mut dst, (n + 1) as u32);
                } else {
                    dst.put_i32(n); // partition count
                }

                for p in 0..n {
                    dst.put_i16(0); // error_code
                    dst.put_i32(p); // partition
                    dst.put_i32(0); // leader
                    if version >= 7 {
                        // v7+: leader_epoch
                        dst.put_i32(-1);
                    }
                    // Replicas array
                    if compact {
                        put_unsigned_varint(&mut dst, 2); // count+1
                    } else {
                        dst.put_i32(1); // replicas count
                    }
                    dst.put_i32(0); // replica

                    // ISR array
                    if compact {
                        put_unsigned_varint(&mut dst, 2); // count+1
                    } else {
                        dst.put_i32(1); // isr count
                    }
                    dst.put_i32(0); // isr

                    if version >= 5 {
                        // v5+: offline_replicas
                        if compact {
                            put_unsigned_varint(&mut dst, 1); // empty array
                        } else {
                            dst.put_i32(0); // offline_replicas count
                        }
                    }
                    if compact {
                        put_unsigned_varint(&mut dst, 0); // tagged fields
                    }
                }

                if version >= 8 {
                    // v8+: topic_authorized_operations
                    dst.put_i32(-2147483648); // no ops specified
                }
                if compact {
                    put_unsigned_varint(&mut dst, 0); // tagged fields
                }
            }
            if version >= 8 {
                // v8+: cluster_authorized_operations
                dst.put_i32(-2147483648); // no ops specified
            }
            if compact {
                put_unsigned_varint(&mut dst, 0); // tagged fields at end
            }
        }
        API_PRODUCE => {
            let (topic, partition, base_offset, ok) =
                read_produce_request_and_apply(broker, &mut body, version)?;
            write_response_header(&mut dst, correlation_id);
            // Response: topics array, then throttle_time_ms (v1+). No log_start_offset before v5.
            dst.put_i32(1); // topics count
            write_string(&mut dst, &topic);
            dst.put_i32(1); // partitions count
            dst.put_i32(partition);
            dst.put_i16(if ok { 0 } else { -1 }); // error_code: 0=OK, -1=UNKNOWN_SERVER_ERROR
            dst.put_i64(base_offset); // offset
            if version >= 2 {
                dst.put_i64(-1); // timestamp (v2+)
            }
            if version >= 5 {
                dst.put_i64(0); // log_start_offset (v5+)
            }
            if version >= 1 {
                dst.put_i32(0); // throttle_time_ms (v1+)
            }
        }
        API_FETCH => {
            let (topic, partition, records, high_water_mark) =
                read_fetch_request_and_apply(broker, &mut body, version)?;
            write_response_header(&mut dst, correlation_id);
            if version >= 1 {
                dst.put_i32(0); // throttle_time_ms (v1+)
            }
            dst.put_i32(1); // topic count
            write_string(&mut dst, &topic);
            dst.put_i32(1); // partition count
            dst.put_i32(partition);
            dst.put_i16(0); // error_code
            dst.put_i64(high_water_mark);
            if version >= 4 {
                dst.put_i64(-1); // last_stable_offset (v4+)
            }
            if version >= 5 {
                dst.put_i64(0); // log_start_offset (v5+)
            }
            if version >= 4 {
                dst.put_i32(0); // aborted_transactions count (v4+)
            }
            let batch = if records.is_empty() {
                Vec::new()
            } else {
                let base = records[0].0;
                let refs: Vec<(i64, &Record)> = records.iter().map(|(o, r)| (*o, r)).collect();
                build_record_batch(base, &refs)
            };
            dst.put_i32(batch.len() as i32);
            dst.extend_from_slice(&batch);
        }
        API_LIST_OFFSETS => {
            // ListOffsets: used by consumers to find earliest/latest offsets
            let _replica_id = body.get_i32();
            let topic_count = body.get_i32();
            // For simplicity, handle first topic/partition only
            let mut topic_name = String::new();
            let mut partition_id = 0i32;
            let mut timestamp = 0i64;
            if topic_count > 0 {
                topic_name = read_string(&mut body)?.unwrap_or_default();
                let partition_count = body.get_i32();
                if partition_count > 0 {
                    partition_id = body.get_i32();
                    timestamp = body.get_i64();
                }
            }
            // Determine offset: -2=earliest(0), -1=latest(hwm), else 0
            let offset = if timestamp == -2 {
                broker.start_offset(&topic_name, partition_id).unwrap_or(0)
            } else if timestamp == -1 {
                broker
                    .high_water_mark(&topic_name, partition_id)
                    .unwrap_or(0)
            } else {
                0
            };
            write_response_header(&mut dst, correlation_id);
            if version >= 2 {
                dst.put_i32(0); // throttle_time_ms (v2+)
            }
            dst.put_i32(1); // topic count
            write_string(&mut dst, &topic_name);
            dst.put_i32(1); // partition count
            dst.put_i32(partition_id);
            dst.put_i16(0); // error_code
            if version >= 1 {
                dst.put_i64(timestamp); // timestamp (v1+)
            }
            dst.put_i64(offset); // offset
        }
        _ => {
            write_response_header(&mut dst, correlation_id);
            dst.put_i16(0);
        }
    }
    Ok(dst)
}

fn read_metadata_request(body: &mut Cursor<Vec<u8>>, _version: i16) -> Result<Vec<String>> {
    let n = body.get_i32();
    // Negative count or very large count means null (request all topics)
    if n < 0 {
        return Ok(Vec::new());
    }
    let mut topics = Vec::with_capacity(n.clamp(0, 1000) as usize);
    for _ in 0..n {
        if let Some(s) = read_string(body)? {
            topics.push(s);
        }
    }
    Ok(topics)
}

fn read_produce_request_and_apply(
    broker: &Broker,
    body: &mut Cursor<Vec<u8>>,
    version: i16,
) -> Result<(String, i32, i64, bool)> {
    if !broker.is_leader() {
        return Err(ThorstreamError::NotLeader {
            leader_id: broker.leader_id(),
            leader_addr: broker.leader_addr(),
        });
    }
    if version >= 3 {
        let _ = read_string(body)?; // transactional_id (nullable)
    }
    let _acks = body.get_i16();
    let _timeout = body.get_i32();
    let topic_count = body.get_i32();
    let mut last_topic = String::new();
    let mut last_partition = 0i32;
    let mut last_offset = 0i64;
    let mut ok = true;
    for _ in 0..topic_count {
        let topic = read_string(body)?.unwrap_or_default();
        last_topic = topic.clone();
        let _ = broker.ensure_topic(&topic);
        let partition_count = body.get_i32();
        for _ in 0..partition_count {
            let partition = body.get_i32();
            last_partition = partition;
            let records_len = body.get_i32();
            if records_len <= 0 {
                continue;
            }
            let mut buf = vec![0u8; records_len as usize];
            body.copy_to_slice(&mut buf);
            match parse_record_batch(&buf) {
                Ok((_, records)) => {
                    for (i, rec) in records.into_iter().enumerate() {
                        match broker.produce(&topic, Some(partition), rec.clone()) {
                            Ok((p, offset)) => {
                                if cluster::replicate_to_quorum(broker, &topic, p, offset, &rec)
                                    .is_err()
                                {
                                    ok = false;
                                }
                                if i == 0 {
                                    last_offset = offset;
                                }
                            }
                            Err(_) => ok = false,
                        }
                    }
                }
                Err(_) => ok = false,
            }
        }
    }
    Ok((last_topic, last_partition, last_offset, ok))
}

fn read_fetch_request_and_apply(
    broker: &Broker,
    body: &mut Cursor<Vec<u8>>,
    version: i16,
) -> Result<FetchResponseData> {
    let _replica_id = body.get_i32();
    let _max_wait = body.get_i32();
    let _min_bytes = body.get_i32();
    if version >= 3 {
        let _max_bytes = body.get_i32(); // v3+
    }
    if version >= 4 {
        let _isolation_level = body.get_i8(); // v4+
    }
    if version >= 7 {
        let _session_id = body.get_i32(); // v7+
        let _session_epoch = body.get_i32();
    }
    let topic_count = body.get_i32();
    if topic_count <= 0 {
        return Err(ThorstreamError::Protocol("fetch: no topics".into()));
    }
    let topic = read_string(body)?.unwrap_or_default();
    let partition_count = body.get_i32();
    if partition_count <= 0 {
        return Err(ThorstreamError::Protocol("fetch: no partitions".into()));
    }
    let partition = body.get_i32();
    let fetch_offset = body.get_i64();
    if version >= 5 {
        let _log_start_offset = body.get_i64(); // v5+
    }
    let partition_max_bytes = body.get_i32().max(0) as usize;
    let max_bytes = partition_max_bytes.max(1);
    if version >= 7 {
        // v7+: forgotten topics
        let _forgotten_topics_count = body.get_i32();
    }
    let stored = broker.fetch(&topic, partition, fetch_offset, max_bytes, 500)?;
    let high = broker.high_water_mark(&topic, partition)?;
    let pairs: Vec<(i64, Record)> = stored.into_iter().map(|s| (s.offset, s.record)).collect();
    Ok((topic, partition, pairs, high))
}

/// Prepend 4-byte frame length (BE) to response.
pub fn kafka_frame_response(body: BytesMut) -> BytesMut {
    let len = body.len() as i32;
    let mut out = BytesMut::new();
    out.put_i32(len);
    out.extend_from_slice(&body);
    out
}

/// Build a minimal error response so the client does not hang when handle_kafka_request fails.
pub fn build_minimal_error_response(api_key: i16, version: i16, correlation_id: i32) -> BytesMut {
    let mut dst = BytesMut::new();
    write_response_header(&mut dst, correlation_id);
    match api_key {
        API_PRODUCE => {
            dst.put_i32(0); // topics count
            if version >= 1 {
                dst.put_i32(0); // throttle_time_ms
            }
        }
        API_FETCH => {
            if version >= 1 {
                dst.put_i32(0); // throttle_time_ms
            }
            dst.put_i32(0); // topic count
        }
        _ => {
            dst.put_i16(1); // generic error_code
        }
    }
    dst
}
