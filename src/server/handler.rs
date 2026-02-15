//! Handle client connections: decode requests, call broker, encode responses.

use crate::broker::Broker;
use crate::cluster;
use crate::error::Result;
use crate::protocol::{
    decode_request, encode_response, FetchResponse, MetadataResponse, PartitionMetadata,
    ProduceResponse, Request, Response, TopicMetadata,
};
use crate::security::{default_principal, security, AclOperation, AclResourceType};
use bytes::BytesMut;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::error;

const MAX_FRAME_LEN: usize = 100 * 1024 * 1024; // 100MB

/// Run the TCP server loop (accept and spawn per-connection handler).
pub async fn run_server(broker: Arc<Broker>, addr: &str) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    run_server_on_listener(broker, listener).await
}

pub async fn run_server_on_listener(
    broker: Arc<Broker>,
    listener: tokio::net::TcpListener,
) -> Result<()> {
    let addr = listener.local_addr()?;
    tracing::info!("Thorstream server listening on {}", addr);
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                error!("accept error: {}", e);
                continue;
            }
        };
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(broker, stream).await {
                error!("connection {} error: {}", peer, e);
            }
        });
    }
}

async fn handle_connection(broker: Arc<Broker>, mut stream: TcpStream) -> Result<()> {
    let mut read_buf = BytesMut::with_capacity(4096);
    loop {
        read_buf.reserve(4096);
        let n = stream.read_buf(&mut read_buf).await?;
        if n == 0 {
            break;
        }
        while let Some(req) = decode_request(&mut read_buf)? {
            let resp = dispatch(&broker, req).await;
            let mut write_buf = BytesMut::new();
            encode_response(&resp, &mut write_buf)?;
            stream.write_all(&write_buf).await?;
            stream.flush().await?;
        }
        if read_buf.len() > MAX_FRAME_LEN {
            return Err(crate::error::ThorstreamError::Protocol(
                "Frame too large".into(),
            ));
        }
    }
    Ok(())
}

async fn dispatch(broker: &Broker, req: Request) -> Response {
    let principal = default_principal();
    match req {
        Request::Metadata { topics } => {
            if let Err(e) = security().authorize_and_audit(
                &principal,
                AclOperation::Describe,
                AclResourceType::Cluster,
                "cluster",
            ) {
                return Response::Error(e.to_string());
            }
            match handle_metadata(broker, topics) {
                Ok(m) => Response::Metadata(m),
                Err(e) => Response::Error(e.to_string()),
            }
        }
        Request::Produce {
            topic,
            partition,
            records,
        } => {
            if let Err(e) = security().authorize_and_audit(
                &principal,
                AclOperation::Write,
                AclResourceType::Topic,
                &topic,
            ) {
                return Response::Error(e.to_string());
            }
            match handle_produce(broker, topic, partition, records) {
                Ok(r) => Response::Produce(r),
                Err(crate::error::ThorstreamError::NotLeader {
                    leader_id,
                    leader_addr,
                }) => Response::NotLeader {
                    leader_id,
                    leader_addr,
                },
                Err(e) => Response::Error(e.to_string()),
            }
        }
        Request::InternalReplicate {
            topic,
            partition,
            expected_offset,
            record,
        } => match broker.apply_replication(&topic, partition, record, expected_offset) {
            Ok(offset) => Response::InternalReplicateAck(offset),
            Err(e) => Response::Error(e.to_string()),
        },
        Request::ControlPing { .. } => {
            let s = broker.cluster_status();
            Response::ControlPong {
                node_id: s.node_id,
                term: s.term,
                leader_id: s.leader_id,
            }
        }
        Request::Fetch {
            topic,
            partition,
            offset,
            max_bytes,
            max_records,
        } => {
            if let Err(e) = security().authorize_and_audit(
                &principal,
                AclOperation::Read,
                AclResourceType::Topic,
                &topic,
            ) {
                return Response::Error(e.to_string());
            }
            match handle_fetch(broker, &topic, partition, offset, max_bytes, max_records) {
                Ok(r) => Response::Fetch(r),
                Err(e) => Response::Error(e.to_string()),
            }
        }
        Request::OffsetCommit {
            group_id,
            topic,
            partition,
            offset,
        } => {
            if let Err(e) = security().authorize_and_audit(
                &principal,
                AclOperation::Alter,
                AclResourceType::Group,
                &group_id,
            ) {
                return Response::Error(e.to_string());
            }
            match broker.offset_commit(&group_id, &topic, partition, offset) {
                Ok(()) => Response::OffsetCommit,
                Err(e) => Response::Error(e.to_string()),
            }
        }
        Request::OffsetFetch {
            group_id,
            topic,
            partition,
        } => {
            if let Err(e) = security().authorize_and_audit(
                &principal,
                AclOperation::Read,
                AclResourceType::Group,
                &group_id,
            ) {
                return Response::Error(e.to_string());
            }
            match broker.offset_fetch(&group_id, &topic, partition) {
                Ok(off) => Response::OffsetFetch(Some(off)),
                Err(_) => Response::OffsetFetch(None),
            }
        }
    }
}

fn handle_metadata(broker: &Broker, topics: Vec<String>) -> Result<MetadataResponse> {
    let list: Vec<String> = if topics.is_empty() {
        broker.list_topics()
    } else {
        topics
            .into_iter()
            .filter(|t| broker.list_topics().contains(t))
            .collect()
    };
    let topics_meta: Vec<TopicMetadata> = list
        .into_iter()
        .map(|name| {
            let n = broker.num_partitions(&name).unwrap_or(0);
            let rf = broker.replication_factor(&name).unwrap_or(1).max(1) as i32;
            let partitions = (0..n)
                .map(|p| PartitionMetadata {
                    partition_id: p,
                    leader_id: 0,
                    replicas: (0..rf).collect(),
                })
                .collect();
            TopicMetadata { name, partitions }
        })
        .collect();
    Ok(MetadataResponse {
        topics: topics_meta,
    })
}

fn handle_produce(
    broker: &Broker,
    topic: String,
    partition: Option<i32>,
    records: Vec<crate::types::Record>,
) -> Result<ProduceResponse> {
    if !broker.is_leader() {
        return Err(crate::error::ThorstreamError::NotLeader {
            leader_id: broker.leader_id(),
            leader_addr: broker.leader_addr(),
        });
    }

    let mut base_offset = 0i64;
    let mut partition_id = 0i32;
    for record in records {
        let (p, offset) = broker.produce(&topic, partition, record.clone())?;
        cluster::replicate_to_quorum(broker, &topic, p, offset, &record)?;
        partition_id = p;
        base_offset = offset;
    }
    Ok(ProduceResponse {
        topic,
        partition: partition_id,
        base_offset,
    })
}

fn handle_fetch(
    broker: &Broker,
    topic: &str,
    partition: i32,
    offset: i64,
    max_bytes: usize,
    max_records: usize,
) -> Result<FetchResponse> {
    let records = broker.fetch(topic, partition, offset, max_bytes, max_records)?;
    let high_water_mark = broker.high_water_mark(topic, partition)?;
    Ok(FetchResponse {
        topic: topic.to_string(),
        partition,
        records,
        high_water_mark,
    })
}
