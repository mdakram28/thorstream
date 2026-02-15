use crate::broker::Broker;
use crate::error::{Result, ThorstreamError};
use crate::protocol::{encode_request, Request};
use crate::types::Record;
use bytes::BytesMut;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

fn send_request(addr: &str, req: &Request, timeout: Duration) -> Result<(u16, Vec<u8>)> {
    let mut stream = TcpStream::connect(addr)
        .map_err(|e| ThorstreamError::Cluster(format!("connect {} failed: {}", addr, e)))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|e| ThorstreamError::Cluster(format!("set_read_timeout failed: {}", e)))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|e| ThorstreamError::Cluster(format!("set_write_timeout failed: {}", e)))?;

    let mut out = BytesMut::new();
    encode_request(req, &mut out)?;
    stream
        .write_all(&out)
        .map_err(|e| ThorstreamError::Cluster(format!("write {} failed: {}", addr, e)))?;

    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .map_err(|e| ThorstreamError::Cluster(format!("read len {} failed: {}", addr, e)))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len < 2 {
        return Err(ThorstreamError::Cluster("short response".into()));
    }

    let mut frame = vec![0u8; len];
    stream
        .read_exact(&mut frame)
        .map_err(|e| ThorstreamError::Cluster(format!("read frame {} failed: {}", addr, e)))?;
    let api_key = u16::from_be_bytes([frame[0], frame[1]]);
    Ok((api_key, frame[2..].to_vec()))
}

pub fn replicate_to_peer(
    addr: &str,
    topic: &str,
    partition: i32,
    expected_offset: i64,
    record: &Record,
) -> Result<i64> {
    let req = Request::InternalReplicate {
        topic: topic.to_string(),
        partition,
        expected_offset,
        record: record.clone(),
    };
    let (api, payload) = send_request(addr, &req, Duration::from_millis(1800))?;
    match api {
        5 => bincode::deserialize::<i64>(&payload)
            .map_err(|e| ThorstreamError::Cluster(format!("replicate decode ack failed: {}", e))),
        0xFFFE => {
            let (leader_id, leader_addr): (Option<i32>, Option<String>) =
                bincode::deserialize(&payload).map_err(|e| {
                    ThorstreamError::Cluster(format!("not leader decode failed: {}", e))
                })?;
            Err(ThorstreamError::NotLeader {
                leader_id,
                leader_addr,
            })
        }
        _ => Err(ThorstreamError::Cluster(format!(
            "unexpected replicate response api {}",
            api
        ))),
    }
}

pub fn ping_peer(addr: &str, node_id: i32, term: i64) -> Result<(i32, i64, Option<i32>)> {
    let req = Request::ControlPing { node_id, term };
    let (api, payload) = send_request(addr, &req, Duration::from_millis(1200))?;
    if api != 6 {
        return Err(ThorstreamError::Cluster(format!(
            "unexpected ping response api {}",
            api
        )));
    }
    bincode::deserialize::<(i32, i64, Option<i32>)>(&payload)
        .map_err(|e| ThorstreamError::Cluster(format!("decode ping failed: {}", e)))
}

pub async fn run_control_plane(broker: Arc<Broker>) {
    let status = broker.cluster_status();
    if status.peers.is_empty() {
        return;
    }

    let mut term = status.term;
    let mut last_leader = status.leader_id;

    loop {
        let current = broker.cluster_status();
        let mut alive = vec![current.node_id];
        for (peer_id, addr) in &current.peers {
            if ping_peer(addr, current.node_id, term).is_ok() {
                alive.push(*peer_id);
            }
        }
        alive.sort_unstable();
        let elected = alive.first().copied();
        if elected != last_leader {
            term += 1;
            broker.set_leader(elected, term);
            last_leader = elected;
        }

        tokio::time::sleep(Duration::from_millis(900)).await;
    }
}

pub fn replicate_to_quorum(
    broker: &Broker,
    topic: &str,
    partition: i32,
    offset: i64,
    record: &Record,
) -> Result<()> {
    let status = broker.cluster_status();
    if status.peers.is_empty() {
        return Ok(());
    }

    let total_nodes = status.peers.len() + 1;
    let quorum = total_nodes / 2 + 1;
    let mut acked = 1usize;

    for addr in status.peers.values() {
        let mut peer_acked = false;
        for _ in 0..3 {
            if replicate_to_peer(addr, topic, partition, offset, record).is_ok() {
                peer_acked = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(80));
        }
        if peer_acked {
            acked += 1;
        }
    }

    if acked < quorum {
        return Err(ThorstreamError::Cluster(format!(
            "quorum not reached: acked={} quorum={}",
            acked, quorum
        )));
    }
    Ok(())
}
