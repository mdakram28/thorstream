//! Thorstream server binary: runs the TCP server.
//!
//! Optional: set THORSTREAM_KAFKA_ADDR (e.g. 0.0.0.0:9093) to also run the
//! Kafka wire protocol server for standard Kafka client compatibility.

use std::sync::Arc;
use thorstream::{cluster, compat, server, Broker, BrokerConfig};
use tracing_subscriber::EnvFilter;

fn parse_peers() -> std::collections::HashMap<i32, String> {
    let mut peers = std::collections::HashMap::new();
    if let Ok(raw) = std::env::var("THORSTREAM_CLUSTER_PEERS") {
        for item in raw.split(',').filter(|s| !s.trim().is_empty()) {
            let mut parts = item.splitn(2, '=');
            let id = parts.next().and_then(|s| s.parse::<i32>().ok());
            let addr = parts.next().map(|s| s.to_string());
            if let (Some(id), Some(addr)) = (id, addr) {
                peers.insert(id, addr);
            }
        }
    }
    peers
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("thorstream=info".parse()?))
        .init();

    let config = BrokerConfig {
        node_id: std::env::var("THORSTREAM_NODE_ID")
            .ok()
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0),
        peers: parse_peers(),
        ..BrokerConfig::default()
    };
    let broker = Arc::new(Broker::new(config)?);

    if !broker.cluster_status().peers.is_empty() {
        let cp_broker = Arc::clone(&broker);
        tokio::spawn(async move {
            cluster::run_control_plane(cp_broker).await;
        });
    }

    let main_addr = std::env::var("THORSTREAM_ADDR").unwrap_or_else(|_| "0.0.0.0:9092".to_string());
    let main_broker = Arc::clone(&broker);
    let main = tokio::spawn(async move { server::run_server(main_broker, &main_addr).await });

    if let Ok(kafka_addr) = std::env::var("THORSTREAM_KAFKA_ADDR") {
        let kafka_broker = Arc::clone(&broker);
        tokio::spawn(async move { server::run_kafka_server(kafka_broker, &kafka_addr).await });
    }

    if let Ok(compat_addr) = std::env::var("THORSTREAM_COMPAT_API_ADDR") {
        tokio::spawn(async move {
            let _ = compat::run_compat_api(&compat_addr).await;
        });
    }

    main.await??;
    Ok(())
}
