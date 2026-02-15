//! Thorstream server binary: runs the TCP server.
//!
//! Optional: set THORSTREAM_KAFKA_ADDR (e.g. 0.0.0.0:9093) to also run the
//! Kafka wire protocol server for standard Kafka client compatibility.

use std::sync::Arc;
use thorstream::{Broker, BrokerConfig, server};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("thorstream=info".parse()?))
        .init();

    let config = BrokerConfig::default();
    let broker = Arc::new(Broker::new(config)?);

    let main_addr = std::env::var("THORSTREAM_ADDR").unwrap_or_else(|_| "0.0.0.0:9092".to_string());
    let main_broker = Arc::clone(&broker);
    let main = tokio::spawn(async move { server::run_server(main_broker, &main_addr).await });

    if let Ok(kafka_addr) = std::env::var("THORSTREAM_KAFKA_ADDR") {
        let kafka_broker = Arc::clone(&broker);
        tokio::spawn(async move { server::run_kafka_server(kafka_broker, &kafka_addr).await });
    }

    main.await??;
    Ok(())
}
