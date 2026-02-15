//! rust-rdkafka compatibility smoke test.

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use thorstream::{Broker, BrokerConfig};

fn producer(bootstrap: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "5000")
        .set("api.version.request", "false")
        .set("broker.version.fallback", "0.10.2")
        .create()
        .unwrap()
}

#[tokio::test]
async fn rdkafka_client_roundtrip_compat() {
    let dir = TempDir::new().unwrap();
    let broker = Arc::new(
        Broker::new(BrokerConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        })
        .unwrap(),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let bootstrap = format!("127.0.0.1:{}", addr.port());

    std::env::set_var("THORSTREAM_KAFKA_PORT", addr.port().to_string());
    let broker_for_assert = Arc::clone(&broker);
    let server = tokio::spawn(async move {
        thorstream::server::run_kafka_server_on_listener(broker, listener)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    let topic = "rdkafka-compat";
    let p = producer(&bootstrap);

    for payload in ["m1", "m2", "m3"] {
        p.send(
            FutureRecord::<(), str>::to(topic).payload(payload),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    let stored = broker_for_assert
        .fetch(topic, 0, 0, 1024 * 1024, 10)
        .unwrap();
    let values: Vec<Vec<u8>> = stored.into_iter().map(|r| r.record.value).collect();
    assert_eq!(values, vec![b"m1".to_vec(), b"m2".to_vec(), b"m3".to_vec()]);

    server.abort();
}
