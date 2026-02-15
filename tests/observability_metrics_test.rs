use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use tokio::net::TcpListener;
use thorstream::{Broker, BrokerConfig, Record};

#[tokio::test]
async fn metrics_endpoint_exposes_prometheus_metrics() {
    let dir = TempDir::new().unwrap();
    let broker = Arc::new(
        Broker::new(BrokerConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        })
        .unwrap(),
    );

    broker.create_topic("obs", None).unwrap();
    broker
        .produce("obs", Some(0), Record::new(b"m1".to_vec()))
        .unwrap();
    let _ = broker.fetch("obs", 0, 0, 1024 * 1024, 10).unwrap();

    broker.offset_commit("g1", "obs", 0, 0).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{}", addr);

    let server = tokio::spawn(async move {
        thorstream::compat::run_compat_api_on_listener(listener, Some(broker))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(150)).await;

    let body = reqwest::get(format!("{}/metrics", base))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(body.contains("thorstream_consumer_lag"));
    assert!(body.contains("thorstream_partition_size_bytes"));
    assert!(body.contains("thorstream_under_replicated_partitions"));
    assert!(body.contains("thorstream_request_latency_p99_ms"));

    server.abort();
}
