use std::collections::HashMap;
use std::time::Duration;

use serde_json::json;
use tokio::net::TcpListener;

#[tokio::test]
async fn schema_registry_and_connect_surface() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{}", addr);

    let server = tokio::spawn(async move {
        thorstream::compat::run_compat_api_on_listener(listener)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(150)).await;

    let client = reqwest::Client::new();

    let plugins: Vec<HashMap<String, serde_json::Value>> = client
        .get(format!("{}/connector-plugins", base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(plugins.iter().any(|p| {
        p.get("class")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_lowercase()
            .contains("debezium")
    }));

    let create = json!({
        "name": "s3-sink-demo",
        "config": {
            "connector.class": "io.confluent.connect.s3.S3SinkConnector",
            "tasks.max": "1",
            "topics": "events"
        }
    });
    let create_resp: serde_json::Value = client
        .post(format!("{}/connectors", base))
        .json(&create)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(create_resp["name"], "s3-sink-demo");

    let status: serde_json::Value = client
        .get(format!("{}/connectors/s3-sink-demo/status", base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["connector"]["state"], "RUNNING");

    let schema_req = json!({
        "schema": "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}",
        "schemaType": "AVRO"
    });

    let register: serde_json::Value = client
        .post(format!("{}/subjects/events-value/versions", base))
        .json(&schema_req)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = register["id"].as_u64().unwrap();
    assert!(id > 0);

    let by_id: serde_json::Value = client
        .get(format!("{}/schemas/ids/{}", base, id))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(by_id["schemaType"], "AVRO");

    let latest: serde_json::Value = client
        .get(format!("{}/subjects/events-value/versions/latest", base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(latest["version"], 1);

    let compat_req = json!({
        "schema": "{\"type\":\"record\",\"name\":\"Event2\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}",
        "schemaType": "AVRO"
    });
    let compat: serde_json::Value = client
        .post(format!(
            "{}/compatibility/subjects/events-value/versions/latest",
            base
        ))
        .json(&compat_req)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(compat["is_compatible"], true);

    server.abort();
}
