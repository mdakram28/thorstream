use crate::broker::Broker;
use crate::error::{Result, ThorstreamError};
use crate::observability::observability;
use axum::extract::{Path, State};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

#[derive(Clone)]
struct AppState {
    inner: Arc<RwLock<CompatState>>,
    broker: Option<Arc<Broker>>,
}

#[derive(Default)]
struct CompatState {
    connectors: HashMap<String, Connector>,
    subjects: HashMap<String, Vec<SchemaVersion>>,
    id_index: HashMap<u32, SchemaVersionResponse>,
    next_schema_id: u32,
    global_compatibility: String,
    subject_compatibility: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConnectorPlugin {
    class: String,
    #[serde(rename = "type")]
    plugin_type: String,
    version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConnectorCreateRequest {
    name: String,
    config: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Connector {
    name: String,
    config: HashMap<String, String>,
    state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConnectorStatus {
    name: String,
    connector: ConnectorState,
    tasks: Vec<ConnectorTaskState>,
    #[serde(rename = "type")]
    connector_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConnectorState {
    state: String,
    worker_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConnectorTaskState {
    id: i32,
    state: String,
    worker_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchemaRegisterRequest {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchemaVersion {
    id: u32,
    schema: String,
    schema_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchemaVersionResponse {
    subject: String,
    version: u32,
    id: u32,
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: String,
}

pub async fn run_compat_api(addr: &str) -> Result<()> {
    run_compat_api_with_broker(addr, None).await
}

pub async fn run_compat_api_with_broker(addr: &str, broker: Option<Arc<Broker>>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    run_compat_api_on_listener(listener, broker).await
}

pub async fn run_compat_api_on_listener(
    listener: TcpListener,
    broker: Option<Arc<Broker>>,
) -> Result<()> {
    let state = AppState {
        inner: Arc::new(RwLock::new(CompatState {
            global_compatibility: "BACKWARD".to_string(),
            ..CompatState::default()
        })),
        broker,
    };

    let app = Router::new()
        .route("/connectors", get(list_connectors).post(create_connector))
        .route("/connector-plugins", get(list_connector_plugins))
        .route(
            "/connectors/:name",
            get(get_connector).delete(delete_connector),
        )
        .route("/connectors/:name/status", get(get_connector_status))
        .route("/connectors/:name/pause", put(pause_connector))
        .route("/connectors/:name/resume", put(resume_connector))
        .route("/subjects", get(list_subjects))
        .route(
            "/subjects/:subject/versions",
            get(list_subject_versions).post(register_schema),
        )
        .route(
            "/subjects/:subject/versions/:version",
            get(get_subject_version),
        )
        .route("/schemas/ids/:id", get(get_schema_by_id))
        .route("/metrics", get(metrics))
        .route("/config", get(get_global_config).put(set_global_config))
        .route(
            "/config/:subject",
            get(get_subject_config).put(set_subject_config),
        )
        .route(
            "/compatibility/subjects/:subject/versions/:version",
            post(check_compatibility),
        )
        .with_state(state);

    axum::serve(listener, app)
        .await
        .map_err(|e| ThorstreamError::Protocol(e.to_string()))
}

async fn metrics(State(state): State<AppState>) -> String {
    observability().render_prometheus(state.broker.as_deref())
}

async fn list_connector_plugins() -> Json<Vec<ConnectorPlugin>> {
    Json(vec![
        ConnectorPlugin {
            class: "io.confluent.connect.s3.S3SinkConnector".to_string(),
            plugin_type: "sink".to_string(),
            version: "1.0.0".to_string(),
        },
        ConnectorPlugin {
            class: "io.confluent.connect.s3.source.S3SourceConnector".to_string(),
            plugin_type: "source".to_string(),
            version: "1.0.0".to_string(),
        },
        ConnectorPlugin {
            class: "io.confluent.connect.jdbc.JdbcSinkConnector".to_string(),
            plugin_type: "sink".to_string(),
            version: "1.0.0".to_string(),
        },
        ConnectorPlugin {
            class: "io.confluent.connect.jdbc.JdbcSourceConnector".to_string(),
            plugin_type: "source".to_string(),
            version: "1.0.0".to_string(),
        },
        ConnectorPlugin {
            class: "io.debezium.connector.postgresql.PostgresConnector".to_string(),
            plugin_type: "source".to_string(),
            version: "1.0.0".to_string(),
        },
    ])
}

async fn list_connectors(State(state): State<AppState>) -> Json<Vec<String>> {
    let guard = state.inner.read().await;
    Json(guard.connectors.keys().cloned().collect())
}

async fn create_connector(
    State(state): State<AppState>,
    Json(req): Json<ConnectorCreateRequest>,
) -> Json<Value> {
    let connector = Connector {
        name: req.name.clone(),
        config: req.config.clone(),
        state: "RUNNING".to_string(),
    };

    let mut guard = state.inner.write().await;
    guard.connectors.insert(req.name.clone(), connector);

    Json(json!({ "name": req.name, "config": req.config }))
}

async fn get_connector(Path(name): Path<String>, State(state): State<AppState>) -> Json<Value> {
    let guard = state.inner.read().await;
    if let Some(connector) = guard.connectors.get(&name) {
        return Json(json!({ "name": connector.name, "config": connector.config }));
    }
    Json(json!({ "error_code": 404, "message": "Connector not found" }))
}

async fn delete_connector(Path(name): Path<String>, State(state): State<AppState>) -> Json<Value> {
    let mut guard = state.inner.write().await;
    guard.connectors.remove(&name);
    Json(json!({ "message": "deleted" }))
}

async fn get_connector_status(
    Path(name): Path<String>,
    State(state): State<AppState>,
) -> Json<Value> {
    let guard = state.inner.read().await;
    if let Some(connector) = guard.connectors.get(&name) {
        let connector_type = connector
            .config
            .get("connector.class")
            .map(|v| {
                if v.to_lowercase().contains("sink") {
                    "sink"
                } else {
                    "source"
                }
            })
            .unwrap_or("sink")
            .to_string();

        let status = ConnectorStatus {
            name: connector.name.clone(),
            connector: ConnectorState {
                state: connector.state.clone(),
                worker_id: "thorstream-compat-0".to_string(),
            },
            tasks: vec![ConnectorTaskState {
                id: 0,
                state: connector.state.clone(),
                worker_id: "thorstream-compat-0".to_string(),
            }],
            connector_type,
        };
        return Json(serde_json::to_value(status).unwrap_or_else(|_| json!({})));
    }
    Json(json!({ "error_code": 404, "message": "Connector not found" }))
}

async fn pause_connector(Path(name): Path<String>, State(state): State<AppState>) -> Json<Value> {
    let mut guard = state.inner.write().await;
    if let Some(connector) = guard.connectors.get_mut(&name) {
        connector.state = "PAUSED".to_string();
    }
    Json(json!({ "message": "ok" }))
}

async fn resume_connector(Path(name): Path<String>, State(state): State<AppState>) -> Json<Value> {
    let mut guard = state.inner.write().await;
    if let Some(connector) = guard.connectors.get_mut(&name) {
        connector.state = "RUNNING".to_string();
    }
    Json(json!({ "message": "ok" }))
}

async fn list_subjects(State(state): State<AppState>) -> Json<Vec<String>> {
    let guard = state.inner.read().await;
    Json(guard.subjects.keys().cloned().collect())
}

async fn list_subject_versions(
    Path(subject): Path<String>,
    State(state): State<AppState>,
) -> Json<Value> {
    let guard = state.inner.read().await;
    if let Some(versions) = guard.subjects.get(&subject) {
        let out: Vec<u32> = (1..=versions.len() as u32).collect();
        return Json(json!(out));
    }
    Json(json!([]))
}

async fn register_schema(
    Path(subject): Path<String>,
    State(state): State<AppState>,
    Json(req): Json<SchemaRegisterRequest>,
) -> Json<Value> {
    let schema_type = req.schema_type.unwrap_or_else(|| "AVRO".to_string());
    let mut guard = state.inner.write().await;
    guard.next_schema_id += 1;
    let id = guard.next_schema_id;
    let versions = guard.subjects.entry(subject.clone()).or_default();
    let version = versions.len() as u32 + 1;

    let entry = SchemaVersion {
        id,
        schema: req.schema,
        schema_type: schema_type.clone(),
    };
    versions.push(entry.clone());

    guard.id_index.insert(
        id,
        SchemaVersionResponse {
            subject,
            version,
            id,
            schema: entry.schema,
            schema_type,
        },
    );

    Json(json!({ "id": id }))
}

async fn get_subject_version(
    Path((subject, version)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Json<Value> {
    let guard = state.inner.read().await;
    let Some(versions) = guard.subjects.get(&subject) else {
        return Json(json!({ "error_code": 404, "message": "Subject not found" }));
    };

    let idx = if version == "latest" {
        if versions.is_empty() {
            return Json(json!({ "error_code": 404, "message": "No versions" }));
        }
        versions.len() - 1
    } else {
        let parsed = version.parse::<usize>().ok().unwrap_or(0);
        if parsed == 0 || parsed > versions.len() {
            return Json(json!({ "error_code": 404, "message": "Version not found" }));
        }
        parsed - 1
    };

    let schema = &versions[idx];
    Json(json!({
        "subject": subject,
        "version": (idx + 1) as u32,
        "id": schema.id,
        "schema": schema.schema,
        "schemaType": schema.schema_type,
    }))
}

async fn get_schema_by_id(Path(id): Path<u32>, State(state): State<AppState>) -> Json<Value> {
    let guard = state.inner.read().await;
    if let Some(schema) = guard.id_index.get(&id) {
        return Json(json!({
            "schema": schema.schema,
            "schemaType": schema.schema_type,
        }));
    }
    Json(json!({ "error_code": 404, "message": "Schema not found" }))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompatibilityConfigRequest {
    compatibility: Option<String>,
    #[serde(rename = "compatibilityLevel")]
    compatibility_level: Option<String>,
}

async fn get_global_config(State(state): State<AppState>) -> Json<Value> {
    let guard = state.inner.read().await;
    Json(json!({ "compatibilityLevel": guard.global_compatibility }))
}

async fn set_global_config(
    State(state): State<AppState>,
    Json(req): Json<CompatibilityConfigRequest>,
) -> Json<Value> {
    let mut guard = state.inner.write().await;
    let value = req
        .compatibility_level
        .or(req.compatibility)
        .unwrap_or_else(|| "BACKWARD".to_string());
    guard.global_compatibility = value.clone();
    Json(json!({ "compatibility": value }))
}

async fn get_subject_config(
    Path(subject): Path<String>,
    State(state): State<AppState>,
) -> Json<Value> {
    let guard = state.inner.read().await;
    let value = guard
        .subject_compatibility
        .get(&subject)
        .cloned()
        .unwrap_or_else(|| guard.global_compatibility.clone());
    Json(json!({ "compatibilityLevel": value }))
}

async fn set_subject_config(
    Path(subject): Path<String>,
    State(state): State<AppState>,
    Json(req): Json<CompatibilityConfigRequest>,
) -> Json<Value> {
    let mut guard = state.inner.write().await;
    let value = req
        .compatibility_level
        .or(req.compatibility)
        .unwrap_or_else(|| "BACKWARD".to_string());
    guard.subject_compatibility.insert(subject, value.clone());
    Json(json!({ "compatibility": value }))
}

async fn check_compatibility(
    Path((subject, version)): Path<(String, String)>,
    State(state): State<AppState>,
    Json(req): Json<SchemaRegisterRequest>,
) -> Json<Value> {
    let guard = state.inner.read().await;
    let Some(versions) = guard.subjects.get(&subject) else {
        return Json(json!({ "is_compatible": true }));
    };

    if versions.is_empty() {
        return Json(json!({ "is_compatible": true }));
    }

    let idx = if version == "latest" {
        versions.len() - 1
    } else {
        let parsed = version.parse::<usize>().ok().unwrap_or(versions.len());
        parsed.saturating_sub(1).min(versions.len() - 1)
    };

    let existing = &versions[idx];
    let incoming_type = req.schema_type.unwrap_or_else(|| "AVRO".to_string());
    let is_compatible = incoming_type == existing.schema_type && !req.schema.trim().is_empty();
    Json(json!({ "is_compatible": is_compatible }))
}
