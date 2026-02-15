//! TCP server that speaks the Kafka wire protocol for standard client compatibility.

use crate::broker::Broker;
use crate::error::Result;
use crate::observability::observability;
use crate::protocol::{
    build_minimal_error_response, decode_kafka_request, handle_kafka_request, kafka_frame_response,
};
use crate::security::{default_principal, security, AclOperation, AclResourceType};
use bytes::BytesMut;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{error, info};

const MAX_FRAME_LEN: usize = 100 * 1024 * 1024;

/// Run the Kafka-protocol server on an existing listener (e.g. from bind("127.0.0.1:0")).
pub async fn run_kafka_server_on_listener(
    broker: Arc<Broker>,
    listener: tokio::net::TcpListener,
) -> Result<()> {
    let addr = listener.local_addr()?;
    tracing::info!("Thorstream Kafka protocol server listening on {}", addr);
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                error!("kafka accept error: {}", e);
                continue;
            }
        };
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            if let Err(e) = handle_kafka_connection(broker, stream).await {
                error!("kafka connection {} error: {}", peer, e);
            }
        });
    }
}

/// Run the Kafka-protocol server loop (binds to addr).
pub async fn run_kafka_server(broker: Arc<Broker>, addr: &str) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    run_kafka_server_on_listener(broker, listener).await
}

async fn handle_kafka_connection(broker: Arc<Broker>, mut stream: TcpStream) -> Result<()> {
    let mut read_buf = BytesMut::with_capacity(4096);
    loop {
        read_buf.reserve(4096);
        let n = stream.read_buf(&mut read_buf).await?;
        if n == 0 {
            break;
        }
        while let Some((api_key, version, correlation_id, body)) =
            decode_kafka_request(&mut read_buf)?
        {
            let span = tracing::info_span!("thorstream.request", transport = "kafka", api_key, version);
            let _entered = span.enter();
            let started = Instant::now();
            let _ = security().authorize_and_audit(
                &default_principal(),
                AclOperation::Describe,
                AclResourceType::Cluster,
                "kafka-api",
            );
            info!(api_key, version, correlation_id, "kafka request");
            let body_in = body.into_inner();
            let body_cursor = Cursor::new(body_in);
            let resp = match handle_kafka_request(
                &broker,
                api_key,
                version,
                correlation_id,
                body_cursor,
            ) {
                Ok(r) => r,
                Err(e) => {
                    error!("kafka request error: {}", e);
                    build_minimal_error_response(api_key, version, correlation_id)
                }
            };
            observability().record_request(started.elapsed(), true);
            let framed = kafka_frame_response(resp);
            info!(api_key, len = framed.len(), "kafka response");
            stream.write_all(&framed).await?;
            stream.flush().await?;
        }
        if read_buf.len() > MAX_FRAME_LEN {
            return Err(crate::error::ThorstreamError::Protocol(
                "Kafka frame too large".into(),
            ));
        }
    }
    Ok(())
}
