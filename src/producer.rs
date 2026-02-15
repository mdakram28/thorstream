//! Producer: send records to topics (Kafka-compatible API).

use crate::broker::Broker;
use crate::error::Result;
use crate::types::Record;
use std::sync::Arc;

/// Result of a successful produce (Kafka-compatible).
#[derive(Debug, Clone)]
pub struct ProduceResult {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// Producer client: send records to the broker.
pub struct Producer {
    broker: Arc<Broker>,
}

impl Producer {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }

    /// Send a single record; partition is chosen by broker (round-robin or specified).
    pub fn send(
        &self,
        topic: impl AsRef<str>,
        record: Record,
        partition: Option<i32>,
    ) -> Result<ProduceResult> {
        let topic = topic.as_ref().to_string();
        let (partition_id, offset) = self.broker.produce(&topic, partition, record)?;
        Ok(ProduceResult {
            topic,
            partition: partition_id,
            offset,
        })
    }

    /// Send multiple records to the same topic (batch).
    pub fn send_batch(
        &self,
        topic: impl AsRef<str>,
        records: Vec<Record>,
        partition: Option<i32>,
    ) -> Result<Vec<ProduceResult>> {
        let topic = topic.as_ref().to_string();
        let mut results = Vec::with_capacity(records.len());
        for record in records {
            let (partition_id, offset) = self.broker.produce(&topic, partition, record)?;
            results.push(ProduceResult {
                topic: topic.clone(),
                partition: partition_id,
                offset,
            });
        }
        Ok(results)
    }
}
