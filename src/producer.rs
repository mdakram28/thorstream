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

    pub fn send_idempotent(
        &self,
        topic: impl AsRef<str>,
        record: Record,
        partition: Option<i32>,
        producer_id: i64,
        sequence: i32,
    ) -> Result<ProduceResult> {
        let topic = topic.as_ref().to_string();
        let (partition_id, offset) =
            self.broker
                .produce_idempotent(&topic, partition, record, producer_id, sequence)?;
        Ok(ProduceResult {
            topic,
            partition: partition_id,
            offset,
        })
    }

    pub fn begin_transaction(&self, transaction_id: impl Into<String>, producer_id: i64) {
        self.broker.begin_transaction(transaction_id, producer_id);
    }

    pub fn send_transactional(
        &self,
        transaction_id: &str,
        topic: impl AsRef<str>,
        record: Record,
        partition: Option<i32>,
    ) -> Result<()> {
        self.broker
            .produce_transactional(transaction_id, topic, partition, record)
    }

    pub fn commit_transaction(&self, transaction_id: &str) -> Result<Vec<ProduceResult>> {
        let rows = self.broker.commit_transaction(transaction_id)?;
        Ok(rows
            .into_iter()
            .map(|(topic, partition, offset)| ProduceResult {
                topic,
                partition,
                offset,
            })
            .collect())
    }

    pub fn abort_transaction(&self, transaction_id: &str) {
        self.broker.abort_transaction(transaction_id)
    }
}
