use crate::broker::Broker;
use crate::error::Result;
use crate::types::Record;
use std::sync::Arc;

pub struct StreamsBuilder {
    broker: Arc<Broker>,
}

impl StreamsBuilder {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }

    pub fn stream(&self, topic: impl Into<String>) -> KStream {
        KStream {
            broker: Arc::clone(&self.broker),
            source_topic: topic.into(),
            transforms: Vec::new(),
        }
    }
}

type Transform = Arc<dyn Fn(Record) -> Option<Record> + Send + Sync>;

pub struct KStream {
    broker: Arc<Broker>,
    source_topic: String,
    transforms: Vec<Transform>,
}

impl KStream {
    pub fn filter_values<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&[u8]) -> bool + Send + Sync + 'static,
    {
        let op: Transform = Arc::new(move |record: Record| {
            if predicate(&record.value) {
                Some(record)
            } else {
                None
            }
        });
        self.transforms.push(op);
        self
    }

    pub fn map_values<F>(mut self, mapper: F) -> Self
    where
        F: Fn(&[u8]) -> Vec<u8> + Send + Sync + 'static,
    {
        let op: Transform = Arc::new(move |mut record: Record| {
            record.value = mapper(&record.value);
            Some(record)
        });
        self.transforms.push(op);
        self
    }

    pub fn to(self, output_topic: impl Into<String>) -> StreamTask {
        StreamTask {
            broker: self.broker,
            source_topic: self.source_topic,
            output_topic: output_topic.into(),
            transforms: self.transforms,
            next_offset: 0,
        }
    }
}

pub struct StreamTask {
    broker: Arc<Broker>,
    source_topic: String,
    output_topic: String,
    transforms: Vec<Transform>,
    next_offset: i64,
}

impl StreamTask {
    pub fn run_once(&mut self, max_records: usize) -> Result<usize> {
        self.broker.ensure_topic(&self.source_topic)?;
        self.broker.ensure_topic(&self.output_topic)?;

        let fetched = self.broker.fetch(
            &self.source_topic,
            0,
            self.next_offset,
            4 * 1024 * 1024,
            max_records,
        )?;

        if fetched.is_empty() {
            return Ok(0);
        }

        let mut processed = 0usize;
        for stored in fetched {
            self.next_offset = stored.offset + 1;
            let mut current = Some(stored.record);
            for transform in &self.transforms {
                current = current.and_then(|record| transform(record));
                if current.is_none() {
                    break;
                }
            }
            if let Some(out) = current {
                self.broker.produce(&self.output_topic, Some(0), out)?;
                processed += 1;
            }
        }

        Ok(processed)
    }
}
