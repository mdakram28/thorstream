use crate::broker::Broker;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

pub struct Observability {
    produce_records_total: AtomicU64,
    produce_bytes_total: AtomicU64,
    fetch_records_total: AtomicU64,
    fetch_bytes_total: AtomicU64,
    requests_total: AtomicU64,
    request_errors_total: AtomicU64,
    request_latency_ms: Mutex<VecDeque<u64>>,
}

impl Observability {
    fn new() -> Self {
        Self {
            produce_records_total: AtomicU64::new(0),
            produce_bytes_total: AtomicU64::new(0),
            fetch_records_total: AtomicU64::new(0),
            fetch_bytes_total: AtomicU64::new(0),
            requests_total: AtomicU64::new(0),
            request_errors_total: AtomicU64::new(0),
            request_latency_ms: Mutex::new(VecDeque::with_capacity(10_000)),
        }
    }

    pub fn record_produce(&self, records: usize, bytes: usize) {
        self.produce_records_total
            .fetch_add(records as u64, Ordering::Relaxed);
        self.produce_bytes_total
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn record_fetch(&self, records: usize, bytes: usize) {
        self.fetch_records_total
            .fetch_add(records as u64, Ordering::Relaxed);
        self.fetch_bytes_total
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn record_request(&self, latency: Duration, ok: bool) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        if !ok {
            self.request_errors_total.fetch_add(1, Ordering::Relaxed);
        }
        let ms = latency.as_millis() as u64;
        let mut lock = self.request_latency_ms.lock().expect("metrics mutex poisoned");
        if lock.len() >= 10_000 {
            lock.pop_front();
        }
        lock.push_back(ms);
    }

    fn p99_latency_ms(&self) -> u64 {
        let lock = self.request_latency_ms.lock().expect("metrics mutex poisoned");
        if lock.is_empty() {
            return 0;
        }
        let mut v: Vec<u64> = lock.iter().copied().collect();
        v.sort_unstable();
        let idx = ((v.len() as f64) * 0.99).floor() as usize;
        v[idx.min(v.len() - 1)]
    }

    pub fn render_prometheus(&self, broker: Option<&Broker>) -> String {
        let mut out = String::new();
        out.push_str("# TYPE thorstream_produce_records_total counter\n");
        out.push_str(&format!(
            "thorstream_produce_records_total {}\n",
            self.produce_records_total.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE thorstream_produce_bytes_total counter\n");
        out.push_str(&format!(
            "thorstream_produce_bytes_total {}\n",
            self.produce_bytes_total.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE thorstream_fetch_records_total counter\n");
        out.push_str(&format!(
            "thorstream_fetch_records_total {}\n",
            self.fetch_records_total.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE thorstream_fetch_bytes_total counter\n");
        out.push_str(&format!(
            "thorstream_fetch_bytes_total {}\n",
            self.fetch_bytes_total.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE thorstream_requests_total counter\n");
        out.push_str(&format!(
            "thorstream_requests_total {}\n",
            self.requests_total.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE thorstream_request_errors_total counter\n");
        out.push_str(&format!(
            "thorstream_request_errors_total {}\n",
            self.request_errors_total.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE thorstream_request_latency_p99_ms gauge\n");
        out.push_str(&format!(
            "thorstream_request_latency_p99_ms {}\n",
            self.p99_latency_ms()
        ));

        if let Some(broker) = broker {
            out.push_str("# TYPE thorstream_under_replicated_partitions gauge\n");
            out.push_str(&format!(
                "thorstream_under_replicated_partitions {}\n",
                broker.under_replicated_partitions()
            ));

            out.push_str("# TYPE thorstream_partition_size_bytes gauge\n");
            for (topic, partition, size) in broker.partition_sizes() {
                out.push_str(&format!(
                    "thorstream_partition_size_bytes{{topic=\"{}\",partition=\"{}\"}} {}\n",
                    topic, partition, size
                ));
            }

            out.push_str("# TYPE thorstream_consumer_lag gauge\n");
            for (group, topic, partition, lag) in broker.consumer_lag_metrics() {
                out.push_str(&format!(
                    "thorstream_consumer_lag{{group=\"{}\",topic=\"{}\",partition=\"{}\"}} {}\n",
                    group, topic, partition, lag
                ));
            }
        }

        out
    }
}

static OBS: OnceLock<Observability> = OnceLock::new();

pub fn observability() -> &'static Observability {
    OBS.get_or_init(Observability::new)
}
