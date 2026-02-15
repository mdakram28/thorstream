use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use tempfile::TempDir;
use thorstream::{Broker, BrokerConfig, Record};

fn bench_produce(c: &mut Criterion) {
    let mut group = c.benchmark_group("produce");
    for size in [128usize, 1024usize, 4096usize] {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let dir = TempDir::new().unwrap();
            let broker = Arc::new(
                Broker::new(BrokerConfig {
                    data_dir: dir.path().to_path_buf(),
                    ..Default::default()
                })
                .unwrap(),
            );
            broker.create_topic("bench", None).unwrap();
            let payload = vec![7u8; size];

            b.iter(|| {
                let _ = broker
                    .produce("bench", Some(0), Record::new(payload.clone()))
                    .unwrap();
            });
        });
    }
    group.finish();
}

fn bench_fetch(c: &mut Criterion) {
    let mut group = c.benchmark_group("fetch");

    let dir = TempDir::new().unwrap();
    let broker = Arc::new(
        Broker::new(BrokerConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        })
        .unwrap(),
    );
    broker.create_topic("bench", None).unwrap();

    for i in 0..2000 {
        let payload = format!("event-{}", i).into_bytes();
        let _ = broker
            .produce("bench", Some(0), Record::new(payload))
            .unwrap();
    }

    for records in [10usize, 100usize, 500usize] {
        group.throughput(Throughput::Elements(records as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(records),
            &records,
            |b, &records| {
                b.iter(|| {
                    let out = broker
                        .fetch("bench", 0, 0, 4 * 1024 * 1024, records)
                        .unwrap();
                    assert!(!out.is_empty());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_produce, bench_fetch);
criterion_main!(benches);
