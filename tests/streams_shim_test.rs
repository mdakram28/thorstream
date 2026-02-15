use std::sync::Arc;
use tempfile::TempDir;
use thorstream::{Broker, BrokerConfig, Record};

#[test]
fn streams_shim_filters_and_maps() {
    let dir = TempDir::new().unwrap();
    let broker = Arc::new(
        Broker::new(BrokerConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        })
        .unwrap(),
    );

    broker.create_topic("in", None).unwrap();
    broker.create_topic("out", None).unwrap();

    broker
        .produce("in", Some(0), Record::new(b"keep-1".to_vec()))
        .unwrap();
    broker
        .produce("in", Some(0), Record::new(b"drop-1".to_vec()))
        .unwrap();
    broker
        .produce("in", Some(0), Record::new(b"keep-2".to_vec()))
        .unwrap();

    let mut task = thorstream::streams_shim::StreamsBuilder::new(Arc::clone(&broker))
        .stream("in")
        .filter_values(|value| value.starts_with(b"keep"))
        .map_values(|value| {
            let mut out = b"processed:".to_vec();
            out.extend_from_slice(value);
            out
        })
        .to("out");

    let processed = task.run_once(100).unwrap();
    assert_eq!(processed, 2);

    let out = broker.fetch("out", 0, 0, 1024 * 1024, 100).unwrap();
    let values: Vec<Vec<u8>> = out.into_iter().map(|r| r.record.value).collect();
    assert_eq!(
        values,
        vec![b"processed:keep-1".to_vec(), b"processed:keep-2".to_vec()]
    );
}
