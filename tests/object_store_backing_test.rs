use std::sync::{Mutex, OnceLock};

use tempfile::TempDir;
use thorstream::{Broker, BrokerConfig, Record};

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn restores_local_state_from_object_store_mirror() {
    let _guard = env_lock().lock().unwrap();

    let local1 = TempDir::new().unwrap();
    let local2 = TempDir::new().unwrap();
    let object_store = TempDir::new().unwrap();

    std::env::set_var("THORSTREAM_OBJECT_STORE_DIR", object_store.path());
    std::env::set_var("THORSTREAM_OBJECT_STORE_REQUIRED", "true");

    {
        let broker = Broker::new(BrokerConfig {
            data_dir: local1.path().to_path_buf(),
            ..Default::default()
        })
        .unwrap();

        broker.create_topic("events", None).unwrap();
        broker
            .produce("events", Some(0), Record::new(b"e1".to_vec()))
            .unwrap();
        broker
            .produce("events", Some(0), Record::new(b"e2".to_vec()))
            .unwrap();
    }

    {
        let broker = Broker::new(BrokerConfig {
            data_dir: local2.path().to_path_buf(),
            ..Default::default()
        })
        .unwrap();

        broker.create_topic("events", None).unwrap();
        let out = broker.fetch("events", 0, 0, 1024 * 1024, 10).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].record.value.as_slice(), b"e1");
        assert_eq!(out[1].record.value.as_slice(), b"e2");
    }

    std::env::remove_var("THORSTREAM_OBJECT_STORE_DIR");
    std::env::remove_var("THORSTREAM_OBJECT_STORE_REQUIRED");
}
