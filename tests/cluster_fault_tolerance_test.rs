use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use thorstream::broker::BrokerConfig;
use thorstream::cluster;
use thorstream::server;
use thorstream::types::Record;
use thorstream::Broker;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_plane_elects_and_fails_over_leader() {
    let listeners = vec![
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
    ];
    let addrs: Vec<String> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().to_string())
        .collect();

    let mut brokers: Vec<Arc<Broker>> = Vec::new();
    let mut _dirs: Vec<TempDir> = Vec::new();
    for node_id in 0..3i32 {
        let dir = tempfile::tempdir().unwrap();
        let mut peers = HashMap::new();
        for peer_id in 0..3i32 {
            if peer_id != node_id {
                peers.insert(peer_id, addrs[peer_id as usize].clone());
            }
        }
        let cfg = BrokerConfig {
            data_dir: dir.path().to_path_buf(),
            node_id,
            peers,
            ..Default::default()
        };
        brokers.push(Arc::new(Broker::new(cfg).unwrap()));
        _dirs.push(dir);
    }

    let mut server_tasks = Vec::new();
    let mut cp_tasks = Vec::new();
    for (idx, listener) in listeners.into_iter().enumerate() {
        let b = Arc::clone(&brokers[idx]);
        server_tasks.push(tokio::spawn(async move {
            let _ = server::run_server_on_listener(b, listener).await;
        }));
    }
    for b in &brokers {
        let cp_b = Arc::clone(b);
        cp_tasks.push(tokio::spawn(async move {
            cluster::run_control_plane(cp_b).await;
        }));
    }

    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        brokers[0].is_leader(),
        "expected node 0 to be elected leader"
    );

    server_tasks[0].abort();
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(
        brokers[1].is_leader() || brokers[2].is_leader(),
        "expected a new leader after node 0 failure"
    );

    for t in server_tasks {
        t.abort();
    }
    for t in cp_tasks {
        t.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replication_rpc_applies_record_on_follower() {
    let follower_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let follower_addr = follower_listener.local_addr().unwrap().to_string();

    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();

    let mut leader_peers = HashMap::new();
    leader_peers.insert(1, follower_addr.clone());

    let leader = Arc::new(
        Broker::new(BrokerConfig {
            data_dir: leader_dir.path().to_path_buf(),
            node_id: 0,
            peers: leader_peers,
            ..Default::default()
        })
        .unwrap(),
    );

    let follower = Arc::new(
        Broker::new(BrokerConfig {
            data_dir: follower_dir.path().to_path_buf(),
            node_id: 1,
            peers: HashMap::new(),
            ..Default::default()
        })
        .unwrap(),
    );

    let follower_server = {
        let b = Arc::clone(&follower);
        tokio::spawn(async move {
            let _ = server::run_server_on_listener(b, follower_listener).await;
        })
    };

    tokio::time::sleep(Duration::from_millis(200)).await;

    let record = Record::new(b"replicated-value".to_vec());
    let (_, offset) = leader.produce("r-topic", Some(0), record.clone()).unwrap();
    let ack = cluster::replicate_to_peer(&follower_addr, "r-topic", 0, offset, &record).unwrap();
    assert_eq!(ack, offset);

    let fetched = follower.fetch("r-topic", 0, 0, 1024, 10).unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].record.value.as_slice(), b"replicated-value");

    follower_server.abort();
}
