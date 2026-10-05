//! Distributed key-value store. A static Raft group keeps the key space
//! consistent and partition tolerant: a write or linearizable read is
//! acknowledged only after a majority has accepted the leader.

mod api;
mod net;
mod raft;

use raft::{Config, Node};
use std::sync::Arc;

pub use raft::{Config as ServerConfig, Peer};

pub async fn serve(config: Config) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(&config.listen).await?;
    let addr = listener.local_addr()?;
    let members = if config.peers.is_empty() {
        1
    } else {
        config.peers.len()
    };
    eprintln!(
        "key-value-store node {} listening on {addr} ({members} members)",
        config.id
    );
    let node = Node::start(config);
    let app = api::router(Arc::clone(&node));
    tokio::select! {
        result = axum::serve(listener, app) => {
            node.shutdown();
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            node.shutdown();
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Peer;
    use crate::api;
    use crate::net::call;
    use crate::raft::{Config, Node};
    use serde_json::json;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    struct Running {
        node: Arc<Node>,
        addr: String,
        server: JoinHandle<()>,
    }

    impl Running {
        async fn stop(self) {
            self.node.shutdown();
            self.server.abort();
            let _ = self.server.await;
        }
    }

    async fn cluster(size: u64, data_dir: Option<std::path::PathBuf>) -> Vec<Running> {
        let mut listeners = Vec::new();
        let mut peers = Vec::new();
        for id in 1..=size {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            peers.push(Peer { id, addr });
            listeners.push(listener);
        }
        let mut running = Vec::new();
        for (id, listener) in (1..=size).zip(listeners) {
            let addr = peers[(id - 1) as usize].addr.clone();
            let node = Node::start(Config {
                id,
                listen: addr.clone(),
                peers: peers.clone(),
                data_dir: data_dir.clone(),
            });
            let app = api::router(Arc::clone(&node));
            let server = tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            running.push(Running { node, addr, server });
        }
        running
    }

    async fn wait_leader(nodes: &[Running]) -> usize {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(8) {
            for (index, node) in nodes.iter().enumerate() {
                if node.node.is_leader().await {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("no leader elected");
    }

    async fn exchange(
        addr: &str,
        method: &str,
        key: &str,
        body: Option<Vec<u8>>,
    ) -> (u16, serde_json::Value) {
        let path = format!("/{key}");
        let mut last = String::from("no attempt");
        for _ in 0..50 {
            match call(addr, method, &path, body.clone()).await {
                Ok((status, bytes)) => {
                    let parsed = if bytes.is_empty() {
                        serde_json::Value::Null
                    } else {
                        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
                    };
                    return (status, parsed);
                }
                Err(err)
                    if err.contains("refused")
                        || err.contains("reset")
                        || err.contains("closed")
                        || err.contains("Connection") =>
                {
                    last = err;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(err) => panic!("request failed: {err}"),
            }
        }
        panic!("request failed: {last}");
    }

    async fn put(addr: &str, key: &str, value: serde_json::Value) -> (u16, serde_json::Value) {
        let body = serde_json::to_vec(&value).unwrap();
        exchange(addr, "PUT", key, Some(body)).await
    }

    async fn get(addr: &str, key: &str) -> (u16, serde_json::Value) {
        exchange(addr, "GET", key, None).await
    }

    async fn delete(addr: &str, key: &str) -> u16 {
        exchange(addr, "DELETE", key, None).await.0
    }

    #[tokio::test]
    async fn single_node_put_get_delete() {
        let mut nodes = cluster(1, None).await;
        let addr = nodes[0].addr.clone();
        let missing = get(&addr, "alpha").await;
        assert_eq!(missing.0, 404);

        let created = put(&addr, "alpha", json!({"n": 1})).await;
        assert_eq!(created.0, 201);
        assert_eq!(created.1, json!({"n": 1}));

        let found = get(&addr, "alpha").await;
        assert_eq!(found.0, 200);
        assert_eq!(found.1, json!({"n": 1}));

        let rejected = put(&addr, "alpha", json!([1, 2, 3])).await;
        assert_eq!(rejected.0, 400);

        assert_eq!(delete(&addr, "alpha").await, 204);
        assert_eq!(get(&addr, "alpha").await.0, 404);
        assert_eq!(delete(&addr, "alpha").await, 204);

        nodes.pop().unwrap().stop().await;
    }

    #[tokio::test]
    async fn cluster_replicates_and_forwards() {
        let nodes = cluster(3, None).await;
        let leader = wait_leader(&nodes).await;
        let follower = (leader + 1) % nodes.len();

        let created = put(&nodes[follower].addr, "widget", json!({"color": "red"})).await;
        assert_eq!(created.0, 201, "{created:?}");

        for node in &nodes {
            let (status, body) = get(&node.addr, "widget").await;
            assert_eq!(status, 200);
            assert_eq!(body, json!({"color": "red"}));
        }

        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            let mut stored = 0;
            for node in &nodes {
                if node.node.local_get("widget").await == Some(json!({"color": "red"})) {
                    stored += 1;
                }
            }
            if stored >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        let mut stored = 0;
        for node in &nodes {
            if node.node.local_get("widget").await == Some(json!({"color": "red"})) {
                stored += 1;
            }
        }
        assert!(stored >= 2, "committed value reached {stored} members");

        assert_eq!(delete(&nodes[leader].addr, "widget").await, 204);
        for node in &nodes {
            assert_eq!(get(&node.addr, "widget").await.0, 404);
        }

        for node in nodes {
            node.stop().await;
        }
    }

    #[tokio::test]
    async fn minority_cannot_commit_and_survivor_can() {
        let mut nodes = cluster(3, None).await;
        let leader = wait_leader(&nodes).await;
        assert_eq!(
            put(&nodes[leader].addr, "before", json!({"ok": true}))
                .await
                .0,
            201
        );

        let lost = nodes.remove(leader);
        lost.stop().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let next = wait_leader(&nodes).await;
        let created = put(&nodes[next].addr, "after", json!({"ok": true})).await;
        assert_eq!(created.0, 201, "{created:?}");
        assert_eq!(
            get(&nodes[(next + 1) % nodes.len()].addr, "before").await.0,
            200
        );
        assert_eq!(get(&nodes[next].addr, "after").await.1, json!({"ok": true}));

        let isolated = nodes.remove(next);
        for extra in nodes {
            extra.stop().await;
        }
        let (status, _) = put(&isolated.addr, "split", json!({"ok": false})).await;
        assert_eq!(status, 503);
        let (status, _) = get(&isolated.addr, "after").await;
        assert_eq!(status, 503);
        isolated.stop().await;
    }

    #[tokio::test]
    async fn data_dir_survives_restart() {
        let dir = std::env::temp_dir().join(format!(
            "kv-store-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let nodes = cluster(1, Some(dir.clone())).await;
        let addr = nodes[0].addr.clone();
        assert_eq!(put(&addr, "saved", json!({"v": 7})).await.0, 201);
        nodes.into_iter().next().unwrap().stop().await;

        let nodes = cluster(1, Some(dir.clone())).await;
        let (status, body) = get(&nodes[0].addr, "saved").await;
        assert_eq!(status, 200);
        assert_eq!(body, json!({"v": 7}));
        nodes.into_iter().next().unwrap().stop().await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
