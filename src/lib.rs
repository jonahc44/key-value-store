//! Distributed key-value store. A Raft group keeps the key space consistent
//! and partition tolerant: a write or linearizable read is acknowledged only
//! after a majority has accepted the leader. Members can be added at runtime.

mod api;
mod identity;
mod net;
mod raft;
mod telemetry;

use raft::{Config, Node};
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub use raft::{Config as ServerConfig, Peer};

const JOIN_RETRY: Duration = Duration::from_millis(500);

/// Command line settings. Unlike [`ServerConfig`], no ids are given: this
/// member picks its own and learns its peers' ids from their addresses.
#[derive(Clone, Debug)]
pub struct Options {
    pub listen: String,
    pub peers: Vec<String>,
    pub data_dir: Option<PathBuf>,
    pub join: Option<String>,
}

/// Resolve this member's id (random on first start, saved under the data
/// dir), learn the bootstrap peers' ids, then serve.
pub async fn run(options: Options) -> std::io::Result<()> {
    let listener = std::net::TcpListener::bind(&options.listen)?;
    listener.set_nonblocking(true)?;
    let identity = identity::resolve(
        &listener,
        &options.listen,
        &options.peers,
        options.data_dir.as_ref(),
    )
    .await?;
    let config = Config {
        id: identity.id,
        listen: options.listen,
        peers: identity.peers,
        data_dir: options.data_dir,
        join: options.join,
    };
    serve_on(tokio::net::TcpListener::from_std(listener)?, config).await
}

pub async fn serve(config: Config) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(&config.listen).await?;
    serve_on(listener, config).await
}

async fn serve_on(listener: tokio::net::TcpListener, config: Config) -> std::io::Result<()> {
    let addr = listener.local_addr()?;
    let members = if config.peers.is_empty() {
        1
    } else {
        config.peers.len()
    };
    let node_id = config.id;
    tracing::info!(
        node = node_id,
        %addr,
        members,
        "node id {node_id} listening on {addr}; see it in GET /cluster/members, \
         remove it with DELETE /cluster/members/{node_id}"
    );
    let join = config
        .join
        .clone()
        .map(|seed| (seed, config.listen.clone()));
    let node = Node::start(config);
    let app = api::router(Arc::clone(&node));
    if let Some((seed, advertise)) = join {
        tokio::spawn(async move {
            if let Err(err) = join_cluster(&seed, node_id, &advertise).await {
                tracing::error!(node = node_id, %seed, %err, "join rejected");
            }
        });
    }
    tokio::select! {
        result = axum::serve(listener, app) => {
            node.shutdown();
            tracing::info!(node = node_id, "server stopped");
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!(node = node_id, "shutting down");
            node.shutdown();
        }
    }
    Ok(())
}

/// Ask `seed` (any member) to add this node, retrying until the cluster
/// accepts it. Returns an error only if the cluster refuses the request.
async fn join_cluster(seed: &str, id: u64, addr: &str) -> Result<(), String> {
    let body = json!({ "id": id, "addr": addr });
    loop {
        match net::post(seed, "/cluster/members", &body, api::MEMBER_TIMEOUT).await {
            Ok((200, _)) => {
                tracing::info!(node = id, %seed, "joined cluster");
                return Ok(());
            }
            Ok((status, response)) if (400..500).contains(&status) => {
                return Err(String::from_utf8_lossy(&response).into_owned());
            }
            Ok((status, response)) => {
                let reason = String::from_utf8_lossy(&response);
                tracing::warn!(node = id, %seed, status, %reason, "join deferred; retrying");
            }
            Err(err) => {
                tracing::warn!(node = id, %seed, %err, "join request failed; retrying");
            }
        }
        tokio::time::sleep(JOIN_RETRY).await;
    }
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

    fn launch(listener: TcpListener, config: Config) -> Running {
        let addr = config.listen.clone();
        let node = Node::start(config);
        let app = api::router(Arc::clone(&node));
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Running { node, addr, server }
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
            running.push(launch(
                listener,
                Config {
                    id,
                    listen: addr,
                    peers: peers.clone(),
                    data_dir: data_dir.clone(),
                    join: None,
                },
            ));
        }
        running
    }

    /// Start a node in join mode and ask `seed` to admit it.
    async fn join(id: u64, seed: &str) -> Running {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let running = launch(
            listener,
            Config {
                id,
                listen: addr.clone(),
                peers: Vec::new(),
                data_dir: None,
                join: Some(seed.to_string()),
            },
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !running.node.is_leader().await,
            "a joining node must not elect itself"
        );
        tokio::time::timeout(
            Duration::from_secs(20),
            super::join_cluster(seed, id, &addr),
        )
        .await
        .expect("join timed out")
        .expect("join rejected");
        running
    }

    async fn voters(node: &Node) -> Vec<u64> {
        node.membership().await["voters"]
            .as_array()
            .unwrap()
            .iter()
            .map(|peer| peer["id"].as_u64().unwrap())
            .collect()
    }

    async fn wait_local(node: &Node, key: &str, value: &serde_json::Value) {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            if node.local_get(key).await.as_ref() == Some(value) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("{key} never reached node {}", node.id());
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

    #[tokio::test]
    async fn torn_log_line_is_dropped_on_restart() {
        let dir = std::env::temp_dir().join(format!(
            "kv-torn-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let nodes = cluster(1, Some(dir.clone())).await;
        let addr = nodes[0].addr.clone();
        assert_eq!(put(&addr, "a", json!({"v": 1})).await.0, 201);
        assert_eq!(put(&addr, "b", json!({"v": 2})).await.0, 201);
        nodes.into_iter().next().unwrap().stop().await;

        // A crash mid-append leaves half a line at the end of the log.
        let log = dir.join("1.log.jsonl");
        let mut torn = std::fs::read(&log).unwrap();
        torn.extend_from_slice(br#"{"term":1,"op":"put","ke"#);
        std::fs::write(&log, torn).unwrap();

        let nodes = cluster(1, Some(dir.clone())).await;
        let addr = nodes[0].addr.clone();
        assert_eq!(get(&addr, "b").await.1, json!({"v": 2}));
        assert_eq!(put(&addr, "c", json!({"v": 3})).await.0, 201);
        nodes.into_iter().next().unwrap().stop().await;

        let nodes = cluster(1, Some(dir.clone())).await;
        let addr = nodes[0].addr.clone();
        for (key, v) in [("a", 1), ("b", 2), ("c", 3)] {
            assert_eq!(get(&addr, key).await.1, json!({"v": v}), "{key}");
        }
        nodes.into_iter().next().unwrap().stop().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A new leader learns how far the old one committed only once it commits
    /// an entry of its own term. Until then its applied state can trail writes
    /// the old leader acknowledged, so it must not serve reads from it.
    #[tokio::test]
    async fn new_leader_does_not_read_before_committing_in_its_term() {
        let dir = std::env::temp_dir().join(format!(
            "kv-readindex-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        // Node 1 holds what the old leader (node 3, never started) committed
        // with it: k = old, filler, then k = new as the last entry. Node 1 had
        // only heard of commit 1, so it has applied k = old. Node 2 is empty,
        // so node 1 spends hundreds of rounds catching it up before its
        // no-op can commit, while node 2 answers every one of them.
        let filler = 20_000;
        let mut log = vec![
            json!({"term": 0, "op": "noop"}),
            json!({"term": 1, "op": "put", "key": "k", "value": "old"}),
        ];
        log.extend(
            (0..filler)
                .map(|n| json!({"term": 1, "op": "put", "key": format!("f{n}"), "value": n})),
        );
        log.push(json!({"term": 1, "op": "put", "key": "k", "value": "new"}));
        let lines: String = log.iter().map(|entry| format!("{entry}\n")).collect();
        std::fs::write(dir.join("1.log.jsonl"), lines).unwrap();
        std::fs::write(
            dir.join("1.json"),
            json!({"current_term": 1, "voted_for": 3, "commit_index": 1}).to_string(),
        )
        .unwrap();

        let mut listeners = Vec::new();
        let mut peers = Vec::new();
        for id in 1..=3 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            peers.push(Peer {
                id,
                addr: listener.local_addr().unwrap().to_string(),
            });
            listeners.push(listener);
        }
        listeners.pop(); // node 3 stays down
        let nodes: Vec<Running> = listeners
            .into_iter()
            .zip(&peers)
            .map(|(listener, peer)| {
                launch(
                    listener,
                    Config {
                        id: peer.id,
                        listen: peer.addr.clone(),
                        peers: peers.clone(),
                        data_dir: Some(dir.clone()),
                        join: None,
                    },
                )
            })
            .collect();

        let leader = wait_leader(&nodes).await;
        assert_eq!(
            nodes[leader].node.id(),
            1,
            "only node 1 has the committed entries"
        );
        let addr = nodes[leader].addr.clone();
        let start = Instant::now();
        let mut served = 0;
        while start.elapsed() < Duration::from_secs(10) {
            let (status, body) = get(&addr, "k").await;
            assert_ne!(body, json!("old"), "stale read: status {status}");
            if status == 200 {
                assert_eq!(body, json!("new"));
                served += 1;
                if served >= 20 {
                    break;
                }
            }
        }
        assert!(
            served >= 20,
            "reads never resumed after the no-op committed"
        );

        for node in nodes {
            node.stop().await;
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn node_added_at_runtime_becomes_voter() {
        let mut nodes = cluster(3, None).await;
        let leader = wait_leader(&nodes).await;
        assert_eq!(
            put(&nodes[leader].addr, "early", json!({"v": 1})).await.0,
            201
        );

        // Go through a follower so the request is forwarded to the leader.
        let seed = nodes[(leader + 1) % nodes.len()].addr.clone();
        let joiner = join(4, &seed).await;
        wait_local(&joiner.node, "early", &json!({"v": 1})).await;
        assert_eq!(voters(&nodes[leader].node).await, vec![1, 2, 3, 4]);

        nodes.push(joiner);
        let lost = nodes.remove(leader);
        lost.stop().await;

        // Three of four voters remain, so a write needs the new member's ack.
        let next = wait_leader(&nodes).await;
        let created = put(&nodes[next].addr, "late", json!({"v": 2})).await;
        assert_eq!(created.0, 201, "{created:?}");
        let joiner = nodes.iter().find(|n| n.node.id() == 4).unwrap();
        wait_local(&joiner.node, "late", &json!({"v": 2})).await;

        // Two of four is not a majority.
        let original = nodes.iter().position(|n| n.node.id() != 4).unwrap();
        nodes.remove(original).stop().await;
        let survivor = nodes[0].addr.clone();
        let (status, _) = put(&survivor, "split", json!({"v": 3})).await;
        assert_eq!(status, 503);

        for node in nodes {
            node.stop().await;
        }
    }

    #[tokio::test]
    async fn write_through_node_right_after_join() {
        let mut nodes = cluster(3, None).await;
        let leader = wait_leader(&nodes).await;
        let seed = nodes[(leader + 1) % nodes.len()].addr.clone();

        let joiner = join(4, &seed).await;
        let created = put(&joiner.addr, "fresh", json!({"from": 4})).await;
        assert_eq!(created.0, 201, "{created:?}");

        nodes.push(joiner);
        for node in &nodes {
            let (status, body) = get(&node.addr, "fresh").await;
            assert_eq!(status, 200, "node {}", node.node.id());
            assert_eq!(body, json!({"from": 4}));
        }

        for node in nodes {
            node.stop().await;
        }
    }

    #[tokio::test]
    async fn single_node_grows_and_rejects_conflicts() {
        let mut nodes = cluster(1, None).await;
        let seed = nodes[0].addr.clone();
        assert_eq!(put(&seed, "a", json!({"v": 1})).await.0, 201);

        let joiner = join(2, &seed).await;
        wait_local(&joiner.node, "a", &json!({"v": 1})).await;
        assert_eq!(voters(&nodes[0].node).await, vec![1, 2]);
        assert_eq!(voters(&joiner.node).await, vec![1, 2]);

        // Repeating the same join is a no-op; reusing the id elsewhere is not.
        super::join_cluster(&seed, 2, &joiner.addr).await.unwrap();
        let conflict = super::join_cluster(&seed, 2, "127.0.0.1:1").await;
        assert!(conflict.unwrap_err().contains("already exists"));

        // With two voters, losing the new one means no majority.
        joiner.stop().await;
        let (status, _) = put(&seed, "b", json!({"v": 2})).await;
        assert_eq!(status, 503);

        nodes.pop().unwrap().stop().await;
    }

    async fn remove(addr: &str, id: u64) -> (u16, serde_json::Value) {
        let (status, body) = call(addr, "DELETE", &format!("/cluster/members/{id}"), None)
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).unwrap_or_default())
    }

    async fn learners(node: &Node) -> Vec<u64> {
        node.membership().await["learners"]
            .as_array()
            .unwrap()
            .iter()
            .map(|peer| peer["id"].as_u64().unwrap())
            .collect()
    }

    async fn term(addr: &str) -> u64 {
        let (_, body) = call(addr, "GET", "/cluster/metrics", None).await.unwrap();
        String::from_utf8(body)
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("raft_term "))
            .and_then(|value| value.trim().parse().ok())
            .expect("raft_term gauge missing")
    }

    #[tokio::test]
    async fn removed_followers_shrink_quorum() {
        let mut nodes = cluster(3, None).await;
        let leader = wait_leader(&nodes).await;
        let leader_id = nodes[leader].node.id();
        let leader_addr = nodes[leader].addr.clone();
        assert_eq!(put(&leader_addr, "a", json!({"v": 1})).await.0, 201);
        let followers: Vec<usize> = (0..nodes.len()).filter(|i| *i != leader).collect();
        let (first, second) = (followers[0], followers[1]);
        let first_id = nodes[first].node.id();
        let second_id = nodes[second].node.id();

        // Ask the member that is about to go, so the call is forwarded.
        let (status, body) = remove(&nodes[first].addr, first_id).await;
        assert_eq!(status, 200, "{body:?}");
        let mut expected: Vec<u64> = vec![leader_id, second_id];
        expected.sort_unstable();
        assert_eq!(voters(&nodes[leader].node).await, expected);

        // The removed node keeps running. It is sent the configuration that
        // removed it, so it stops campaigning and cannot depose the leader.
        let removed = &nodes[first].node;
        let start = Instant::now();
        while voters(removed).await.contains(&first_id) {
            assert!(
                start.elapsed() < Duration::from_secs(3),
                "node {first_id} never learned it was removed"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!events_of(removed, "removed_from_cluster").await.is_empty());
        let elections = removed.telemetry().elections_started.get();
        let before = term(&leader_addr).await;
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(nodes[leader].node.is_leader().await);
        assert_eq!(term(&leader_addr).await, before);
        assert_eq!(removed.telemetry().elections_started.get(), elections);
        assert_eq!(put(&leader_addr, "b", json!({"v": 2})).await.0, 201);

        // Removing again is a no-op.
        assert_eq!(remove(&leader_addr, first_id).await.0, 200);

        let (status, body) = remove(&leader_addr, second_id).await;
        assert_eq!(status, 200, "{body:?}");
        assert_eq!(voters(&nodes[leader].node).await, vec![leader_id]);

        // With both removed nodes gone the leader is a quorum on its own;
        // under the original configuration this write would fail.
        let mut removed: Vec<Running> = Vec::new();
        for index in [first.max(second), first.min(second)] {
            removed.push(nodes.remove(index));
        }
        for node in removed {
            node.stop().await;
        }
        let created = put(&leader_addr, "c", json!({"v": 3})).await;
        assert_eq!(created.0, 201, "{created:?}");

        // The leader no longer reports the removed peers.
        let (_, body) = call(&leader_addr, "GET", "/cluster/metrics", None)
            .await
            .unwrap();
        let text = String::from_utf8(body).unwrap();
        assert!(!text.contains(&format!("peer=\"{first_id}\"")), "{text}");

        for node in nodes {
            node.stop().await;
        }
    }

    async fn events_of(node: &Node, kind: &str) -> Vec<serde_json::Value> {
        node.telemetry()
            .recent_events()
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["event"] == kind)
            .cloned()
            .collect()
    }

    #[tokio::test]
    async fn leader_removes_itself_and_hands_off() {
        let mut nodes = cluster(3, None).await;
        let leader = wait_leader(&nodes).await;
        let old_id = nodes[leader].node.id();
        assert_eq!(put(&nodes[leader].addr, "a", json!({"v": 1})).await.0, 201);

        // Go through a follower so the request is forwarded to the leader.
        let follower = nodes[(leader + 1) % nodes.len()].addr.clone();
        let (status, body) = remove(&follower, old_id).await;
        assert_eq!(status, 200, "{body:?}");
        assert!(!voters(&nodes[leader].node).await.contains(&old_id));

        let old = nodes.remove(leader);
        let next = wait_leader(&nodes).await;
        assert!(!old.node.is_leader().await);
        let stepped = events_of(&old.node, "stepped_down").await;
        assert!(
            stepped.iter().any(|event| event["reason"] == "removed"),
            "{stepped:?}"
        );
        let transferred = events_of(&old.node, "leadership_transferred").await;
        assert_eq!(
            transferred.last().map(|event| event["target"].as_u64()),
            Some(Some(nodes[next].node.id())),
            "{transferred:?}"
        );

        // The removed leader no longer counts toward quorum.
        old.stop().await;
        let created = put(&nodes[next].addr, "b", json!({"v": 2})).await;
        assert_eq!(created.0, 201, "{created:?}");
        for node in &nodes {
            wait_local(&node.node, "a", &json!({"v": 1})).await;
            wait_local(&node.node, "b", &json!({"v": 2})).await;
        }

        // Down to two voters, removing the leader leaves one that leads alone.
        let survivor_id = nodes[1 - next].node.id();
        let (status, body) = remove(&nodes[next].addr, nodes[next].node.id()).await;
        assert_eq!(status, 200, "{body:?}");
        let gone = nodes.remove(next);
        assert_eq!(wait_leader(&nodes).await, 0);
        gone.stop().await;
        assert_eq!(voters(&nodes[0].node).await, vec![survivor_id]);
        let created = put(&nodes[0].addr, "c", json!({"v": 3})).await;
        assert_eq!(created.0, 201, "{created:?}");

        // The last voter cannot go, and unknown ids are already removed.
        let (status, body) = remove(&nodes[0].addr, survivor_id).await;
        assert_eq!(status, 409, "{body:?}");
        assert_eq!(remove(&nodes[0].addr, 99).await.0, 200);

        for node in nodes {
            node.stop().await;
        }
    }

    #[tokio::test]
    async fn stuck_learner_can_be_removed() {
        let nodes = cluster(3, None).await;
        let leader = wait_leader(&nodes).await;
        let leader_addr = nodes[leader].addr.clone();

        // Nothing listens on port 1, so the learner never catches up.
        let body = serde_json::to_vec(&json!({"id": 4, "addr": "127.0.0.1:1"})).unwrap();
        let (status, _) = crate::net::forward(
            &leader_addr,
            "POST",
            "/cluster/members",
            Some(body),
            api::MEMBER_TIMEOUT,
        )
        .await
        .unwrap();
        assert_eq!(status, 503);
        assert_eq!(learners(&nodes[leader].node).await, vec![4]);

        let (status, body) = remove(&leader_addr, 4).await;
        assert_eq!(status, 200, "{body:?}");
        assert!(learners(&nodes[leader].node).await.is_empty());
        assert_eq!(voters(&nodes[leader].node).await, vec![1, 2, 3]);
        assert_eq!(put(&leader_addr, "k", json!({"v": 1})).await.0, 201);

        for node in nodes {
            node.stop().await;
        }
    }

    #[tokio::test]
    async fn dropped_peer_is_recorded() {
        let dir = std::env::temp_dir().join(format!(
            "kv-telemetry-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut nodes = cluster(3, Some(dir.clone())).await;
        let leader = wait_leader(&nodes).await;
        assert_eq!(put(&nodes[leader].addr, "k", json!({"v": 1})).await.0, 201);

        let follower = (leader + 1) % nodes.len();
        let lost_id = nodes[follower].node.id();
        nodes.remove(follower).stop().await;
        let leader = wait_leader(&nodes).await;
        let leader_node = &nodes[leader].node;

        let start = Instant::now();
        let dropped = loop {
            let events = leader_node.telemetry().recent_events();
            let found = events.as_array().unwrap().iter().any(|event| {
                event["event"] == "peer_down" && event["peer"].as_u64() == Some(lost_id)
            });
            if found || start.elapsed() > Duration::from_secs(5) {
                break found;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert!(dropped, "leader never reported node {lost_id} down");

        let (status, body) = call(&nodes[leader].addr, "GET", "/cluster/metrics", None)
            .await
            .unwrap();
        assert_eq!(status, 200);
        let text = String::from_utf8(body).unwrap();
        assert!(
            text.contains(&format!("raft_peer_up{{peer=\"{lost_id}\"}} 0")),
            "{text}"
        );
        assert!(text.contains(&format!("raft_peer_dropouts_total{{peer=\"{lost_id}\"}} 1")));
        assert!(text.contains("kv_requests_total{op=\"put\",code=\"201\"} 1"));
        assert!(text.contains("raft_role{role=\"leader\"} 1"));

        let leader_id = leader_node.id();
        for node in nodes {
            node.stop().await;
        }
        let journal =
            std::fs::read_to_string(dir.join(format!("{leader_id}.events.jsonl"))).unwrap();
        let kinds: Vec<String> = journal
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .map(|event| event["event"].as_str().unwrap().to_string())
            .collect();
        for expected in [
            "node_started",
            "leader_changed",
            "peer_down",
            "node_stopped",
        ] {
            assert!(
                kinds.iter().any(|k| k == expected),
                "{expected} missing: {kinds:?}"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
