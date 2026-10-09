//! Node identity. Each process picks a random id the first time it runs and,
//! with a data dir, keeps it across restarts. Bootstrap peers are given by
//! address only; their ids are learned by asking each one over HTTP.

use crate::net;
use crate::raft::Peer;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};
use std::hash::{BuildHasher, Hasher};
use std::path::{Path, PathBuf};
use std::time::Duration;

const DISCOVER_RETRY: Duration = Duration::from_millis(500);
/// Ids stay below 2^53 so tools that read JSON numbers as doubles (jq,
/// JavaScript) print them exactly.
const ID_MASK: u64 = (1 << 53) - 1;

pub(crate) struct Identity {
    pub id: u64,
    pub peers: Vec<Peer>,
}

/// Load this member's saved identity, or pick a random id and learn the ids
/// of `peers`. While discovering, `listener` answers `GET /cluster/id` so
/// peers starting at the same time can learn this member's id in turn.
pub(crate) async fn resolve(
    listener: &std::net::TcpListener,
    listen: &str,
    peers: &[String],
    data_dir: Option<&PathBuf>,
) -> std::io::Result<Identity> {
    let path = data_dir.map(|dir| identity_path(dir, listen));
    if let Some(identity) = path.as_deref().and_then(load) {
        tracing::info!(node = identity.id, "using saved node id {}", identity.id);
        return Ok(identity);
    }
    let id = random_id();
    tracing::info!(node = id, "assigned random node id {id}");
    let peers = if peers.is_empty() {
        Vec::new()
    } else {
        discover(listener, id, peers).await?
    };
    let identity = Identity { id, peers };
    if let Some(path) = &path {
        save(path, &identity)?;
    }
    Ok(identity)
}

fn random_id() -> u64 {
    // RandomState is seeded from the OS; the clock and pid guard against a
    // platform that seeds it poorly.
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or_default();
    hasher.write_u128(nanos);
    hasher.write_u32(std::process::id());
    (hasher.finish() & ID_MASK).max(1)
}

/// Members may share a data dir, so the file is keyed by listen address.
fn identity_path(dir: &Path, listen: &str) -> PathBuf {
    let name: String = listen
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    dir.join(format!("{name}.node.json"))
}

fn load(path: &Path) -> Option<Identity> {
    let value: Value = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
    let id = value.get("id")?.as_u64()?;
    let peers = value
        .get("peers")?
        .as_array()?
        .iter()
        .map(|peer| {
            Some(Peer {
                id: peer.get("id")?.as_u64()?,
                addr: peer.get("addr")?.as_str()?.to_string(),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(Identity { id, peers })
}

fn save(path: &Path, identity: &Identity) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let peers: Vec<Value> = identity
        .peers
        .iter()
        .map(|peer| json!({ "id": peer.id, "addr": peer.addr }))
        .collect();
    let body = json!({ "id": identity.id, "peers": peers });
    std::fs::write(path, serde_json::to_vec(&body).unwrap_or_default())
}

async fn discover(
    listener: &std::net::TcpListener,
    id: u64,
    addrs: &[String],
) -> std::io::Result<Vec<Peer>> {
    let announce = tokio::net::TcpListener::from_std(listener.try_clone()?)?;
    let app = Router::new().route(
        "/cluster/id",
        get(move || async move { Json(json!({ "id": id })) }),
    );
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let _ = axum::serve(announce, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await;
    });

    let mut peers = Vec::new();
    for addr in addrs {
        let peer_id = peer_id(addr).await;
        tracing::info!(node = id, peer = peer_id, %addr, "discovered peer");
        peers.push(Peer {
            id: peer_id,
            addr: addr.clone(),
        });
    }

    let _ = stop.send(());
    let _ = server.await;
    Ok(peers)
}

/// Ask `addr` for its id, retrying until it answers.
async fn peer_id(addr: &str) -> u64 {
    let mut attempts = 0u32;
    loop {
        let reply = net::call(addr, "GET", "/cluster/id", None).await;
        let id = reply.as_ref().ok().and_then(|(status, body)| {
            let value: Value = serde_json::from_slice(body).ok()?;
            (*status == 200).then(|| value.get("id")?.as_u64())?
        });
        if let Some(id) = id {
            return id;
        }
        if attempts % 10 == 0 {
            tracing::info!(%addr, "waiting for peer to come up");
        }
        attempts += 1;
        tokio::time::sleep(DISCOVER_RETRY).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_ids_differ_and_fit_in_a_double() {
        let a = random_id();
        let b = random_id();
        assert_ne!(a, b);
        assert!(a > 0 && a <= ID_MASK);
    }

    #[tokio::test]
    async fn peers_learn_each_others_ids() {
        let a = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let b = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        a.set_nonblocking(true).unwrap();
        b.set_nonblocking(true).unwrap();
        let addrs = vec![
            a.local_addr().unwrap().to_string(),
            b.local_addr().unwrap().to_string(),
        ];
        let (first, second) = tokio::join!(
            resolve(&a, &addrs[0], &addrs, None),
            resolve(&b, &addrs[1], &addrs, None),
        );
        let (first, second) = (first.unwrap(), second.unwrap());
        assert_ne!(first.id, second.id);
        let ids = |identity: &Identity| identity.peers.iter().map(|p| p.id).collect::<Vec<_>>();
        assert_eq!(ids(&first), vec![first.id, second.id]);
        assert_eq!(ids(&second), vec![first.id, second.id]);
    }

    #[tokio::test]
    async fn saved_identity_is_reused() {
        let dir = std::env::temp_dir().join(format!("kv-identity-{}", random_id()));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let listen = listener.local_addr().unwrap().to_string();
        let first = resolve(&listener, &listen, &[], Some(&dir)).await.unwrap();
        let again = resolve(&listener, &listen, &[], Some(&dir)).await.unwrap();
        assert_eq!(first.id, again.id);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
