//! Client HTTP API. Any member accepts a call: followers forward to the
//! current leader, and the leader answers only after Raft has committed the
//! write or confirmed it still holds a majority.

use crate::net;
use crate::raft::{Command, Node, Peer, RaftError};
use crate::telemetry::ClientOp;
use axum::body::{Body, Bytes};
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::{Duration, Instant};

const FORWARD_TIMEOUT: Duration = Duration::from_millis(2200);
/// Covers learner catch-up plus two configuration commits on the leader.
pub(crate) const MEMBER_TIMEOUT: Duration = Duration::from_secs(10);

pub fn router(node: Arc<Node>) -> Router {
    // Raft RPCs are most of the traffic a member serves, so they bypass the
    // per-request middleware.
    let client = Router::new()
        .route("/cluster/members", get(list_members).post(add_member))
        .route("/cluster/members/{id}", delete(remove_member))
        .route("/{key}", get(get_key).put(put_key).delete(delete_key))
        .route_layer(middleware::from_fn_with_state(Arc::clone(&node), track));
    Router::new()
        .route("/raft/request-vote", post(request_vote))
        .route("/raft/append-entries", post(append_entries))
        .route("/raft/timeout-now", post(timeout_now))
        .route("/cluster/metrics", get(metrics))
        .route("/cluster/events", get(events))
        .merge(client)
        .with_state(node)
}

/// Count and time client requests. Requests forwarded by another member are
/// tallied separately so the cluster-wide request total is not doubled.
async fn track(State(node): State<Arc<Node>>, request: Request, next: Next) -> Response {
    let Some(op) = ClientOp::classify(request.method().as_str(), request.uri().path()) else {
        return next.run(request).await;
    };
    if request.headers().contains_key("x-raft-forwarded") {
        node.telemetry().forwarded_received.inc();
        return next.run(request).await;
    }
    let started = Instant::now();
    let response = next.run(request).await;
    node.telemetry()
        .record_request(op, response.status().as_u16(), started.elapsed());
    response
}

async fn metrics(State(node): State<Arc<Node>>) -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        node.metrics_text().await,
    )
        .into_response()
}

async fn events(State(node): State<Arc<Node>>) -> Json<Value> {
    Json(node.telemetry().recent_events())
}

async fn request_vote(State(node): State<Arc<Node>>, Json(body): Json<Value>) -> Json<Value> {
    Json(node.handle_request_vote(&body).await)
}

async fn append_entries(State(node): State<Arc<Node>>, Json(body): Json<Value>) -> Json<Value> {
    Json(node.handle_append_entries(&body).await)
}

async fn timeout_now(State(node): State<Arc<Node>>, Json(body): Json<Value>) -> Json<Value> {
    Json(node.handle_timeout_now(&body).await)
}

async fn list_members(State(node): State<Arc<Node>>) -> Json<Value> {
    Json(node.membership().await)
}

#[tracing::instrument(skip_all, fields(node = node.id(), method = "POST", path = "/cluster/members"))]
async fn add_member(State(node): State<Arc<Node>>, headers: HeaderMap, body: Bytes) -> Response {
    let forwarded = headers.contains_key("x-raft-forwarded");
    let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let id = request.get("id").and_then(|v| v.as_u64());
    let addr = request.get("addr").and_then(|v| v.as_str());
    let (Some(id), Some(addr)) = (id, addr.filter(|addr| !addr.is_empty())) else {
        tracing::warn!("rejected member add; malformed body");
        return error(
            StatusCode::BAD_REQUEST,
            "body must look like {\"id\": N, \"addr\": \"HOST:PORT\"}",
        );
    };
    tracing::info!(member = id, %addr, "adding member");
    match node
        .add_member(Peer {
            id,
            addr: addr.to_string(),
        })
        .await
    {
        Ok(membership) => {
            tracing::info!(status = 200, member = id, "member added");
            (StatusCode::OK, Json(membership)).into_response()
        }
        Err(err) => {
            dispatch_error(
                &node,
                &err,
                forwarded,
                "POST",
                "/cluster/members",
                Some(body.to_vec()),
                MEMBER_TIMEOUT,
            )
            .await
        }
    }
}

#[tracing::instrument(skip_all, fields(node = node.id(), method = "DELETE", path = "/cluster/members", member = id))]
async fn remove_member(
    State(node): State<Arc<Node>>,
    Path(id): Path<u64>,
    headers: HeaderMap,
) -> Response {
    let forwarded = headers.contains_key("x-raft-forwarded");
    tracing::info!(member = id, "removing member");
    match node.remove_member(id).await {
        Ok(membership) => {
            tracing::info!(status = 200, member = id, "member removed");
            (StatusCode::OK, Json(membership)).into_response()
        }
        Err(err) => {
            dispatch_error(
                &node,
                &err,
                forwarded,
                "DELETE",
                &format!("/cluster/members/{id}"),
                None,
                FORWARD_TIMEOUT,
            )
            .await
        }
    }
}

#[tracing::instrument(skip_all, fields(node = node.id(), method = "GET", key = %key))]
async fn get_key(
    State(node): State<Arc<Node>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Response {
    let forwarded = headers.contains_key("x-raft-forwarded");
    match node.read(key.clone()).await {
        Ok(Some(value)) => {
            tracing::info!(status = 200, "client request");
            (StatusCode::OK, Json(value)).into_response()
        }
        Ok(None) => {
            tracing::info!(status = 404, "client request");
            error(StatusCode::NOT_FOUND, "not found")
        }
        Err(err) => {
            dispatch_error(
                &node,
                &err,
                forwarded,
                "GET",
                &key_path(&key),
                None,
                FORWARD_TIMEOUT,
            )
            .await
        }
    }
}

#[tracing::instrument(skip_all, fields(node = node.id(), method = "PUT", key = %key))]
async fn put_key(
    State(node): State<Arc<Node>>,
    Path(key): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let forwarded = headers.contains_key("x-raft-forwarded");
    let value: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(%err, "rejected put; body is not JSON");
            return error(StatusCode::BAD_REQUEST, "body must be JSON");
        }
    };
    if !value.is_object() {
        tracing::warn!("rejected put; body is not a JSON object");
        return error(StatusCode::BAD_REQUEST, "body must be a JSON object");
    }
    match node
        .propose(Command::Put {
            key: key.clone(),
            value: value.clone(),
        })
        .await
    {
        Ok(()) => {
            tracing::info!(status = 201, "client request");
            (StatusCode::CREATED, Json(value)).into_response()
        }
        Err(err) => {
            dispatch_error(
                &node,
                &err,
                forwarded,
                "PUT",
                &key_path(&key),
                Some(body.to_vec()),
                FORWARD_TIMEOUT,
            )
            .await
        }
    }
}

#[tracing::instrument(skip_all, fields(node = node.id(), method = "DELETE", key = %key))]
async fn delete_key(
    State(node): State<Arc<Node>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Response {
    let forwarded = headers.contains_key("x-raft-forwarded");
    match node.propose(Command::Delete { key: key.clone() }).await {
        Ok(()) => {
            tracing::info!(status = 204, "client request");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(err) => {
            dispatch_error(
                &node,
                &err,
                forwarded,
                "DELETE",
                &key_path(&key),
                None,
                FORWARD_TIMEOUT,
            )
            .await
        }
    }
}

async fn dispatch_error(
    node: &Node,
    err: &RaftError,
    forwarded: bool,
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
    timeout: Duration,
) -> Response {
    match err {
        RaftError::NotLeader { leader: Some(addr) } if !forwarded => {
            tracing::debug!(leader = %addr, "forwarding client request");
            match net::forward(addr, method, path, body, timeout).await {
                Ok((status, response)) => {
                    node.telemetry().forwards_ok.inc();
                    tracing::info!(status, leader = %addr, "forwarded client request");
                    passthrough(status, response)
                }
                Err(err) => {
                    node.telemetry().forwards_failed.inc();
                    tracing::warn!(leader = %addr, %err, "leader unreachable");
                    error(StatusCode::SERVICE_UNAVAILABLE, "leader unreachable")
                }
            }
        }
        RaftError::NotLeader { leader: None } => {
            tracing::warn!("client request failed; no leader");
            error(StatusCode::SERVICE_UNAVAILABLE, "no leader")
        }
        RaftError::NotLeader { .. } | RaftError::Unavailable => {
            tracing::warn!("client request failed; quorum unavailable");
            error(StatusCode::SERVICE_UNAVAILABLE, "unavailable")
        }
        RaftError::Busy(message) => {
            tracing::warn!(reason = %message, "client request deferred");
            error(StatusCode::SERVICE_UNAVAILABLE, message)
        }
        RaftError::Rejected(message) => {
            tracing::warn!(reason = %message, "client request rejected");
            error(StatusCode::CONFLICT, message)
        }
    }
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

fn passthrough(status: u16, body: Vec<u8>) -> Response {
    let code = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    let builder = Response::builder().status(code);
    let builder = if body.is_empty() {
        builder
    } else {
        builder.header("content-type", "application/json")
    };
    builder
        .body(Body::from(body))
        .unwrap_or_else(|_| (StatusCode::BAD_GATEWAY, "bad response").into_response())
}

fn key_path(key: &str) -> String {
    format!("/{}", encode_key(key))
}

fn encode_key(key: &str) -> String {
    let mut out = String::new();
    for byte in key.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}
