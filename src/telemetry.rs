//! Reliability and performance telemetry.
//!
//! Hot paths only touch atomics or a briefly held, uncontended mutex. Three
//! outputs are derived from that state:
//!
//! * `GET /cluster/metrics`: Prometheus text format, for scraping into a time
//!   series database.
//! * `{data_dir}/{id}.events.jsonl`: an append-only journal of notable events
//!   (peers dropping out and returning, elections, leader changes, ...) plus a
//!   periodic `stats` snapshot, so history survives without external tooling.
//!   A background thread does the file I/O; if it falls behind, events are
//!   dropped and counted instead of blocking Raft.
//! * `GET /cluster/events`: the most recent events, kept in memory.
//!
//! Counters are cumulative since process start; compare successive snapshots
//! to get rates.

use serde_json::{Map, Value, json};
use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, mpsc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const RECENT_EVENTS: usize = 256;
const JOURNAL_QUEUE: usize = 1024;
const JOURNAL_MAX_BYTES: u64 = 16 * 1024 * 1024;
const FLUSH_WAIT: Duration = Duration::from_millis(500);
/// A peer is reported down once this many consecutive RPCs have failed over
/// at least `PEER_DOWN_AFTER`, which is longer than any election timeout.
const PEER_DOWN_FAILURES: u32 = 3;
const PEER_DOWN_AFTER: Duration = Duration::from_secs(1);

/// Upper bounds in seconds, chosen to span loopback RPCs through timeouts.
const BUCKETS: [f64; 14] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(Default)]
pub(crate) struct Counter(AtomicU64);

impl Counter {
    pub(crate) fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

pub(crate) struct Histogram {
    buckets: [AtomicU64; BUCKETS.len()],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; BUCKETS.len()],
            count: AtomicU64::new(0),
            sum_micros: AtomicU64::new(0),
        }
    }
}

impl Histogram {
    pub(crate) fn observe(&self, elapsed: Duration) {
        let secs = elapsed.as_secs_f64();
        if let Some(slot) = BUCKETS.iter().position(|bound| secs <= *bound) {
            self.buckets[slot].fetch_add(1, Ordering::Relaxed);
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_micros
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
    }

    fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Upper bound of the bucket holding quantile `q`, in milliseconds.
    fn quantile_ms(&self, q: f64) -> Option<f64> {
        let count = self.count();
        if count == 0 {
            return None;
        }
        let target = ((count as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (bound, bucket) in BUCKETS.iter().zip(&self.buckets) {
            seen += bucket.load(Ordering::Relaxed);
            if seen >= target {
                return Some(bound * 1000.0);
            }
        }
        Some(f64::INFINITY)
    }

    fn summary(&self) -> Value {
        let count = self.count();
        let mean = if count == 0 {
            None
        } else {
            Some(self.sum_micros.load(Ordering::Relaxed) as f64 / count as f64 / 1000.0)
        };
        json!({
            "count": count,
            "mean_ms": mean,
            "p50_ms": self.quantile_ms(0.5).filter(|v| v.is_finite()),
            "p99_ms": self.quantile_ms(0.99).filter(|v| v.is_finite()),
        })
    }

    fn render(&self, out: &mut String, name: &str, labels: &str) {
        let sep = if labels.is_empty() { "" } else { "," };
        let mut cumulative = 0;
        for (bound, bucket) in BUCKETS.iter().zip(&self.buckets) {
            cumulative += bucket.load(Ordering::Relaxed);
            let _ = writeln!(
                out,
                "{name}_bucket{{{labels}{sep}le=\"{bound}\"}} {cumulative}"
            );
        }
        let count = self.count();
        let sum = self.sum_micros.load(Ordering::Relaxed) as f64 / 1e6;
        let _ = writeln!(out, "{name}_bucket{{{labels}{sep}le=\"+Inf\"}} {count}");
        let braced = if labels.is_empty() {
            String::new()
        } else {
            format!("{{{labels}}}")
        };
        let _ = writeln!(out, "{name}_sum{braced} {sum}");
        let _ = writeln!(out, "{name}_count{braced} {count}");
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ClientOp {
    Get,
    Put,
    Delete,
    ListMembers,
    AddMember,
    RemoveMember,
}

impl ClientOp {
    const ALL: [ClientOp; 6] = [
        ClientOp::Get,
        ClientOp::Put,
        ClientOp::Delete,
        ClientOp::ListMembers,
        ClientOp::AddMember,
        ClientOp::RemoveMember,
    ];

    /// The client operation a request performs, or `None` for internal and
    /// telemetry endpoints.
    pub(crate) fn classify(method: &str, path: &str) -> Option<Self> {
        if path == "/cluster/members" {
            return match method {
                "GET" => Some(ClientOp::ListMembers),
                "POST" => Some(ClientOp::AddMember),
                _ => None,
            };
        }
        if path.starts_with("/cluster/members/") {
            return (method == "DELETE").then_some(ClientOp::RemoveMember);
        }
        if path.starts_with("/raft/") || path.starts_with("/cluster/") {
            return None;
        }
        match method {
            "GET" => Some(ClientOp::Get),
            "PUT" => Some(ClientOp::Put),
            "DELETE" => Some(ClientOp::Delete),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            ClientOp::Get => "get",
            ClientOp::Put => "put",
            ClientOp::Delete => "delete",
            ClientOp::ListMembers => "list_members",
            ClientOp::AddMember => "add_member",
            ClientOp::RemoveMember => "remove_member",
        }
    }
}

/// Status codes the API can return; anything else is counted as "other".
const CODES: [u16; 7] = [200, 201, 204, 400, 404, 409, 503];
const CODE_SLOTS: usize = CODES.len() + 1;

#[derive(Default)]
struct PeerStats {
    addr: String,
    up: Option<bool>,
    consecutive_failures: u32,
    first_failure: Option<Instant>,
    last_ok: Option<Instant>,
    down_since: Option<Instant>,
    rpc_ok: u64,
    rpc_timeout: u64,
    rpc_error: u64,
    dropouts: u64,
    latency: Histogram,
}

enum PeerTransition {
    Down {
        failures: u32,
        last_contact_ms: Option<u128>,
    },
    Up {
        down_ms: u128,
    },
}

/// Raft state sampled under the node lock when metrics are read.
pub(crate) struct Gauges {
    pub(crate) role: &'static str,
    pub(crate) term: u64,
    pub(crate) leader: Option<u64>,
    pub(crate) commit_index: usize,
    pub(crate) applied_index: usize,
    pub(crate) last_index: usize,
    pub(crate) snapshot_index: usize,
    /// Log entries held after the log base.
    pub(crate) log_entries: usize,
    pub(crate) voters: usize,
    pub(crate) learners: usize,
    pub(crate) keys: usize,
    /// Leader only: `(peer, match_index)` for every other member.
    pub(crate) peer_match: Vec<(u64, usize)>,
}

enum JournalMsg {
    Line(String),
    Flush(mpsc::SyncSender<()>),
}

pub(crate) struct Telemetry {
    node: u64,
    started: Instant,
    started_unix: f64,

    pub(crate) elections_started: Counter,
    pub(crate) prevotes_failed: Counter,
    pub(crate) elections_won: Counter,
    pub(crate) leader_changes: Counter,
    pub(crate) step_downs: Counter,
    pub(crate) term_changes: Counter,
    pub(crate) proposals: Counter,
    pub(crate) proposal_timeouts: Counter,
    pub(crate) proposals_lost_leadership: Counter,
    pub(crate) read_quorum_failures: Counter,
    pub(crate) log_truncations: Counter,
    pub(crate) snapshots_taken: Counter,
    pub(crate) snapshots_sent: Counter,
    pub(crate) snapshots_installed: Counter,
    pub(crate) persist_failures: Counter,
    pub(crate) forwards_ok: Counter,
    pub(crate) forwards_failed: Counter,
    pub(crate) forwarded_received: Counter,
    events_dropped: Counter,

    pub(crate) commit_latency: Histogram,
    pub(crate) persist_duration: Histogram,
    requests: [[Counter; CODE_SLOTS]; ClientOp::ALL.len()],
    request_duration: [Histogram; ClientOp::ALL.len()],

    persist_failing: AtomicBool,
    peers: Mutex<HashMap<u64, PeerStats>>,
    recent: Mutex<VecDeque<Value>>,
    journal: Option<mpsc::SyncSender<JournalMsg>>,
}

impl Telemetry {
    pub(crate) fn new(node: u64, data_dir: Option<&PathBuf>) -> Self {
        let journal =
            data_dir.and_then(|dir| spawn_journal(node, dir.join(format!("{node}.events.jsonl"))));
        Self {
            node,
            started: Instant::now(),
            started_unix: unix_secs(),
            elections_started: Counter::default(),
            prevotes_failed: Counter::default(),
            elections_won: Counter::default(),
            leader_changes: Counter::default(),
            step_downs: Counter::default(),
            term_changes: Counter::default(),
            proposals: Counter::default(),
            proposal_timeouts: Counter::default(),
            proposals_lost_leadership: Counter::default(),
            read_quorum_failures: Counter::default(),
            log_truncations: Counter::default(),
            snapshots_taken: Counter::default(),
            snapshots_sent: Counter::default(),
            snapshots_installed: Counter::default(),
            persist_failures: Counter::default(),
            forwards_ok: Counter::default(),
            forwards_failed: Counter::default(),
            forwarded_received: Counter::default(),
            events_dropped: Counter::default(),
            commit_latency: Histogram::default(),
            persist_duration: Histogram::default(),
            requests: Default::default(),
            request_duration: Default::default(),
            persist_failing: AtomicBool::new(false),
            peers: Mutex::new(HashMap::new()),
            recent: Mutex::new(VecDeque::with_capacity(RECENT_EVENTS)),
            journal,
        }
    }

    /// Record a notable event. Never blocks on I/O.
    pub(crate) fn event(&self, kind: &str, fields: Value) {
        let mut record = Map::new();
        record.insert("ts_ms".into(), json!(unix_millis()));
        record.insert("node".into(), json!(self.node));
        record.insert("event".into(), json!(kind));
        if let Value::Object(fields) = fields {
            record.extend(fields);
        }
        let record = Value::Object(record);
        if let Some(journal) = &self.journal {
            let line = record.to_string();
            if journal.try_send(JournalMsg::Line(line)).is_err() {
                self.events_dropped.inc();
            }
        }
        // Periodic snapshots would crowd real events out of the short buffer.
        if kind != "stats" {
            let mut recent = lock(&self.recent);
            if recent.len() == RECENT_EVENTS {
                recent.pop_front();
            }
            recent.push_back(record);
        }
    }

    pub(crate) fn recent_events(&self) -> Value {
        Value::Array(lock(&self.recent).iter().cloned().collect())
    }

    /// Wait briefly for the journal to reach disk. Used at shutdown.
    pub(crate) fn flush(&self) {
        let Some(journal) = &self.journal else {
            return;
        };
        let (tx, rx) = mpsc::sync_channel(1);
        if journal.try_send(JournalMsg::Flush(tx)).is_ok() {
            let _ = rx.recv_timeout(FLUSH_WAIT);
        }
    }

    pub(crate) fn record_request(&self, op: ClientOp, status: u16, elapsed: Duration) {
        let slot = CODES
            .iter()
            .position(|code| *code == status)
            .unwrap_or(CODES.len());
        self.requests[op as usize][slot].inc();
        self.request_duration[op as usize].observe(elapsed);
    }

    pub(crate) fn record_persist(&self, elapsed: Duration, ok: bool, error: Option<&str>) {
        self.persist_duration.observe(elapsed);
        if ok {
            if self.persist_failing.swap(false, Ordering::Relaxed) {
                self.event("persist_recovered", json!({}));
            }
        } else {
            self.persist_failures.inc();
            if !self.persist_failing.swap(true, Ordering::Relaxed) {
                self.event("persist_failed", json!({ "error": error }));
            }
        }
    }

    /// Drop the stats of a peer that left the configuration, so it is not
    /// reported as down forever.
    pub(crate) fn forget_peer(&self, peer: u64) {
        lock(&self.peers).remove(&peer);
    }

    /// Record the outcome of one Raft RPC to `peer` and report the peer
    /// dropping out or coming back.
    pub(crate) fn record_peer_rpc<T>(
        &self,
        peer: u64,
        addr: &str,
        elapsed: Duration,
        result: &Result<T, String>,
    ) {
        let now = Instant::now();
        let transition = {
            let mut peers = lock(&self.peers);
            let stats = peers.entry(peer).or_default();
            if stats.addr != addr {
                stats.addr = addr.to_string();
            }
            match result {
                Ok(_) => {
                    stats.rpc_ok += 1;
                    stats.latency.observe(elapsed);
                    stats.consecutive_failures = 0;
                    stats.first_failure = None;
                    stats.last_ok = Some(now);
                    let was_down = stats.up == Some(false);
                    stats.up = Some(true);
                    match stats.down_since.take() {
                        Some(since) if was_down => Some(PeerTransition::Up {
                            down_ms: now.duration_since(since).as_millis(),
                        }),
                        _ => None,
                    }
                }
                Err(err) => {
                    if err == "timeout" {
                        stats.rpc_timeout += 1;
                    } else {
                        stats.rpc_error += 1;
                    }
                    stats.consecutive_failures += 1;
                    let first = *stats.first_failure.get_or_insert(now);
                    if stats.up != Some(false)
                        && stats.consecutive_failures >= PEER_DOWN_FAILURES
                        && now.duration_since(first) >= PEER_DOWN_AFTER
                    {
                        stats.up = Some(false);
                        stats.down_since = Some(first);
                        stats.dropouts += 1;
                        Some(PeerTransition::Down {
                            failures: stats.consecutive_failures,
                            last_contact_ms: stats
                                .last_ok
                                .map(|at| now.duration_since(at).as_millis()),
                        })
                    } else {
                        None
                    }
                }
            }
        };
        match transition {
            Some(PeerTransition::Down {
                failures,
                last_contact_ms,
            }) => {
                let error = result.as_ref().err().map(String::as_str).unwrap_or("");
                tracing::warn!(
                    node = self.node,
                    peer,
                    %addr,
                    failures,
                    last_contact_ms,
                    %error,
                    "peer dropped out"
                );
                self.event(
                    "peer_down",
                    json!({
                        "peer": peer,
                        "addr": addr,
                        "consecutive_failures": failures,
                        "last_contact_ms_ago": last_contact_ms,
                        "error": error,
                    }),
                );
            }
            Some(PeerTransition::Up { down_ms }) => {
                tracing::info!(node = self.node, peer, %addr, down_ms, "peer reachable again");
                self.event(
                    "peer_up",
                    json!({ "peer": peer, "addr": addr, "down_ms": down_ms }),
                );
            }
            None => {}
        }
    }

    /// Compact cumulative summary written to the journal periodically.
    pub(crate) fn snapshot(&self, gauges: &Gauges) -> Value {
        let mut requests = Map::new();
        for op in ClientOp::ALL {
            let counts = &self.requests[op as usize];
            let total: u64 = counts.iter().map(Counter::get).sum();
            if total == 0 {
                continue;
            }
            let unavailable = counts[CODES.iter().position(|c| *c == 503).unwrap_or(0)].get();
            let mut entry = self.request_duration[op as usize].summary();
            entry["unavailable"] = json!(unavailable);
            requests.insert(op.name().into(), entry);
        }
        let peers: Vec<Value> = {
            let peers = lock(&self.peers);
            let mut ids: Vec<_> = peers.keys().copied().collect();
            ids.sort_unstable();
            ids.iter()
                .map(|id| {
                    let stats = &peers[id];
                    json!({
                        "peer": id,
                        "up": stats.up,
                        "rpc_ok": stats.rpc_ok,
                        "rpc_timeout": stats.rpc_timeout,
                        "rpc_error": stats.rpc_error,
                        "dropouts": stats.dropouts,
                        "last_contact_ms_ago": stats.last_ok.map(|at| at.elapsed().as_millis()),
                        "rpc_latency": stats.latency.summary(),
                    })
                })
                .collect()
        };
        json!({
            "uptime_s": self.started.elapsed().as_secs(),
            "role": gauges.role,
            "term": gauges.term,
            "leader": gauges.leader,
            "commit_index": gauges.commit_index,
            "applied_index": gauges.applied_index,
            "log_entries": gauges.last_index,
            "snapshot_index": gauges.snapshot_index,
            "log_entries_held": gauges.log_entries,
            "snapshots_taken": self.snapshots_taken.get(),
            "snapshots_installed": self.snapshots_installed.get(),
            "keys": gauges.keys,
            "voters": gauges.voters,
            "learners": gauges.learners,
            "requests": requests,
            "commit_latency": self.commit_latency.summary(),
            "persist_duration": self.persist_duration.summary(),
            "elections_started": self.elections_started.get(),
            "elections_won": self.elections_won.get(),
            "leader_changes": self.leader_changes.get(),
            "step_downs": self.step_downs.get(),
            "proposal_timeouts": self.proposal_timeouts.get(),
            "read_quorum_failures": self.read_quorum_failures.get(),
            "forwards_failed": self.forwards_failed.get(),
            "persist_failures": self.persist_failures.get(),
            "events_dropped": self.events_dropped.get(),
            "peers": peers,
        })
    }

    /// Prometheus text exposition format, version 0.0.4.
    pub(crate) fn render(&self, gauges: &Gauges) -> String {
        let mut out = String::with_capacity(16 * 1024);
        let o = &mut out;

        header(
            o,
            "kv_start_time_seconds",
            "gauge",
            "Unix time the process started.",
        );
        let _ = writeln!(o, "kv_start_time_seconds {}", self.started_unix);
        header(
            o,
            "kv_uptime_seconds",
            "gauge",
            "Seconds since the process started.",
        );
        let _ = writeln!(
            o,
            "kv_uptime_seconds {}",
            self.started.elapsed().as_secs_f64()
        );
        header(o, "kv_keys", "gauge", "Keys in the applied state machine.");
        let _ = writeln!(o, "kv_keys {}", gauges.keys);

        header(
            o,
            "raft_role",
            "gauge",
            "1 for the role this member currently holds.",
        );
        for role in ["follower", "candidate", "leader"] {
            let _ = writeln!(
                o,
                "raft_role{{role=\"{role}\"}} {}",
                u8::from(gauges.role == role)
            );
        }
        gauge(o, "raft_term", "Current Raft term.", gauges.term);
        header(
            o,
            "raft_leader_id",
            "gauge",
            "Leader this member follows; 0 when unknown.",
        );
        let _ = writeln!(o, "raft_leader_id {}", gauges.leader.unwrap_or(0));
        gauge(
            o,
            "raft_commit_index",
            "Highest committed log index.",
            gauges.commit_index,
        );
        gauge(
            o,
            "raft_applied_index",
            "Highest applied log index.",
            gauges.applied_index,
        );
        gauge(
            o,
            "raft_last_log_index",
            "Index of the last log entry.",
            gauges.last_index,
        );
        gauge(
            o,
            "raft_snapshot_index",
            "Last log index covered by this member's snapshot.",
            gauges.snapshot_index,
        );
        gauge(
            o,
            "raft_log_entries",
            "Log entries held in memory, after the compacted prefix.",
            gauges.log_entries,
        );
        gauge(
            o,
            "raft_voters",
            "Voting members in the configuration.",
            gauges.voters,
        );
        gauge(
            o,
            "raft_learners",
            "Non-voting members in the configuration.",
            gauges.learners,
        );

        counter(
            o,
            "raft_elections_started_total",
            "Elections this member started (prevote phase).",
            &self.elections_started,
        );
        counter(
            o,
            "raft_prevotes_failed_total",
            "Elections abandoned for lack of prevotes.",
            &self.prevotes_failed,
        );
        counter(
            o,
            "raft_elections_won_total",
            "Times this member became leader.",
            &self.elections_won,
        );
        counter(
            o,
            "raft_leader_changes_total",
            "Times this member learned of a new leader.",
            &self.leader_changes,
        );
        counter(
            o,
            "raft_step_downs_total",
            "Times this member stopped being leader.",
            &self.step_downs,
        );
        counter(
            o,
            "raft_term_changes_total",
            "Higher terms observed from other members.",
            &self.term_changes,
        );
        counter(
            o,
            "raft_proposals_total",
            "Log entries proposed while leader.",
            &self.proposals,
        );
        counter(
            o,
            "raft_proposal_timeouts_total",
            "Proposals that did not commit in time.",
            &self.proposal_timeouts,
        );
        counter(
            o,
            "raft_proposals_lost_leadership_total",
            "Proposals abandoned because leadership was lost.",
            &self.proposals_lost_leadership,
        );
        counter(
            o,
            "raft_read_quorum_failures_total",
            "Linearizable reads refused for lack of quorum.",
            &self.read_quorum_failures,
        );
        counter(
            o,
            "raft_log_truncations_total",
            "Conflicting log suffixes discarded.",
            &self.log_truncations,
        );
        counter(
            o,
            "raft_snapshots_taken_total",
            "Snapshots taken to compact the log.",
            &self.snapshots_taken,
        );
        counter(
            o,
            "raft_snapshots_sent_total",
            "Snapshots followers behind the compacted log accepted.",
            &self.snapshots_sent,
        );
        counter(
            o,
            "raft_snapshots_installed_total",
            "Snapshots received from the leader and installed.",
            &self.snapshots_installed,
        );
        counter(
            o,
            "raft_persist_failures_total",
            "Failed writes of Raft state to disk.",
            &self.persist_failures,
        );
        counter(
            o,
            "kv_forwards_total",
            "Client requests forwarded to the leader that got a response.",
            &self.forwards_ok,
        );
        counter(
            o,
            "kv_forward_failures_total",
            "Client requests that could not be forwarded to the leader.",
            &self.forwards_failed,
        );
        counter(
            o,
            "kv_forwarded_requests_received_total",
            "Requests received from other members on a client's behalf.",
            &self.forwarded_received,
        );
        counter(
            o,
            "kv_events_dropped_total",
            "Journal events dropped because the writer fell behind.",
            &self.events_dropped,
        );

        header(
            o,
            "raft_commit_latency_seconds",
            "histogram",
            "Time from proposal to commit on the leader.",
        );
        self.commit_latency
            .render(o, "raft_commit_latency_seconds", "");
        header(
            o,
            "raft_persist_duration_seconds",
            "histogram",
            "Time to write Raft state to disk.",
        );
        self.persist_duration
            .render(o, "raft_persist_duration_seconds", "");

        header(
            o,
            "kv_requests_total",
            "counter",
            "Client requests answered by this member, by operation and status.",
        );
        for op in ClientOp::ALL {
            for (slot, count) in self.requests[op as usize].iter().enumerate() {
                let count = count.get();
                if count == 0 {
                    continue;
                }
                let code = CODES.get(slot).map_or("other".to_string(), u16::to_string);
                let _ = writeln!(
                    o,
                    "kv_requests_total{{op=\"{}\",code=\"{code}\"}} {count}",
                    op.name()
                );
            }
        }
        header(
            o,
            "kv_request_duration_seconds",
            "histogram",
            "Client request latency, including forwarding.",
        );
        for op in ClientOp::ALL {
            let histogram = &self.request_duration[op as usize];
            if histogram.count() > 0 {
                histogram.render(
                    o,
                    "kv_request_duration_seconds",
                    &format!("op=\"{}\"", op.name()),
                );
            }
        }

        if !gauges.peer_match.is_empty() {
            header(
                o,
                "raft_peer_match_index",
                "gauge",
                "Leader only: highest log index known replicated to the peer.",
            );
            for (peer, matched) in &gauges.peer_match {
                let _ = writeln!(o, "raft_peer_match_index{{peer=\"{peer}\"}} {matched}");
            }
            header(
                o,
                "raft_peer_lag_entries",
                "gauge",
                "Leader only: log entries the peer is behind.",
            );
            for (peer, matched) in &gauges.peer_match {
                let lag = gauges.last_index.saturating_sub(*matched);
                let _ = writeln!(o, "raft_peer_lag_entries{{peer=\"{peer}\"}} {lag}");
            }
        }

        let peers = lock(&self.peers);
        let mut ids: Vec<_> = peers.keys().copied().collect();
        ids.sort_unstable();
        header(
            o,
            "raft_peer_up",
            "gauge",
            "1 if the last RPCs to the peer succeeded, 0 if it is considered down.",
        );
        for id in &ids {
            if let Some(up) = peers[id].up {
                let _ = writeln!(o, "raft_peer_up{{peer=\"{id}\"}} {}", u8::from(up));
            }
        }
        header(
            o,
            "raft_peer_dropouts_total",
            "counter",
            "Times the peer was detected as down.",
        );
        for id in &ids {
            let _ = writeln!(
                o,
                "raft_peer_dropouts_total{{peer=\"{id}\"}} {}",
                peers[id].dropouts
            );
        }
        header(
            o,
            "raft_peer_last_contact_seconds",
            "gauge",
            "Seconds since the last successful RPC to the peer.",
        );
        for id in &ids {
            if let Some(at) = peers[id].last_ok {
                let _ = writeln!(
                    o,
                    "raft_peer_last_contact_seconds{{peer=\"{id}\"}} {}",
                    at.elapsed().as_secs_f64()
                );
            }
        }
        header(
            o,
            "raft_peer_rpcs_total",
            "counter",
            "Raft RPCs sent to the peer, by result.",
        );
        for id in &ids {
            let stats = &peers[id];
            for (result, count) in [
                ("ok", stats.rpc_ok),
                ("timeout", stats.rpc_timeout),
                ("error", stats.rpc_error),
            ] {
                let _ = writeln!(
                    o,
                    "raft_peer_rpcs_total{{peer=\"{id}\",result=\"{result}\"}} {count}"
                );
            }
        }
        header(
            o,
            "raft_peer_rpc_duration_seconds",
            "histogram",
            "Latency of successful Raft RPCs to the peer.",
        );
        for id in &ids {
            peers[id].latency.render(
                o,
                "raft_peer_rpc_duration_seconds",
                &format!("peer=\"{id}\""),
            );
        }
        drop(peers);
        out
    }
}

fn spawn_journal(node: u64, path: PathBuf) -> Option<mpsc::SyncSender<JournalMsg>> {
    let open = |path: &PathBuf| -> std::io::Result<(BufWriter<File>, u64)> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let size = file.metadata()?.len();
        Ok((BufWriter::new(file), size))
    };
    let (mut writer, mut size) = match open(&path) {
        Ok(opened) => opened,
        Err(err) => {
            tracing::error!(node, path = %path.display(), %err, "cannot open event journal; events stay in memory");
            return None;
        }
    };
    let (tx, rx) = mpsc::sync_channel::<JournalMsg>(JOURNAL_QUEUE);
    let spawned = std::thread::Builder::new()
        .name(format!("kv-journal-{node}"))
        .spawn(move || {
            let handle = |msg: JournalMsg, writer: &mut BufWriter<File>, size: &mut u64| match msg {
                JournalMsg::Line(line) => {
                    if writer
                        .write_all(line.as_bytes())
                        .and_then(|()| writer.write_all(b"\n"))
                        .is_ok()
                    {
                        *size += line.len() as u64 + 1;
                    }
                }
                JournalMsg::Flush(done) => {
                    let _ = writer.flush();
                    let _ = done.send(());
                }
            };
            while let Ok(msg) = rx.recv() {
                handle(msg, &mut writer, &mut size);
                while let Ok(msg) = rx.try_recv() {
                    handle(msg, &mut writer, &mut size);
                }
                if let Err(err) = writer.flush() {
                    tracing::warn!(node, path = %path.display(), %err, "event journal write failed");
                }
                if size >= JOURNAL_MAX_BYTES {
                    let rotated = path.with_extension("jsonl.1");
                    let _ = std::fs::rename(&path, &rotated);
                    match open(&path) {
                        Ok((next, next_size)) => {
                            writer = next;
                            size = next_size;
                        }
                        Err(err) => {
                            tracing::error!(node, path = %path.display(), %err, "cannot reopen event journal");
                            return;
                        }
                    }
                }
            }
        });
    match spawned {
        Ok(_) => Some(tx),
        Err(err) => {
            tracing::error!(node, %err, "cannot start event journal thread");
            None
        }
    }
}

fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn gauge(out: &mut String, name: &str, help: &str, value: impl std::fmt::Display) {
    header(out, name, "gauge", help);
    let _ = writeln!(out, "{name} {value}");
}

fn counter(out: &mut String, name: &str, help: &str, value: &Counter) {
    header(out, name, "counter", help);
    let _ = writeln!(out, "{name} {}", value.get());
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}

fn unix_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}
