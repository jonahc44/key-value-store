//! A single-Raft-group key-value store.
//!
//! The cluster elects one leader and replicates a command log to a majority
//! before a write is applied or acknowledged. A member that cannot reach a
//! majority refuses the client call, so a partition can serve from at most one
//! side. That is the same consistency/partition choice etcd makes.
//!
//! Membership is stored in the log as `Command::Config` entries and changes
//! one server at a time (Raft thesis §4.1). Every server uses the latest
//! configuration in its log, committed or not, and falls back to the previous
//! one if that entry is truncated. A new server joins as a learner that
//! receives the log without voting, and is promoted once it has caught up.

use crate::net;
use crate::telemetry::{Gauges, Telemetry};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinSet;

const HEARTBEAT: Duration = Duration::from_millis(40);
const TICK: Duration = Duration::from_millis(20);
const PROPOSE_TIMEOUT: Duration = Duration::from_millis(1500);
const CATCHUP_TIMEOUT: Duration = Duration::from_secs(5);
const STATS_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Peer {
    pub id: u64,
    pub addr: String,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub id: u64,
    pub listen: String,
    pub peers: Vec<Peer>,
    pub data_dir: Option<PathBuf>,
    pub join: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Membership {
    voters: Vec<Peer>,
    learners: Vec<Peer>,
}

impl Membership {
    fn find(&self, id: u64) -> Option<&Peer> {
        self.voters
            .iter()
            .chain(&self.learners)
            .find(|peer| peer.id == id)
    }

    fn is_voter(&self, id: u64) -> bool {
        self.voters.iter().any(|peer| peer.id == id)
    }

    fn is_learner(&self, id: u64) -> bool {
        self.learners.iter().any(|peer| peer.id == id)
    }

    fn ids(peers: &[Peer]) -> Vec<u64> {
        peers.iter().map(|peer| peer.id).collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug)]
pub(crate) enum Command {
    Noop,
    Put { key: String, value: Value },
    Delete { key: String },
    Config(Membership),
}

#[derive(Clone, Debug)]
struct Entry {
    term: u64,
    command: Command,
}

pub(crate) enum RaftError {
    NotLeader { leader: Option<String> },
    Unavailable,
    /// Temporarily refused; the caller should retry.
    Busy(String),
    /// Refused because the request conflicts with the current membership.
    Rejected(String),
}

enum RpcOutcome {
    CaughtUp,
    Higher,
    Unreachable,
}

struct Inner {
    id: u64,
    bootstrap: Membership,
    membership: Membership,
    config_index: usize,
    role: Role,
    current_term: u64,
    voted_for: Option<u64>,
    log: Vec<Entry>,
    commit_index: usize,
    last_applied: usize,
    next_index: HashMap<u64, usize>,
    match_index: HashMap<u64, usize>,
    leader_id: Option<u64>,
    data: HashMap<String, Value>,
    waiters: HashMap<usize, oneshot::Sender<bool>>,
    election_timeout: Duration,
    election_deadline: Instant,
    last_leader_contact: Instant,
    election_in_progress: bool,
    last_heartbeat_sent: Instant,
    last_quorum_ok: Instant,
    data_dir: Option<PathBuf>,
    votes: usize,
    telemetry: Arc<Telemetry>,
}

pub struct Node {
    id: u64,
    telemetry: Arc<Telemetry>,
    inner: Mutex<Inner>,
    peer_locks: std::sync::Mutex<HashMap<u64, Arc<Mutex<()>>>>,
    running: AtomicBool,
}

impl Node {
    pub fn start(config: Config) -> Arc<Self> {
        let bootstrap = if config.join.is_some() {
            Membership::default()
        } else {
            let mut voters = config.peers;
            if !voters.iter().any(|peer| peer.id == config.id) {
                voters.push(Peer {
                    id: config.id,
                    addr: config.listen.clone(),
                });
            }
            voters.sort_by_key(|peer| peer.id);
            voters.dedup_by_key(|peer| peer.id);
            Membership {
                voters,
                learners: Vec::new(),
            }
        };

        let telemetry = Arc::new(Telemetry::new(config.id, config.data_dir.as_ref()));
        let mut inner = Inner::new(
            config.id,
            bootstrap,
            config.data_dir,
            Arc::clone(&telemetry),
        );
        inner.load();
        telemetry.event(
            "node_started",
            json!({
                "addr": config.listen,
                "joining": config.join.is_some(),
                "term": inner.current_term,
                "commit_index": inner.commit_index,
                "log_entries": inner.last_index(),
                "voters": Membership::ids(&inner.membership.voters),
                "learners": Membership::ids(&inner.membership.learners),
            }),
        );
        if inner.is_voter() && inner.membership.voters.len() == 1 {
            inner.current_term = inner.current_term.saturating_add(1).max(1);
            inner.voted_for = Some(inner.id);
            inner.become_leader();
        }

        tracing::info!(
            node = inner.id,
            voters = ?Membership::ids(&inner.membership.voters),
            learners = ?Membership::ids(&inner.membership.learners),
            term = inner.current_term,
            role = ?inner.role,
            "raft started"
        );
        let node = Arc::new(Self {
            id: inner.id,
            telemetry,
            inner: Mutex::new(inner),
            peer_locks: std::sync::Mutex::new(HashMap::new()),
            running: AtomicBool::new(true),
        });
        let ticker = Arc::clone(&node);
        tokio::spawn(async move { ticker.tick_loop().await });
        tokio::spawn(Self::stats_loop(Arc::downgrade(&node)));
        node
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn telemetry(&self) -> &Telemetry {
        &self.telemetry
    }

    pub(crate) fn shutdown(&self) {
        tracing::debug!(node = self.id, "stopping raft ticker");
        if self.running.swap(false, Ordering::Relaxed) {
            self.telemetry.event("node_stopped", json!({}));
            self.telemetry.flush();
        }
    }

    /// Prometheus exposition of this member's metrics.
    pub(crate) async fn metrics_text(&self) -> String {
        let gauges = self.inner.lock().await.gauges();
        self.telemetry.render(&gauges)
    }

    async fn stats_loop(node: std::sync::Weak<Self>) {
        let mut interval = tokio::time::interval(STATS_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            interval.tick().await;
            let Some(node) = node.upgrade() else {
                break;
            };
            if !node.running.load(Ordering::Relaxed) {
                break;
            }
            let gauges = node.inner.lock().await.gauges();
            node.telemetry
                .event("stats", node.telemetry.snapshot(&gauges));
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn is_leader(&self) -> bool {
        self.inner.lock().await.role == Role::Leader
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn local_get(&self, key: &str) -> Option<Value> {
        self.inner.lock().await.data.get(key).cloned()
    }

    pub(crate) async fn propose(self: &Arc<Self>, command: Command) -> Result<(), RaftError> {
        let (index, rx) = {
            let mut inner = self.inner.lock().await;
            if inner.role != Role::Leader {
                return Err(RaftError::NotLeader {
                    leader: inner.leader_addr(),
                });
            }
            inner.append(command)
        };
        self.await_commit(index, rx).await
    }

    /// Local view of the membership; not linearizable.
    pub(crate) async fn membership(&self) -> Value {
        let inner = self.inner.lock().await;
        json!({
            "id": inner.id,
            "leader": inner.leader_id,
            "voters": peers_json(&inner.membership.voters),
            "learners": peers_json(&inner.membership.learners),
            "config_index": inner.config_index,
            "committed": inner.config_index <= inner.commit_index,
        })
    }

    /// Add `peer` as a learner, wait for it to catch up, then promote it to a
    /// voter. Safe to retry: each step is skipped if it already happened.
    pub(crate) async fn add_member(self: &Arc<Self>, peer: Peer) -> Result<Value, RaftError> {
        self.change_membership(|current| {
            if let Some(existing) = current.find(peer.id) {
                if existing.addr != peer.addr {
                    return Err(RaftError::Rejected(format!(
                        "member {} already exists at {}",
                        peer.id, existing.addr
                    )));
                }
                return Ok(None);
            }
            if let Some(other) = current
                .voters
                .iter()
                .chain(&current.learners)
                .find(|other| other.addr == peer.addr)
            {
                return Err(RaftError::Rejected(format!(
                    "address {} already belongs to member {}",
                    peer.addr, other.id
                )));
            }
            let mut next = current.clone();
            next.learners.push(peer.clone());
            next.learners.sort_by_key(|peer| peer.id);
            Ok(Some(next))
        })
        .await?;

        self.wait_caught_up(peer.id).await?;

        self.change_membership(|current| {
            if current.is_voter(peer.id) {
                return Ok(None);
            }
            if !current.is_learner(peer.id) {
                return Err(RaftError::Rejected(format!(
                    "member {} left the configuration during catch-up",
                    peer.id
                )));
            }
            let mut next = current.clone();
            next.learners.retain(|learner| learner.id != peer.id);
            next.voters.push(peer.clone());
            next.voters.sort_by_key(|peer| peer.id);
            Ok(Some(next))
        })
        .await?;

        Ok(self.membership().await)
    }

    /// Append a new configuration computed by `change` and wait for it to
    /// commit. `change` returns `None` when the membership already matches.
    async fn change_membership(
        self: &Arc<Self>,
        change: impl FnOnce(&Membership) -> Result<Option<Membership>, RaftError>,
    ) -> Result<(), RaftError> {
        let (index, rx) = {
            let mut inner = self.inner.lock().await;
            if inner.role != Role::Leader {
                return Err(RaftError::NotLeader {
                    leader: inner.leader_addr(),
                });
            }
            // One change at a time keeps every old and new majority
            // overlapping.
            if inner.config_index > inner.commit_index {
                return Err(RaftError::Busy(
                    "another membership change is still committing".into(),
                ));
            }
            // A new leader may hold an uncommitted configuration from an
            // earlier term; committing an entry of its own term first rules
            // that out (Ongaro, raft-dev 2015).
            if inner.log[inner.commit_index].term != inner.current_term {
                return Err(RaftError::Busy(
                    "leader has not committed an entry in its term yet".into(),
                ));
            }
            let Some(next) = change(&inner.membership)? else {
                return Ok(());
            };
            inner.append(Command::Config(next))
        };
        self.await_commit(index, rx).await
    }

    async fn wait_caught_up(self: &Arc<Self>, id: u64) -> Result<(), RaftError> {
        let deadline = Instant::now() + CATCHUP_TIMEOUT;
        loop {
            {
                let inner = self.inner.lock().await;
                if inner.role != Role::Leader {
                    return Err(RaftError::NotLeader {
                        leader: inner.leader_addr(),
                    });
                }
                let matched = inner.match_index.get(&id).copied().unwrap_or(0);
                if matched >= inner.commit_index {
                    tracing::info!(node = inner.id, learner = id, matched, "learner caught up");
                    return Ok(());
                }
                if Instant::now() >= deadline {
                    tracing::warn!(
                        node = inner.id,
                        learner = id,
                        matched,
                        commit = inner.commit_index,
                        "learner did not catch up"
                    );
                    return Err(RaftError::Busy(format!(
                        "member {id} has not caught up; it remains a learner, retry to promote it"
                    )));
                }
            }
            tokio::time::sleep(TICK).await;
        }
    }

    async fn await_commit(
        self: &Arc<Self>,
        index: usize,
        rx: oneshot::Receiver<bool>,
    ) -> Result<(), RaftError> {
        let started = Instant::now();
        self.telemetry.proposals.inc();
        let _ = self.replicate_all().await;
        match tokio::time::timeout(PROPOSE_TIMEOUT, rx).await {
            Ok(Ok(true)) => {
                self.telemetry.commit_latency.observe(started.elapsed());
                Ok(())
            }
            _ => {
                let mut inner = self.inner.lock().await;
                inner.waiters.remove(&index);
                if inner.role != Role::Leader {
                    self.telemetry.proposals_lost_leadership.inc();
                    tracing::warn!(node = inner.id, index, "proposal lost leadership");
                    Err(RaftError::NotLeader {
                        leader: inner.leader_addr(),
                    })
                } else {
                    self.telemetry.proposal_timeouts.inc();
                    tracing::warn!(
                        node = inner.id,
                        index,
                        term = inner.current_term,
                        "proposal timed out before commit"
                    );
                    Err(RaftError::Unavailable)
                }
            }
        }
    }

    /// Confirm this node is still the leader with a majority, then read.
    pub(crate) async fn read(self: &Arc<Self>, key: String) -> Result<Option<Value>, RaftError> {
        let term = {
            let inner = self.inner.lock().await;
            if inner.role != Role::Leader {
                return Err(RaftError::NotLeader {
                    leader: inner.leader_addr(),
                });
            }
            inner.current_term
        };
        let quorum_ok = self.replicate_all().await;
        let inner = self.inner.lock().await;
        if inner.role != Role::Leader || inner.current_term != term {
            return Err(RaftError::NotLeader {
                leader: inner.leader_addr(),
            });
        }
        if !quorum_ok {
            self.telemetry.read_quorum_failures.inc();
            tracing::warn!(node = inner.id, term, "linearizable read failed; no quorum");
            return Err(RaftError::Unavailable);
        }
        Ok(inner.data.get(&key).cloned())
    }

    pub(crate) async fn handle_request_vote(&self, req: &Value) -> Value {
        let mut inner = self.inner.lock().await;
        let term = u64_field(req, "term").unwrap_or(0);
        let candidate = u64_field(req, "candidate_id").unwrap_or(0);
        let last_index = u64_field(req, "last_log_index").unwrap_or(0);
        let last_term = u64_field(req, "last_log_term").unwrap_or(0);
        let prevote = req
            .get("prevote")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if prevote {
            if term < inner.current_term {
                return json!({ "term": inner.current_term, "vote_granted": false });
            }
            if inner.role == Role::Leader {
                return json!({ "term": inner.current_term, "vote_granted": false });
            }
            let lease_held = inner.leader_id.is_some()
                && inner.last_leader_contact.elapsed() < inner.election_timeout;
            if lease_held {
                return json!({ "term": inner.current_term, "vote_granted": false });
            }
            let granted = log_up_to_date(
                inner.last_term(),
                inner.last_index() as u64,
                last_term,
                last_index,
            );
            return json!({ "term": inner.current_term, "vote_granted": granted });
        }

        if term < inner.current_term {
            return json!({ "term": inner.current_term, "vote_granted": false });
        }
        if term > inner.current_term {
            inner.observe_term(term);
        }
        let can_vote = inner.voted_for.is_none() || inner.voted_for == Some(candidate);
        let granted = can_vote
            && log_up_to_date(
                inner.last_term(),
                inner.last_index() as u64,
                last_term,
                last_index,
            );
        if granted {
            inner.voted_for = Some(candidate);
            inner.refresh_follower_timer();
            inner.persist();
            tracing::info!(
                node = inner.id,
                candidate,
                term = inner.current_term,
                "granted vote"
            );
        } else {
            tracing::debug!(
                node = inner.id,
                candidate,
                term = inner.current_term,
                "denied vote"
            );
        }
        json!({ "term": inner.current_term, "vote_granted": granted })
    }

    pub(crate) async fn handle_append_entries(&self, req: &Value) -> Value {
        let mut inner = self.inner.lock().await;
        let term = u64_field(req, "term").unwrap_or(0);
        let leader_id = u64_field(req, "leader_id").unwrap_or(0);
        if term < inner.current_term {
            tracing::debug!(
                node = inner.id,
                leader = leader_id,
                term,
                current = inner.current_term,
                "rejected append from older term"
            );
            return json!({
                "term": inner.current_term,
                "success": false,
                "next_index": 1
            });
        }
        if term > inner.current_term {
            inner.observe_term(term);
        } else if inner.role != Role::Follower {
            inner.role = Role::Follower;
            inner.fail_uncommitted();
        }
        if inner.leader_id != Some(leader_id) {
            inner.note_leader(leader_id);
        }
        inner.refresh_follower_timer();

        let prev = u64_field(req, "prev_log_index").unwrap_or(0);
        let prev_term = u64_field(req, "prev_log_term").unwrap_or(0);
        if prev > inner.last_index() as u64 {
            let next = inner.last_index() + 1;
            tracing::debug!(
                node = inner.id,
                leader = leader_id,
                prev,
                last = inner.last_index(),
                "rejected append; log is shorter than leader expects"
            );
            return json!({
                "term": inner.current_term,
                "success": false,
                "next_index": next
            });
        }
        let prev_index = prev as usize;
        if inner.log[prev_index].term != prev_term {
            tracing::info!(
                node = inner.id,
                leader = leader_id,
                prev,
                prev_term,
                local_term = inner.log[prev_index].term,
                "rejected append; log conflict"
            );
            return json!({
                "term": inner.current_term,
                "success": false,
                "next_index": prev_index.max(1)
            });
        }

        let mut dirty = false;
        let mut membership_changed = false;
        if let Some(entries) = req.get("entries").and_then(|v| v.as_array()) {
            for (offset, raw) in entries.iter().enumerate() {
                let index = prev_index + 1 + offset;
                let Some(entry) = parse_entry(raw) else {
                    return json!({
                        "term": inner.current_term,
                        "success": false,
                        "next_index": index.max(1)
                    });
                };
                if index < inner.log.len() {
                    if inner.log[index].term != entry.term {
                        let dropped = inner.log.len().saturating_sub(index);
                        tracing::info!(
                            node = inner.id,
                            index,
                            kept = dropped,
                            "truncated conflicting log suffix"
                        );
                        inner.telemetry.log_truncations.inc();
                        inner.telemetry.event(
                            "log_truncated",
                            json!({ "index": index, "dropped": dropped, "leader": leader_id }),
                        );
                        inner.log.truncate(index);
                        inner.log.push(entry);
                        dirty = true;
                        membership_changed = true;
                    }
                } else if index == inner.log.len() {
                    membership_changed |= matches!(entry.command, Command::Config(_));
                    inner.log.push(entry);
                    dirty = true;
                } else {
                    return json!({
                        "term": inner.current_term,
                        "success": false,
                        "next_index": inner.last_index() + 1
                    });
                }
            }
        }
        if membership_changed {
            inner.refresh_membership();
        }

        let leader_commit = u64_field(req, "leader_commit").unwrap_or(0) as usize;
        if leader_commit > inner.commit_index {
            inner.commit_index = leader_commit.min(inner.last_index());
            inner.apply();
            dirty = true;
        }
        if dirty {
            inner.persist();
        }
        json!({
            "term": inner.current_term,
            "success": true,
            "next_index": inner.last_index() + 1
        })
    }

    async fn tick_loop(self: Arc<Self>) {
        let mut interval = tokio::time::interval(TICK);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        while self.running.load(Ordering::Relaxed) {
            interval.tick().await;
            if !self.running.load(Ordering::Relaxed) {
                break;
            }
            self.tick().await;
        }
    }

    async fn tick(self: &Arc<Self>) {
        enum Action {
            Elect,
            Heartbeat,
            None,
        }
        let action = {
            let mut inner = self.inner.lock().await;
            if inner.role == Role::Leader {
                if inner.last_quorum_ok.elapsed() >= inner.election_timeout {
                    inner.step_down();
                    Action::None
                } else if inner.last_heartbeat_sent.elapsed() >= HEARTBEAT {
                    inner.last_heartbeat_sent = Instant::now();
                    Action::Heartbeat
                } else {
                    Action::None
                }
            } else if inner.is_voter()
                && !inner.election_in_progress
                && Instant::now() >= inner.election_deadline
            {
                Action::Elect
            } else {
                Action::None
            }
        };
        match action {
            Action::Elect => self.start_election().await,
            Action::Heartbeat => {
                let _ = self.replicate_all().await;
            }
            Action::None => {}
        }
    }

    async fn start_election(self: &Arc<Self>) {
        {
            let mut inner = self.inner.lock().await;
            if inner.role == Role::Leader
                || !inner.is_voter()
                || inner.election_in_progress
                || Instant::now() < inner.election_deadline
            {
                return;
            }
            inner.election_in_progress = true;
        }
        // Spread nodes that time out together so one can win the prevote
        // before the other starts a competing term.
        tokio::time::sleep(Duration::from_millis(50 + random_below(200))).await;
        let campaign = {
            let mut inner = self.inner.lock().await;
            inner.election_in_progress = false;
            if !self.running.load(Ordering::Relaxed)
                || inner.role == Role::Leader
                || !inner.is_voter()
                || Instant::now() < inner.election_deadline
            {
                return;
            }
            inner.election_timeout = random_timeout(inner.id);
            // A failed attempt must not look like a fresh leader heartbeat.
            inner.election_deadline = Instant::now() + inner.election_timeout;
            tracing::info!(
                node = inner.id,
                term = inner.current_term + 1,
                "starting election"
            );
            inner.telemetry.elections_started.inc();
            inner.telemetry.event(
                "election_started",
                json!({
                    "term": inner.current_term + 1,
                    "last_leader": inner.leader_id,
                    "since_leader_contact_ms": inner.last_leader_contact.elapsed().as_millis(),
                }),
            );
            if inner.quorum() <= 1 {
                inner.current_term = inner.current_term.saturating_add(1).max(1);
                inner.voted_for = Some(inner.id);
                inner.become_leader();
                return;
            }
            Campaign {
                current_term: inner.current_term,
                candidate_id: inner.id,
                last_log_index: inner.last_index() as u64,
                last_log_term: inner.last_term(),
                peers: inner.other_voters(),
                quorum: inner.quorum(),
            }
        };

        let mut grants = 1usize;
        let mut higher_term = None;
        let mut pending = JoinSet::new();
        let peer_count = campaign.peers.len();
        for peer in campaign.peers {
            let req = json!({
                "term": campaign.current_term + 1,
                "candidate_id": campaign.candidate_id,
                "last_log_index": campaign.last_log_index,
                "last_log_term": campaign.last_log_term,
                "prevote": true,
            });
            let this = Arc::clone(self);
            pending.spawn(async move { this.peer_rpc(&peer, "/raft/request-vote", &req).await });
        }
        let mut answered = 0usize;
        while let Some(joined) = pending.join_next().await {
            answered += 1;
            if let Ok(Ok(response)) = joined {
                let term = u64_field(&response, "term").unwrap_or(0);
                if term > campaign.current_term {
                    higher_term = Some(term);
                    break;
                }
                if response
                    .get("vote_granted")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    grants += 1;
                }
            }
            let outstanding = peer_count - answered;
            if grants >= campaign.quorum || grants + outstanding < campaign.quorum {
                break;
            }
        }
        drop(pending);
        if let Some(term) = higher_term {
            self.inner.lock().await.observe_term(term);
            return;
        }
        if grants < campaign.quorum {
            self.telemetry.prevotes_failed.inc();
            tracing::debug!(
                node = campaign.candidate_id,
                grants,
                quorum = campaign.quorum,
                "prevote failed"
            );
            return;
        }
        tracing::debug!(
            node = campaign.candidate_id,
            grants,
            term = campaign.current_term + 1,
            "prevote succeeded"
        );

        let vote_req = {
            let mut inner = self.inner.lock().await;
            if !self.running.load(Ordering::Relaxed) || inner.role == Role::Leader {
                return;
            }
            if inner.current_term != campaign.current_term {
                return;
            }
            inner.current_term = inner.current_term.saturating_add(1);
            inner.role = Role::Candidate;
            inner.voted_for = Some(inner.id);
            inner.votes = 1;
            inner.leader_id = None;
            inner.persist();
            json!({
                "term": inner.current_term,
                "candidate_id": inner.id,
                "last_log_index": inner.last_index(),
                "last_log_term": inner.last_term(),
                "prevote": false,
            })
        };
        let term = u64_field(&vote_req, "term").unwrap_or(0);
        let peers = self.inner.lock().await.other_voters();
        for peer in peers {
            let this = Arc::clone(self);
            let req = vote_req.clone();
            tokio::spawn(async move {
                if let Ok(response) = this.peer_rpc(&peer, "/raft/request-vote", &req).await {
                    this.on_vote(term, response).await;
                }
            });
        }
    }

    async fn on_vote(self: &Arc<Self>, election_term: u64, response: Value) {
        if !self.running.load(Ordering::Relaxed) {
            return;
        }
        let resp_term = u64_field(&response, "term").unwrap_or(0);
        let granted = response
            .get("vote_granted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let won = {
            let mut inner = self.inner.lock().await;
            if resp_term > inner.current_term {
                inner.observe_term(resp_term);
                return;
            }
            if inner.role != Role::Candidate || inner.current_term != election_term {
                return;
            }
            if !granted {
                return;
            }
            inner.votes += 1;
            if inner.votes >= inner.quorum() {
                inner.become_leader();
                true
            } else {
                false
            }
        };
        if won {
            let _ = self.replicate_all().await;
        }
    }

    async fn replicate_all(self: &Arc<Self>) -> bool {
        if !self.running.load(Ordering::Relaxed) {
            return false;
        }
        let (voters, learners, quorum, term, self_vote) = {
            let inner = self.inner.lock().await;
            if inner.role != Role::Leader {
                return false;
            }
            (
                inner.other_voters(),
                inner.other_learners(),
                inner.quorum(),
                inner.current_term,
                usize::from(inner.is_voter()),
            )
        };

        // Learners never count toward quorum, so their replication must not be
        // cancelled when the voter majority answers first.
        for peer in learners {
            let this = Arc::clone(self);
            tokio::spawn(async move { this.replicate_one(peer, false).await });
        }

        let mut pending = JoinSet::new();
        for peer in voters {
            let this = Arc::clone(self);
            pending.spawn(async move { this.replicate_one(peer, true).await });
        }
        let mut ok = self_vote;
        while ok < quorum {
            let Some(joined) = pending.join_next().await else {
                break;
            };
            if matches!(joined, Ok(RpcOutcome::CaughtUp)) {
                ok += 1;
            }
        }
        drop(pending);
        let mut inner = self.inner.lock().await;
        if inner.role == Role::Leader && inner.current_term == term && ok >= quorum {
            inner.last_quorum_ok = Instant::now();
            inner.advance_commit();
            true
        } else {
            false
        }
    }

    async fn peer_rpc(&self, peer: &Peer, path: &str, request: &Value) -> Result<Value, String> {
        let started = Instant::now();
        let result = net::post_json(&peer.addr, path, request).await;
        self.telemetry
            .record_peer_rpc(peer.id, &peer.addr, started.elapsed(), &result);
        result
    }

    fn peer_lock(&self, id: u64) -> Arc<Mutex<()>> {
        let mut locks = self.peer_locks.lock().unwrap_or_else(|err| err.into_inner());
        Arc::clone(locks.entry(id).or_default())
    }

    /// `wait` queues behind an in-flight round to the same peer; otherwise a
    /// busy peer is skipped so detached learner rounds cannot pile up.
    async fn replicate_one(self: &Arc<Self>, peer: Peer, wait: bool) -> RpcOutcome {
        let mutex = self.peer_lock(peer.id);
        let _guard = if wait {
            mutex.lock().await
        } else {
            match mutex.try_lock() {
                Ok(guard) => guard,
                Err(_) => return RpcOutcome::Unreachable,
            }
        };
        if !self.running.load(Ordering::Relaxed) {
            return RpcOutcome::Unreachable;
        }
        for _ in 0..128 {
            let (term, request, last) = {
                let inner = self.inner.lock().await;
                if inner.role != Role::Leader {
                    return RpcOutcome::Unreachable;
                }
                let term = inner.current_term;
                let next = (*inner.next_index.get(&peer.id).unwrap_or(&1)).max(1);
                let prev = next - 1;
                let prev_term = inner.log.get(prev).map(|entry| entry.term).unwrap_or(0);
                let last = inner.last_index();
                let end = (next + 32).min(last + 1);
                let entries = if next <= last {
                    inner.log[next..end]
                        .iter()
                        .map(entry_json)
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                let request = json!({
                    "term": term,
                    "leader_id": inner.id,
                    "prev_log_index": prev,
                    "prev_log_term": prev_term,
                    "entries": entries,
                    "leader_commit": inner.commit_index,
                });
                (term, request, last)
            };

            let response = match self.peer_rpc(&peer, "/raft/append-entries", &request).await {
                Ok(response) => response,
                Err(_) => return RpcOutcome::Unreachable,
            };
            let resp_term = u64_field(&response, "term").unwrap_or(0);
            let success = response
                .get("success")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let mut inner = self.inner.lock().await;
            if resp_term > inner.current_term {
                inner.observe_term(resp_term);
                return RpcOutcome::Higher;
            }
            if inner.role != Role::Leader || inner.current_term != term {
                return RpcOutcome::Unreachable;
            }
            if success {
                let count = request
                    .get("entries")
                    .and_then(|v| v.as_array())
                    .map(|entries| entries.len())
                    .unwrap_or(0);
                let prev = u64_field(&request, "prev_log_index").unwrap_or(0) as usize;
                let matched = prev + count;
                let slot = inner.match_index.entry(peer.id).or_insert(0);
                if matched > *slot {
                    *slot = matched;
                }
                inner.next_index.insert(peer.id, matched + 1);
                inner.advance_commit();
                if matched >= last.max(inner.last_index())
                    || inner.next_index[&peer.id] > inner.last_index()
                {
                    return RpcOutcome::CaughtUp;
                }
            } else {
                let cur = (*inner.next_index.get(&peer.id).unwrap_or(&1)).max(1);
                let hinted = u64_field(&response, "next_index").unwrap_or(1);
                let hinted = hinted.min(inner.log.len() as u64) as usize;
                let mut next = if (1..cur).contains(&hinted) {
                    hinted
                } else {
                    cur.saturating_sub(1)
                };
                if next < 1 {
                    next = 1;
                }
                if next == cur {
                    return RpcOutcome::Unreachable;
                }
                inner.next_index.insert(peer.id, next);
            }
        }
        RpcOutcome::Unreachable
    }
}

struct Campaign {
    current_term: u64,
    candidate_id: u64,
    last_log_index: u64,
    last_log_term: u64,
    peers: Vec<Peer>,
    quorum: usize,
}

impl Inner {
    fn new(
        id: u64,
        bootstrap: Membership,
        data_dir: Option<PathBuf>,
        telemetry: Arc<Telemetry>,
    ) -> Self {
        let now = Instant::now();
        let election_timeout = random_timeout(id);
        Self {
            id,
            membership: bootstrap.clone(),
            bootstrap,
            config_index: 0,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: vec![Entry {
                term: 0,
                command: Command::Noop,
            }],
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            leader_id: None,
            data: HashMap::new(),
            waiters: HashMap::new(),
            election_timeout,
            election_deadline: now + election_timeout,
            last_leader_contact: now,
            election_in_progress: false,
            last_heartbeat_sent: now,
            last_quorum_ok: now,
            data_dir,
            votes: 0,
            telemetry,
        }
    }

    fn gauges(&self) -> Gauges {
        let mut peer_match: Vec<(u64, usize)> = if self.role == Role::Leader {
            self.match_index
                .iter()
                .map(|(id, matched)| (*id, *matched))
                .collect()
        } else {
            Vec::new()
        };
        peer_match.sort_unstable();
        Gauges {
            role: match self.role {
                Role::Follower => "follower",
                Role::Candidate => "candidate",
                Role::Leader => "leader",
            },
            term: self.current_term,
            leader: self.leader_id,
            commit_index: self.commit_index,
            applied_index: self.last_applied,
            last_index: self.last_index(),
            voters: self.membership.voters.len(),
            learners: self.membership.learners.len(),
            keys: self.data.len(),
            peer_match,
        }
    }

    fn note_leader(&mut self, leader: u64) {
        self.leader_id = Some(leader);
        self.telemetry.leader_changes.inc();
        self.telemetry.event(
            "leader_changed",
            json!({ "leader": leader, "term": self.current_term }),
        );
    }

    fn refresh_follower_timer(&mut self) {
        let now = Instant::now();
        self.last_leader_contact = now;
        self.election_deadline = now + self.election_timeout;
    }

    fn quorum(&self) -> usize {
        self.membership.voters.len() / 2 + 1
    }

    fn is_voter(&self) -> bool {
        self.membership.is_voter(self.id)
    }

    /// Append a log entry on the leader and register a commit waiter.
    fn append(&mut self, command: Command) -> (usize, oneshot::Receiver<bool>) {
        let term = self.current_term;
        let (op, key) = command_fields(&command);
        let key = key.to_string();
        let is_config = matches!(command, Command::Config(_));
        self.log.push(Entry { term, command });
        let index = self.last_index();
        tracing::debug!(node = self.id, term, index, op, key, "proposed");
        if is_config {
            self.refresh_membership();
        }
        let (tx, rx) = oneshot::channel();
        self.waiters.insert(index, tx);
        self.persist();
        self.advance_commit();
        (index, rx)
    }

    /// Adopt the latest configuration in the log, or the bootstrap peers if
    /// the log has none.
    fn refresh_membership(&mut self) {
        let (index, membership) = self
            .log
            .iter()
            .enumerate()
            .rev()
            .find_map(|(index, entry)| match &entry.command {
                Command::Config(membership) => Some((index, membership.clone())),
                _ => None,
            })
            .unwrap_or_else(|| (0, self.bootstrap.clone()));
        if membership != self.membership {
            tracing::info!(
                node = self.id,
                index,
                voters = ?Membership::ids(&membership.voters),
                learners = ?Membership::ids(&membership.learners),
                "membership changed"
            );
            self.telemetry.event(
                "membership_changed",
                json!({
                    "index": index,
                    "voters": Membership::ids(&membership.voters),
                    "learners": Membership::ids(&membership.learners),
                }),
            );
        }
        self.membership = membership;
        self.config_index = index;
        if self.role == Role::Leader {
            let next = self.last_index() + 1;
            let targets: Vec<u64> = self
                .other_voters()
                .into_iter()
                .chain(self.other_learners())
                .map(|peer| peer.id)
                .collect();
            for id in &targets {
                self.next_index.entry(*id).or_insert(next);
                self.match_index.entry(*id).or_insert(0);
            }
            self.next_index.retain(|id, _| targets.contains(id));
            self.match_index.retain(|id, _| targets.contains(id));
        }
    }

    fn last_index(&self) -> usize {
        self.log.len() - 1
    }

    fn last_term(&self) -> u64 {
        self.log[self.last_index()].term
    }

    fn other_voters(&self) -> Vec<Peer> {
        self.membership
            .voters
            .iter()
            .filter(|peer| peer.id != self.id)
            .cloned()
            .collect()
    }

    fn other_learners(&self) -> Vec<Peer> {
        self.membership
            .learners
            .iter()
            .filter(|peer| peer.id != self.id)
            .cloned()
            .collect()
    }

    fn leader_addr(&self) -> Option<String> {
        let id = self.leader_id?;
        self.membership.find(id).map(|peer| peer.addr.clone())
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.telemetry.elections_won.inc();
        self.note_leader(self.id);
        let next = self.last_index() + 1;
        self.next_index.clear();
        self.match_index.clear();
        for peer in self.other_voters().into_iter().chain(self.other_learners()) {
            self.next_index.insert(peer.id, next);
            self.match_index.insert(peer.id, 0);
        }
        self.log.push(Entry {
            term: self.current_term,
            command: Command::Noop,
        });
        let now = Instant::now();
        self.last_heartbeat_sent = now;
        self.last_quorum_ok = now;
        self.advance_commit();
        self.persist();
        tracing::info!(node = self.id, term = self.current_term, "became leader");
    }

    fn step_down(&mut self) {
        if self.role == Role::Follower {
            return;
        }
        if self.role == Role::Leader {
            self.telemetry.step_downs.inc();
            self.telemetry.event(
                "stepped_down",
                json!({
                    "term": self.current_term,
                    "reason": "lost_quorum",
                    "quorum_silence_ms": self.last_quorum_ok.elapsed().as_millis(),
                }),
            );
        }
        self.role = Role::Follower;
        self.leader_id = None;
        self.votes = 0;
        self.election_timeout = random_timeout(self.id);
        self.election_deadline = Instant::now() + self.election_timeout;
        tracing::warn!(
            node = self.id,
            term = self.current_term,
            "stepped down; lost quorum"
        );
        self.fail_uncommitted();
    }

    fn observe_term(&mut self, term: u64) {
        if term <= self.current_term {
            return;
        }
        tracing::info!(
            node = self.id,
            from = self.current_term,
            to = term,
            "observed higher term"
        );
        self.telemetry.term_changes.inc();
        if self.role == Role::Leader {
            self.telemetry.step_downs.inc();
            self.telemetry.event(
                "stepped_down",
                json!({ "term": self.current_term, "reason": "higher_term", "new_term": term }),
            );
        }
        self.current_term = term;
        self.voted_for = None;
        self.role = Role::Follower;
        self.leader_id = None;
        self.votes = 0;
        self.election_in_progress = false;
        self.election_timeout = random_timeout(self.id);
        self.election_deadline = Instant::now() + self.election_timeout;
        self.fail_uncommitted();
        self.persist();
    }

    fn fail_uncommitted(&mut self) {
        let commit = self.commit_index;
        let pending: Vec<usize> = self
            .waiters
            .keys()
            .copied()
            .filter(|index| *index > commit)
            .collect();
        if !pending.is_empty() {
            tracing::debug!(
                node = self.id,
                count = pending.len(),
                "failing uncommitted proposals"
            );
        }
        for index in pending {
            if let Some(waiter) = self.waiters.remove(&index) {
                let _ = waiter.send(false);
            }
        }
    }

    fn advance_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let last = self.last_index();
        let mut commit = self.commit_index;
        for index in (self.commit_index + 1)..=last {
            if self.log[index].term != self.current_term {
                continue;
            }
            let mut replicas = usize::from(self.is_voter());
            for voter in &self.membership.voters {
                if voter.id != self.id
                    && self.match_index.get(&voter.id).is_some_and(|m| *m >= index)
                {
                    replicas += 1;
                }
            }
            if replicas >= self.quorum() {
                commit = index;
            }
        }
        if commit > self.commit_index {
            tracing::debug!(
                node = self.id,
                from = self.commit_index,
                to = commit,
                "advanced commit index"
            );
            self.commit_index = commit;
            self.apply();
            self.persist();
        }
    }

    fn apply(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let index = self.last_applied;
            match &self.log[index].command {
                Command::Put { key, value } => {
                    tracing::info!(node = self.id, index, key, "applied put");
                    self.data.insert(key.clone(), value.clone());
                }
                Command::Delete { key } => {
                    tracing::info!(node = self.id, index, key, "applied delete");
                    self.data.remove(key);
                }
                Command::Noop => {
                    tracing::debug!(node = self.id, index, "applied noop");
                }
                Command::Config(_) => {
                    tracing::debug!(node = self.id, index, "committed membership");
                }
            }
            if let Some(waiter) = self.waiters.remove(&self.last_applied) {
                let _ = waiter.send(true);
            }
        }
    }

    fn persist(&self) {
        if self.data_dir.is_none() {
            return;
        }
        let started = Instant::now();
        let result = self.write_state();
        self.telemetry
            .record_persist(started.elapsed(), result.is_ok(), result.err().as_deref());
    }

    fn write_state(&self) -> Result<(), String> {
        let Some(dir) = &self.data_dir else {
            return Ok(());
        };
        if let Err(err) = std::fs::create_dir_all(dir) {
            tracing::error!(node = self.id, path = %dir.display(), %err, "failed to create data dir");
            return Err(err.to_string());
        }
        let path = dir.join(format!("{}.json", self.id));
        let tmp = dir.join(format!("{}.json.tmp", self.id));
        let log: Vec<Value> = self.log.iter().map(entry_json).collect();
        let body = json!({
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "commit_index": self.commit_index,
            "log": log,
        });
        let bytes = serde_json::to_vec(&body).unwrap_or_default();
        if let Err(err) = std::fs::write(&tmp, &bytes) {
            tracing::error!(node = self.id, path = %tmp.display(), %err, "failed to write raft state");
            return Err(err.to_string());
        }
        if let Err(err) = std::fs::rename(&tmp, &path) {
            tracing::error!(node = self.id, path = %path.display(), %err, "failed to replace raft state");
            return Err(err.to_string());
        }
        Ok(())
    }

    fn load(&mut self) {
        let Some(dir) = &self.data_dir else {
            return;
        };
        let path = dir.join(format!("{}.json", self.id));
        let Ok(bytes) = std::fs::read(&path) else {
            tracing::debug!(node = self.id, path = %path.display(), "no raft state to load");
            return;
        };
        let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
            tracing::warn!(node = self.id, path = %path.display(), "ignoring unreadable raft state");
            return;
        };
        let Some(entries) = value.get("log").and_then(|v| v.as_array()) else {
            return;
        };
        let mut log = Vec::new();
        for raw in entries {
            if let Some(entry) = parse_entry(raw) {
                log.push(entry);
            }
        }
        if log.is_empty() {
            return;
        }
        self.current_term = u64_field(&value, "current_term").unwrap_or(0);
        self.voted_for = value.get("voted_for").and_then(|v| v.as_u64());
        self.log = log;
        self.refresh_membership();
        let commit = u64_field(&value, "commit_index").unwrap_or(0) as usize;
        self.commit_index = commit.min(self.last_index());
        self.last_applied = 0;
        self.data.clear();
        self.apply();
        tracing::info!(
            node = self.id,
            term = self.current_term,
            commit = self.commit_index,
            entries = self.last_index(),
            "loaded raft state"
        );
    }
}

fn command_fields(command: &Command) -> (&'static str, &str) {
    match command {
        Command::Noop => ("noop", ""),
        Command::Put { key, .. } => ("put", key),
        Command::Delete { key } => ("del", key),
        Command::Config(_) => ("config", ""),
    }
}

fn log_up_to_date(my_term: u64, my_index: u64, candidate_term: u64, candidate_index: u64) -> bool {
    if candidate_term != my_term {
        candidate_term > my_term
    } else {
        candidate_index >= my_index
    }
}

fn random_below(span: u64) -> u64 {
    if span == 0 {
        return 0;
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.subsec_nanos() as u64)
        .unwrap_or(1);
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let mix = nanos.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ COUNTER
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mix % span
}

fn random_timeout(id: u64) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.subsec_nanos() as u64)
        .unwrap_or(1);
    let mixed = nanos.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ id.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    Duration::from_millis(500 + (mixed % 400))
}

fn u64_field(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(|v| v.as_u64())
}

fn entry_json(entry: &Entry) -> Value {
    match &entry.command {
        Command::Noop => json!({ "term": entry.term, "op": "noop" }),
        Command::Put { key, value } => json!({
            "term": entry.term,
            "op": "put",
            "key": key,
            "value": value
        }),
        Command::Delete { key } => json!({
            "term": entry.term,
            "op": "del",
            "key": key
        }),
        Command::Config(membership) => json!({
            "term": entry.term,
            "op": "config",
            "voters": peers_json(&membership.voters),
            "learners": peers_json(&membership.learners)
        }),
    }
}

fn peers_json(peers: &[Peer]) -> Value {
    peers
        .iter()
        .map(|peer| json!({ "id": peer.id, "addr": peer.addr }))
        .collect()
}

fn parse_peers(value: &Value) -> Option<Vec<Peer>> {
    value
        .as_array()?
        .iter()
        .map(|raw| {
            Some(Peer {
                id: u64_field(raw, "id")?,
                addr: raw.get("addr")?.as_str()?.to_string(),
            })
        })
        .collect()
}

fn parse_entry(value: &Value) -> Option<Entry> {
    let term = u64_field(value, "term")?;
    let op = value.get("op").and_then(|v| v.as_str()).unwrap_or("noop");
    let command = match op {
        "noop" => Command::Noop,
        "put" => Command::Put {
            key: value.get("key")?.as_str()?.to_string(),
            value: value.get("value")?.clone(),
        },
        "del" => Command::Delete {
            key: value.get("key")?.as_str()?.to_string(),
        },
        "config" => Command::Config(Membership {
            voters: parse_peers(value.get("voters")?)?,
            learners: parse_peers(value.get("learners")?)?,
        }),
        _ => return None,
    };
    Some(Entry { term, command })
}
