//! A single-Raft-group key-value store.
//!
//! The cluster elects one leader and replicates a command log to a majority
//! before a write is applied or acknowledged. A member that cannot reach a
//! majority refuses the client call, so a partition can serve from at most one
//! side. That is the same consistency/partition choice etcd makes.

use crate::net;
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

#[derive(Clone, Debug)]
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
}

#[derive(Clone, Debug)]
struct Entry {
    term: u64,
    command: Command,
}

pub(crate) enum RaftError {
    NotLeader { leader: Option<String> },
    Unavailable,
}

enum RpcOutcome {
    CaughtUp,
    Higher,
    Unreachable,
}

struct Inner {
    id: u64,
    peers: Vec<Peer>,
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
}

pub struct Node {
    inner: Mutex<Inner>,
    peer_locks: HashMap<u64, Mutex<()>>,
    running: AtomicBool,
}

impl Node {
    pub fn start(mut config: Config) -> Arc<Self> {
        if config.peers.is_empty() {
            config.peers.push(Peer {
                id: config.id,
                addr: config.listen.clone(),
            });
        } else if !config.peers.iter().any(|peer| peer.id == config.id) {
            config.peers.push(Peer {
                id: config.id,
                addr: config.listen.clone(),
            });
        }
        config.peers.sort_by_key(|peer| peer.id);
        config.peers.dedup_by_key(|peer| peer.id);

        let mut peer_locks = HashMap::new();
        for peer in &config.peers {
            if peer.id != config.id {
                peer_locks.insert(peer.id, Mutex::new(()));
            }
        }

        let mut inner = Inner::new(config.id, config.peers, config.data_dir);
        inner.load();
        if inner.peers.len() == 1 {
            inner.current_term = inner.current_term.saturating_add(1).max(1);
            inner.voted_for = Some(inner.id);
            inner.become_leader();
        }

        let node = Arc::new(Self {
            inner: Mutex::new(inner),
            peer_locks,
            running: AtomicBool::new(true),
        });
        let ticker = Arc::clone(&node);
        tokio::spawn(async move { ticker.tick_loop().await });
        node
    }

    pub(crate) fn shutdown(&self) {
        self.running.store(false, Ordering::Relaxed);
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
            let term = inner.current_term;
            inner.log.push(Entry { term, command });
            let index = inner.last_index();
            let (tx, rx) = oneshot::channel();
            inner.waiters.insert(index, tx);
            inner.persist();
            inner.advance_commit();
            (index, rx)
        };
        let _ = self.replicate_all().await;
        match tokio::time::timeout(PROPOSE_TIMEOUT, rx).await {
            Ok(Ok(true)) => Ok(()),
            _ => {
                let mut inner = self.inner.lock().await;
                inner.waiters.remove(&index);
                if inner.role != Role::Leader {
                    Err(RaftError::NotLeader {
                        leader: inner.leader_addr(),
                    })
                } else {
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
        }
        json!({ "term": inner.current_term, "vote_granted": granted })
    }

    pub(crate) async fn handle_append_entries(&self, req: &Value) -> Value {
        let mut inner = self.inner.lock().await;
        let term = u64_field(req, "term").unwrap_or(0);
        let leader_id = u64_field(req, "leader_id").unwrap_or(0);
        if term < inner.current_term {
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
        inner.leader_id = Some(leader_id);
        inner.refresh_follower_timer();

        let prev = u64_field(req, "prev_log_index").unwrap_or(0);
        let prev_term = u64_field(req, "prev_log_term").unwrap_or(0);
        if prev > inner.last_index() as u64 {
            let next = inner.last_index() + 1;
            return json!({
                "term": inner.current_term,
                "success": false,
                "next_index": next
            });
        }
        let prev_index = prev as usize;
        if inner.log[prev_index].term != prev_term {
            return json!({
                "term": inner.current_term,
                "success": false,
                "next_index": prev_index.max(1)
            });
        }

        let mut dirty = false;
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
                        inner.log.truncate(index);
                        inner.log.push(entry);
                        dirty = true;
                    }
                } else if index == inner.log.len() {
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
            } else if !inner.election_in_progress && Instant::now() >= inner.election_deadline {
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
                || Instant::now() < inner.election_deadline
            {
                return;
            }
            inner.election_timeout = random_timeout(inner.id);
            // A failed attempt must not look like a fresh leader heartbeat.
            inner.election_deadline = Instant::now() + inner.election_timeout;
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
                peers: inner.others(),
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
            pending
                .spawn(async move { net::post_json(&peer.addr, "/raft/request-vote", &req).await });
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
            return;
        }

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
        let peers = self.inner.lock().await.others();
        for peer in peers {
            let this = Arc::clone(self);
            let req = vote_req.clone();
            tokio::spawn(async move {
                if let Ok(response) = net::post_json(&peer.addr, "/raft/request-vote", &req).await {
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
        let (peers, quorum, term) = {
            let inner = self.inner.lock().await;
            if inner.role != Role::Leader {
                return false;
            }
            (inner.others(), inner.quorum(), inner.current_term)
        };
        if peers.is_empty() {
            let mut inner = self.inner.lock().await;
            if inner.role == Role::Leader && inner.current_term == term {
                inner.last_quorum_ok = Instant::now();
                inner.advance_commit();
                return true;
            }
            return false;
        }

        let mut pending = JoinSet::new();
        for peer in peers {
            let this = Arc::clone(self);
            pending.spawn(async move { this.replicate_one(peer).await });
        }
        let mut ok = 1usize;
        while let Some(joined) = pending.join_next().await {
            if matches!(joined, Ok(RpcOutcome::CaughtUp)) {
                ok += 1;
            }
            if ok >= quorum {
                break;
            }
        }
        drop(pending);
        let mut inner = self.inner.lock().await;
        if inner.role == Role::Leader && inner.current_term == term && ok >= quorum {
            inner.last_quorum_ok = Instant::now();
            true
        } else {
            false
        }
    }

    async fn replicate_one(self: &Arc<Self>, peer: Peer) -> RpcOutcome {
        let Some(mutex) = self.peer_locks.get(&peer.id) else {
            return RpcOutcome::Unreachable;
        };
        let _guard = mutex.lock().await;
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

            let response = match net::post_json(&peer.addr, "/raft/append-entries", &request).await
            {
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
    fn new(id: u64, peers: Vec<Peer>, data_dir: Option<PathBuf>) -> Self {
        let now = Instant::now();
        let election_timeout = random_timeout(id);
        Self {
            id,
            peers,
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
        }
    }

    fn refresh_follower_timer(&mut self) {
        let now = Instant::now();
        self.last_leader_contact = now;
        self.election_deadline = now + self.election_timeout;
    }

    fn quorum(&self) -> usize {
        self.peers.len() / 2 + 1
    }

    fn last_index(&self) -> usize {
        self.log.len() - 1
    }

    fn last_term(&self) -> u64 {
        self.log[self.last_index()].term
    }

    fn others(&self) -> Vec<Peer> {
        self.peers
            .iter()
            .filter(|peer| peer.id != self.id)
            .cloned()
            .collect()
    }

    fn leader_addr(&self) -> Option<String> {
        let id = self.leader_id?;
        self.peers
            .iter()
            .find(|peer| peer.id == id)
            .map(|peer| peer.addr.clone())
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        let next = self.last_index() + 1;
        for peer in &self.peers {
            if peer.id == self.id {
                continue;
            }
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
    }

    fn step_down(&mut self) {
        if self.role == Role::Follower {
            return;
        }
        self.role = Role::Follower;
        self.leader_id = None;
        self.votes = 0;
        self.election_timeout = random_timeout(self.id);
        self.election_deadline = Instant::now() + self.election_timeout;
        self.fail_uncommitted();
    }

    fn observe_term(&mut self, term: u64) {
        if term <= self.current_term {
            return;
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
            let mut replicas = 1usize;
            for matched in self.match_index.values() {
                if *matched >= index {
                    replicas += 1;
                }
            }
            if replicas >= self.quorum() {
                commit = index;
            }
        }
        if commit > self.commit_index {
            self.commit_index = commit;
            self.apply();
            self.persist();
        }
    }

    fn apply(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            match &self.log[self.last_applied].command {
                Command::Put { key, value } => {
                    self.data.insert(key.clone(), value.clone());
                }
                Command::Delete { key } => {
                    self.data.remove(key);
                }
                Command::Noop => {}
            }
            if let Some(waiter) = self.waiters.remove(&self.last_applied) {
                let _ = waiter.send(true);
            }
        }
    }

    fn persist(&self) {
        let Some(dir) = &self.data_dir else {
            return;
        };
        let _ = std::fs::create_dir_all(dir);
        let path = dir.join(format!("{}.json", self.id));
        let tmp = dir.join(format!("{}.json.tmp", self.id));
        let log: Vec<Value> = self.log.iter().map(entry_json).collect();
        let body = json!({
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "commit_index": self.commit_index,
            "log": log,
        });
        if std::fs::write(&tmp, serde_json::to_vec(&body).unwrap_or_default()).is_ok() {
            let _ = std::fs::rename(&tmp, &path);
        }
    }

    fn load(&mut self) {
        let Some(dir) = &self.data_dir else {
            return;
        };
        let path = dir.join(format!("{}.json", self.id));
        let Ok(bytes) = std::fs::read(&path) else {
            return;
        };
        let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
            eprintln!("ignoring unreadable raft state at {}", path.display());
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
        let commit = u64_field(&value, "commit_index").unwrap_or(0) as usize;
        self.commit_index = commit.min(self.last_index());
        self.last_applied = 0;
        self.data.clear();
        self.apply();
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
    }
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
        _ => return None,
    };
    Some(Entry { term, command })
}
