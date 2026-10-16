//! Chaos stress test. The real binary runs as a multi-process cluster while a
//! fault injector kills (SIGKILL) and freezes (SIGSTOP) random members at
//! random, frequent intervals, targeting the leader more often than chance,
//! and brings them back from their data directory after a random outage.
//! The membership changes too: on any tick a new member may join through
//! `--join`, or a voter (the leader more often than chance) may be removed
//! through `DELETE /cluster/members/{id}` and stopped once that is
//! acknowledged, as long as the member count stays between the configured
//! minimum and maximum. Concurrent clients meanwhile write in sporadic bursts
//! through whichever current member they pick, dead or alive, as real clients
//! would.
//!
//! Afterwards the cluster is healed and checked:
//! - every acknowledged write is readable with the value that was written;
//! - no read returned a value older than one already acknowledged;
//! - the configuration holds exactly the current members, all as voters, so
//!   every join completed and no acknowledged removal was undone;
//! - all members converge on the same applied state;
//! - all members' persisted logs agree up to the lowest commit index, where
//!   both still hold the entries (a compacted prefix is not compared), and
//!   removed members' logs agree up to their own commit index;
//! - no term ever had two leaders.
//!
//! It runs for a while, so it is ignored by default:
//!
//!     cargo test --release --test chaos -- --ignored --nocapture
//!
//! Environment knobs: CHAOS_SECS (default 30), CHAOS_NODES (5, the starting
//! size), CHAOS_MIN_NODES (3) and CHAOS_MAX_NODES (CHAOS_NODES + 2) bounding
//! the size as members join and leave, CHAOS_MEMBERSHIP_CHANCE (0.01, the
//! chance per 20 ms tick of a join or removal; 0, or equal bounds, keeps the
//! membership fixed), CHAOS_CLIENTS (8), CHAOS_MAX_DOWN (members out at once;
//! the default keeps a majority of the known voters up, larger values allow
//! full outages), CHAOS_SNAPSHOT_EVERY (500 applied entries
//! between snapshots, low so that compaction and snapshot installs happen under
//! faults; 0 never compacts), CHAOS_SNAPSHOT_CHUNK (4096 bytes per snapshot
//! request, small so transfers span many requests and kills land mid-transfer)
//! and CHAOS_SEED. The seed fixes the fault
//! and client decisions, not thread timing, so a rerun is similar, not
//! identical. The data directory (raft state keyed by the random node ids
//! printed at start, event journals, nodeN.log per member, fault timeline)
//! is kept when the run fails, or always with
//! CHAOS_KEEP=1. With CHAOS_SUMMARY=PATH a JSON summary line, tagged with
//! CHAOS_LABEL, is appended to PATH for comparing runs; it includes
//! per-window series (CHAOS_WINDOW seconds, default 10) for long runs.
#![cfg(unix)]

use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};

/// Longer than the server's 2.2 s forward timeout, so a slow forward is seen.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(3);
const PROBE_TIMEOUT: Duration = Duration::from_millis(300);
const HEAL_TIMEOUT: Duration = Duration::from_secs(30);
const STOP_TIMEOUT: Duration = Duration::from_secs(5);

/// Share of faults aimed at whoever is leader right now.
const LEADER_BIAS: f64 = 0.35;
/// Share of faults that freeze a member instead of killing it.
const FREEZE_SHARE: f64 = 0.25;
/// Share of outages long enough for the member to fall far behind.
const LONG_OUTAGE_SHARE: f64 = 0.1;
/// How often to look for joiners that have become voters.
const OBSERVE_EVERY: Duration = Duration::from_millis(500);

/// Current members' addresses, shared with the clients as members come and go.
type Addrs = Arc<RwLock<Vec<String>>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "runs for CHAOS_SECS (default 30 s); use --ignored"]
async fn turbulent_cluster_keeps_acknowledged_writes() {
    let settings = Settings::from_env();
    let dir = std::env::temp_dir().join(format!("kv-chaos-{}-{}", std::process::id(), settings.seed));
    std::fs::create_dir_all(&dir).unwrap();
    eprintln!(
        "chaos: seed={} secs={} nodes={} (min {}, max {}, change chance {}) clients={} max_down={} dir={}",
        settings.seed,
        settings.secs,
        settings.nodes,
        settings.min_nodes,
        settings.max_nodes,
        settings.membership_chance,
        settings.clients,
        settings.max_down.map_or("auto".to_string(), |cap| cap.to_string()),
        dir.display()
    );

    let mut cluster = Cluster::launch(&settings, &dir);
    let addrs = Arc::clone(&cluster.addrs);
    cluster.learn_ids(HEAL_TIMEOUT).await;
    // A member answers /cluster/id while still discovering its peers, before
    // it serves the client API; start only once every member does.
    cluster.wait_serving(HEAL_TIMEOUT).await;
    let ids: Vec<String> = cluster.members.iter().map(|m| format!("{}={}", m.n, m.id)).collect();
    eprintln!("chaos: node ids {}", ids.join(" "));
    if cluster.wait_writable(HEAL_TIMEOUT).await.is_none() {
        cluster.stop_all().await;
        panic!("cluster never became writable; see {}", dir.display());
    }

    let mut rng = Rng::new(settings.seed);
    let stop = Arc::new(AtomicBool::new(false));
    let origin = Instant::now();
    let clients: Vec<_> = (1..=settings.clients)
        .map(|id| {
            let addrs = Arc::clone(&addrs);
            let stop = Arc::clone(&stop);
            tokio::spawn(client(id, addrs, rng.next(), stop, origin))
        })
        .collect();

    let faults = cluster.rampage(&settings, &mut rng, origin).await;
    stop.store(true, Ordering::Relaxed);
    let mut logs = Vec::new();
    for handle in clients {
        logs.push(handle.await.unwrap());
    }
    let chaos_elapsed = cluster.started.elapsed();

    let mut violations: Vec<String> = logs.iter().flat_map(|log| log.violations.clone()).collect();

    cluster.heal().await;
    cluster.finish_removal(HEAL_TIMEOUT).await;
    violations.append(&mut cluster.violations);
    violations.extend(cluster.learn_ids(HEAL_TIMEOUT).await);
    let recovery = cluster.wait_writable(HEAL_TIMEOUT).await;
    match recovery {
        Some(_) => {
            if let Err(err) = cluster.wait_membership(HEAL_TIMEOUT).await {
                violations.push(err);
            }
            if let Err(err) = cluster.wait_converged(HEAL_TIMEOUT).await {
                violations.push(err);
            }
            violations.extend(verify(&Arc::new(cluster.addrs()), &logs).await);
        }
        None => violations.push(format!(
            "cluster not writable {}s after every member came back",
            HEAL_TIMEOUT.as_secs()
        )),
    }
    cluster.stop_all().await;
    std::fs::write(dir.join("chaos.timeline"), cluster.timeline.join("\n")).unwrap();

    let ids: Vec<u64> = cluster.members.iter().map(|m| m.id).collect();
    let retired: Vec<u64> = cluster.retired.iter().map(|m| m.id).collect();
    let everyone: Vec<u64> = ids.iter().chain(&retired).copied().collect();
    let (agreed, log_violations) = check_logs(&dir, &ids, &retired);
    violations.extend(log_violations);
    let terms = leaders_by_term(&dir, &everyone);
    for (term, leaders) in &terms {
        if leaders.len() > 1 {
            violations.push(format!("term {term} had leaders {leaders:?}"));
        }
    }

    let journals = journal_counts(&dir, &everyone);
    let summary = report(&logs, &faults, chaos_elapsed, recovery, agreed, &terms, &journals);
    eprintln!("chaos: {} members at the end", ids.len());
    if let Ok(path) = std::env::var("CHAOS_SUMMARY") {
        let mut summary = summary;
        summary["label"] = json!(std::env::var("CHAOS_LABEL").unwrap_or_default());
        summary["seed"] = json!(settings.seed);
        summary["max_down"] = json!(settings.max_down);
        summary["clients"] = json!(settings.clients);
        summary["nodes"] = json!(settings.nodes);
        summary["min_nodes"] = json!(settings.min_nodes);
        summary["max_nodes"] = json!(settings.max_nodes);
        summary["membership_chance"] = json!(settings.membership_chance);
        summary["final_nodes"] = json!(ids.len());
        summary["snapshot_every"] = json!(settings.snapshot_every);
        summary["snapshot_chunk"] = json!(settings.snapshot_chunk);
        summary["agreed_entries"] = json!(agreed);
        // Raft state is ID.json plus, in newer versions, the ID.log.jsonl log
        // and the ID.snapshot.json snapshot.
        summary["state_bytes_max"] = json!(ids
            .iter()
            .map(|id| {
                [format!("{id}.json"), format!("{id}.log.jsonl"), format!("{id}.snapshot.json")]
                    .iter()
                    .filter_map(|name| std::fs::metadata(dir.join(name)).ok())
                    .map(|meta| meta.len())
                    .sum::<u64>()
            })
            .max());
        summary["violations"] = json!(violations.len());
        summary["violation_samples"] = json!(violations.iter().take(5).collect::<Vec<_>>());
        let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path).unwrap();
        std::io::Write::write_all(&mut file, format!("{summary}\n").as_bytes()).unwrap();
    }
    if !violations.is_empty() {
        for violation in &violations {
            eprintln!("VIOLATION: {violation}");
        }
        panic!(
            "{} violation(s); seed {}; artifacts in {}",
            violations.len(),
            settings.seed,
            dir.display()
        );
    }
    if settings.keep {
        eprintln!("chaos: artifacts kept in {}", dir.display());
    } else {
        let _ = std::fs::remove_dir_all(&dir);
    }
}

struct Settings {
    secs: u64,
    nodes: u64,
    min_nodes: usize,
    max_nodes: usize,
    /// Chance per tick of a member joining or leaving.
    membership_chance: f64,
    clients: u64,
    /// Members out at once; `None` keeps a majority of the known voters up.
    max_down: Option<usize>,
    snapshot_every: usize,
    snapshot_chunk: usize,
    seed: u64,
    keep: bool,
}

impl Settings {
    fn from_env() -> Self {
        let nodes: u64 = env_or("CHAOS_NODES", 5);
        assert!(nodes >= 1, "CHAOS_NODES must be at least 1");
        let min_nodes = env_or("CHAOS_MIN_NODES", 3.min(nodes as usize));
        let max_nodes = env_or("CHAOS_MAX_NODES", nodes as usize + 2);
        assert!(
            1 <= min_nodes && min_nodes as u64 <= nodes && nodes <= max_nodes as u64,
            "need 1 <= CHAOS_MIN_NODES ({min_nodes}) <= CHAOS_NODES ({nodes}) <= CHAOS_MAX_NODES ({max_nodes})"
        );
        let clock = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        Self {
            secs: env_or("CHAOS_SECS", 30),
            nodes,
            min_nodes,
            max_nodes,
            membership_chance: env_or("CHAOS_MEMBERSHIP_CHANCE", 0.01),
            clients: env_or("CHAOS_CLIENTS", 8),
            max_down: std::env::var("CHAOS_MAX_DOWN").is_ok().then(|| env_or("CHAOS_MAX_DOWN", 0)),
            snapshot_every: env_or("CHAOS_SNAPSHOT_EVERY", 500),
            snapshot_chunk: env_or("CHAOS_SNAPSHOT_CHUNK", 4096),
            seed: env_or("CHAOS_SEED", clock % 1_000_000_000),
            keep: env_or("CHAOS_KEEP", 0u8) != 0,
        }
    }
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse()
            .unwrap_or_else(|_| panic!("{name}={raw} is not valid")),
        Err(_) => default,
    }
}

/// SplitMix64; good enough to scatter faults and requests.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn between(&mut self, low: u64, high: u64) -> u64 {
        low + self.next() % (high - low + 1)
    }

    fn millis(&mut self, low: u64, high: u64) -> Duration {
        Duration::from_millis(self.between(low, high))
    }

    fn chance(&mut self, p: f64) -> bool {
        ((self.next() >> 11) as f64 / (1u64 << 53) as f64) < p
    }
}

enum Reply {
    /// The member answered.
    Status(u16, Vec<u8>),
    /// The request never reached a server, so it certainly had no effect.
    Refused,
    /// Timed out or the connection broke: the request may or may not apply.
    Lost,
}

async fn http(addr: &str, method: &str, path: &str, body: Option<&Value>, timeout: Duration) -> Reply {
    let mut stream = match tokio::time::timeout(timeout, TcpStream::connect(addr)).await {
        Ok(Ok(stream)) => stream,
        _ => return Reply::Refused,
    };
    let body = body.map(|value| serde_json::to_vec(value).unwrap()).unwrap_or_default();
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    let exchange = async {
        stream.write_all(head.as_bytes()).await?;
        stream.write_all(&body).await?;
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await?;
        Ok::<_, std::io::Error>(raw)
    };
    let Ok(Ok(raw)) = tokio::time::timeout(timeout, exchange).await else {
        return Reply::Lost;
    };
    let Some(split) = raw.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Reply::Lost;
    };
    let status = std::str::from_utf8(&raw[..split])
        .ok()
        .and_then(|head| head.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok());
    match status {
        Some(status) => Reply::Status(status, raw[split + 4..].to_vec()),
        None => Reply::Lost,
    }
}

fn json_of(body: &[u8]) -> Value {
    serde_json::from_slice(body).unwrap_or(Value::Null)
}

fn gauge(metrics: &str, name: &str) -> Option<u64> {
    metrics.lines().find_map(|line| {
        line.strip_prefix(name)?
            .strip_prefix(' ')?
            .trim()
            .parse()
            .ok()
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Up,
    Frozen,
    Down,
}

struct Member {
    /// Stable 1-based label for the timeline and log file names.
    n: usize,
    /// Raft id, picked at random by the member on first start and learned
    /// from `GET /cluster/id` or a membership view; 0 until then.
    id: u64,
    addr: String,
    child: Option<Child>,
    state: State,
    /// When a frozen or downed member is brought back.
    back_at: Instant,
    /// Started with `--join` rather than as a bootstrap member.
    joiner: bool,
    /// For a joiner, the member it last asked to admit it.
    seed: Option<String>,
    /// Seen as a voter in a committed configuration.
    voter: bool,
}

#[derive(Default)]
struct Faults {
    kills: u64,
    freezes: u64,
    aimed_at_leader: u64,
    joins: u64,
    removals: u64,
    removed_leader: u64,
    /// When each fault was injected, from the start of the chaos phase.
    at: Vec<Duration>,
}

struct Cluster {
    dir: PathBuf,
    members: Vec<Member>,
    /// Members whose removal the cluster acknowledged; they are stopped.
    retired: Vec<Member>,
    /// Addresses of the bootstrap members, passed as `--peer`.
    bootstrap: Vec<String>,
    addrs: Addrs,
    next_n: usize,
    /// The member being removed, by label, and the task asking the cluster
    /// to remove it. One at a time, as the cluster changes membership.
    leaving: Option<(usize, tokio::task::JoinHandle<Result<(), String>>)>,
    started: Instant,
    timeline: Vec<String>,
    /// Problems seen while running: members exiting on their own, removals
    /// that failed.
    violations: Vec<String>,
    snapshot_every: usize,
    snapshot_chunk: usize,
}

impl Cluster {
    fn launch(settings: &Settings, dir: &Path) -> Self {
        // Hold every port until all are picked so none is handed out twice.
        let listeners: Vec<_> = (0..settings.nodes)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let members: Vec<Member> = listeners
            .iter()
            .zip(1..)
            .map(|(listener, n)| Member {
                n,
                id: 0,
                addr: listener.local_addr().unwrap().to_string(),
                child: None,
                state: State::Down,
                back_at: Instant::now(),
                joiner: false,
                seed: None,
                voter: true,
            })
            .collect();
        drop(listeners);
        let bootstrap: Vec<String> = members.iter().map(|m| m.addr.clone()).collect();
        let mut cluster = Self {
            dir: dir.to_path_buf(),
            next_n: members.len() + 1,
            members,
            retired: Vec::new(),
            addrs: Arc::new(RwLock::new(bootstrap.clone())),
            bootstrap,
            leaving: None,
            started: Instant::now(),
            timeline: Vec::new(),
            violations: Vec::new(),
            snapshot_every: settings.snapshot_every,
            snapshot_chunk: settings.snapshot_chunk,
        };
        for index in 0..cluster.members.len() {
            cluster.spawn(index);
        }
        cluster
    }

    fn addrs(&self) -> Vec<String> {
        self.addrs.read().unwrap().clone()
    }

    fn note(&mut self, text: String) {
        let line = format!("{:>9.3}s {text}", self.started.elapsed().as_secs_f64());
        self.timeline.push(line);
    }

    fn spawn(&mut self, index: usize) {
        let seed = self.members[index].joiner.then(|| self.pick_seed(index));
        let member = &self.members[index];
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(format!("node{}.log", member.n)))
            .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_key-value-store"));
        command
            .arg("--listen")
            .arg(&member.addr)
            .arg("--data-dir")
            .arg(&self.dir)
            .arg("--snapshot-every")
            .arg(self.snapshot_every.to_string())
            .arg("--snapshot-chunk")
            .arg(self.snapshot_chunk.to_string());
        // A joiner asks again on every start; the cluster ignores a repeat.
        if let Some(seed) = &seed {
            command.arg("--join").arg(seed);
        } else {
            for peer in self.bootstrap.iter().filter(|addr| **addr != member.addr) {
                command.arg("--peer").arg(peer);
            }
        }
        // The tracing subscriber writes to stdout, so capture both streams.
        let child = command
            .env("RUST_LOG", "key_value_store=info")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .kill_on_drop(true)
            .spawn()
            .expect("failed to start member");
        let via = seed
            .as_ref()
            .and_then(|seed| self.members.iter().find(|m| m.addr == *seed))
            .map(|m| format!(" joining via node {}", m.n))
            .unwrap_or_default();
        let member = &mut self.members[index];
        member.child = Some(child);
        member.state = State::Up;
        member.seed = seed;
        let n = member.n;
        self.note(format!("start  node {n}{via}"));
    }

    /// A member for joiner `index` to ask: preferably a live voter that is
    /// not leaving, as one that is still joining itself cannot admit others.
    fn pick_seed(&self, index: usize) -> String {
        let leaving = self.leaving_n();
        let others = || self.members.iter().enumerate().filter(|(i, _)| *i != index).map(|(_, m)| m);
        others()
            .find(|m| m.state == State::Up && m.voter && Some(m.n) != leaving)
            .or_else(|| others().find(|m| m.state == State::Up))
            .or_else(|| others().next())
            .map(|m| m.addr.clone())
            .expect("a joiner needs another member to ask")
    }

    fn leaving_n(&self) -> Option<usize> {
        self.leaving.as_ref().map(|(n, _)| *n)
    }

    /// Start a new member that joins the running cluster.
    fn join(&mut self) {
        let addr = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .to_string();
        self.members.push(Member {
            n: self.next_n,
            id: 0,
            addr: addr.clone(),
            child: None,
            state: State::Down,
            back_at: Instant::now(),
            joiner: true,
            seed: None,
            voter: false,
        });
        self.next_n += 1;
        self.addrs.write().unwrap().push(addr);
        self.spawn(self.members.len() - 1);
    }

    async fn crash(&mut self, index: usize, outage: Duration) {
        let member = &mut self.members[index];
        if let Some(mut child) = member.child.take() {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
        member.state = State::Down;
        member.back_at = Instant::now() + outage;
        let n = member.n;
        self.note(format!("kill   node {n} for {}ms", outage.as_millis()));
    }

    fn freeze(&mut self, index: usize, outage: Duration) {
        let member = &mut self.members[index];
        signal(member.child.as_ref(), "STOP");
        member.state = State::Frozen;
        member.back_at = Instant::now() + outage;
        let n = member.n;
        self.note(format!("freeze node {n} for {}ms", outage.as_millis()));
    }

    fn revive(&mut self, index: usize) {
        match self.members[index].state {
            State::Up => {}
            State::Frozen => {
                let member = &mut self.members[index];
                signal(member.child.as_ref(), "CONT");
                member.state = State::Up;
                let n = member.n;
                self.note(format!("thaw   node {n}"));
            }
            State::Down => self.spawn(index),
        }
    }

    /// Stop a member whose removal was acknowledged and stop handing its
    /// address to clients. It keeps its files for the final checks.
    async fn retire(&mut self, index: usize) {
        let mut member = self.members.remove(index);
        if member.state == State::Frozen {
            signal(member.child.as_ref(), "CONT");
        }
        // Gracefully, so its journal reaches disk; stop_all waits for it.
        signal(member.child.as_ref(), "INT");
        member.state = State::Down;
        self.addrs.write().unwrap().retain(|addr| *addr != member.addr);
        self.note(format!("retire node {} (id {})", member.n, member.id));
        // A joiner retries the member it was told to ask, so one still
        // asking this member would never get in; restart it to pick another.
        let stranded: Vec<usize> = (0..self.members.len())
            .filter(|&i| !self.members[i].voter && self.members[i].seed.as_deref() == Some(member.addr.as_str()))
            .collect();
        self.retired.push(member);
        for index in stranded {
            self.crash(index, Duration::ZERO).await;
            self.revive(index);
        }
    }

    /// A member that exits without being told to has crashed on its own.
    fn reap(&mut self) {
        for index in 0..self.members.len() {
            let member = &mut self.members[index];
            let Some(child) = member.child.as_mut() else {
                continue;
            };
            if let Ok(Some(status)) = child.try_wait() {
                member.child = None;
                member.state = State::Down;
                member.back_at = Instant::now() + Duration::from_millis(500);
                let text = format!("node {} exited on its own ({status}); see node{}.log", member.n, member.n);
                self.note(text.clone());
                self.violations.push(text);
            }
        }
    }

    async fn wait_serving(&self, limit: Duration) {
        let started = Instant::now();
        for member in &self.members {
            while !matches!(
                http(&member.addr, "GET", "/cluster/members", None, PROBE_TIMEOUT).await,
                Reply::Status(200, _)
            ) {
                assert!(started.elapsed() < limit, "node {} never served the client API", member.n);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    /// Ask every member for its id. Returns those that differ from the id
    /// already known, which after the first call means an id was not kept
    /// across a restart.
    async fn learn_ids(&mut self, limit: Duration) -> Vec<String> {
        let started = Instant::now();
        let mut changed = Vec::new();
        for member in &mut self.members {
            let id = loop {
                if let Reply::Status(200, body) = http(&member.addr, "GET", "/cluster/id", None, PROBE_TIMEOUT).await {
                    if let Some(id) = json_of(&body)["id"].as_u64() {
                        break id;
                    }
                }
                assert!(started.elapsed() < limit, "node {} never reported its id", member.n);
                tokio::time::sleep(Duration::from_millis(50)).await;
            };
            if member.id != 0 && member.id != id {
                changed.push(format!("node {} came back as id {id}, was {}", member.n, member.id));
            }
            member.id = id;
        }
        changed
    }

    /// The membership as the first live member that answers sees it.
    async fn view(&self) -> Option<Value> {
        for member in self.members.iter().filter(|m| m.state == State::Up) {
            if let Reply::Status(200, body) = http(&member.addr, "GET", "/cluster/members", None, PROBE_TIMEOUT).await {
                return Some(json_of(&body));
            }
        }
        None
    }

    /// Mark members that a committed configuration lists as voters, and
    /// learn their ids.
    fn observe(&mut self, view: &Value) {
        if view["committed"] != true {
            return;
        }
        for (id, addr) in voters(view) {
            if let Some(member) = self.members.iter_mut().find(|m| m.addr == addr) {
                member.voter = true;
                member.id = id;
            }
        }
    }

    /// The member a view names as leader.
    fn leader_in(&self, view: &Value) -> Option<usize> {
        let leader = view["leader"].as_u64()?;
        let (_, addr) = voters(view).into_iter().find(|(id, _)| *id == leader)?;
        self.members.iter().position(|m| m.addr == addr)
    }

    /// The leader according to the first live member that answers.
    async fn leader(&self) -> Option<usize> {
        self.view().await.and_then(|view| self.leader_in(&view))
    }

    /// How many members may be out at once. Unless set, a majority of the
    /// voters known for certain, less `excluding`, stays up. The cluster may
    /// have more voters than known (a joiner promoted since the last look),
    /// which only makes this stricter than needed.
    fn down_cap(&self, settings: &Settings, excluding: Option<usize>) -> usize {
        if let Some(cap) = settings.max_down {
            return cap;
        }
        let leaving = self.leaving_n();
        let voters = (0..self.members.len())
            .filter(|&i| self.members[i].voter && Some(self.members[i].n) != leaving && Some(i) != excluding)
            .count();
        voters.saturating_sub(1) / 2
    }

    /// Inject faults and membership changes until the configured duration is
    /// up.
    async fn rampage(&mut self, settings: &Settings, rng: &mut Rng, origin: Instant) -> Faults {
        let mut faults = Faults::default();
        let deadline = Instant::now() + Duration::from_secs(settings.secs);
        let mut next_fault = Instant::now() + rng.millis(100, 1200);
        let mut next_observe = Instant::now();
        while Instant::now() < deadline {
            self.reap();
            self.settle_removal().await;
            let now = Instant::now();
            for index in 0..self.members.len() {
                if self.members[index].state != State::Up && self.members[index].back_at <= now {
                    self.revive(index);
                }
            }
            if now >= next_observe && self.members.iter().any(|m| !m.voter) {
                next_observe = now + OBSERVE_EVERY;
                if let Some(view) = self.view().await {
                    self.observe(&view);
                }
            }
            if rng.chance(settings.membership_chance) {
                self.change_membership(settings, rng, &mut faults).await;
            }
            let out = self.members.iter().filter(|m| m.state != State::Up).count();
            if now >= next_fault && out < self.down_cap(settings, None) {
                next_fault = now + rng.millis(100, 1200);
                let mut target = None;
                if rng.chance(LEADER_BIAS) {
                    target = self.leader().await.filter(|&i| self.members[i].state == State::Up);
                    faults.aimed_at_leader += target.is_some() as u64;
                }
                let up: Vec<usize> = (0..self.members.len())
                    .filter(|&i| self.members[i].state == State::Up)
                    .collect();
                let Some(target) = target.or_else(|| up.get(rng.below(up.len().max(1))).copied()) else {
                    continue;
                };
                let outage = if rng.chance(LONG_OUTAGE_SHARE) {
                    rng.millis(5000, 10_000)
                } else {
                    rng.millis(150, 3000)
                };
                faults.at.push(origin.elapsed());
                if rng.chance(FREEZE_SHARE) {
                    faults.freezes += 1;
                    self.freeze(target, outage);
                } else {
                    faults.kills += 1;
                    self.crash(target, outage).await;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        faults
    }

    /// Add a member or start removing one, whichever keeps the member count
    /// within bounds. A member being removed still counts until the cluster
    /// acknowledges its removal.
    async fn change_membership(&mut self, settings: &Settings, rng: &mut Rng, faults: &mut Faults) {
        let size = self.members.len();
        let can_add = size < settings.max_nodes;
        let can_remove = size > settings.min_nodes && self.leaving.is_none();
        if can_add && (!can_remove || rng.chance(0.5)) {
            faults.joins += 1;
            self.join();
            return;
        }
        if !can_remove {
            return;
        }
        let Some(view) = self.view().await else {
            return;
        };
        self.observe(&view);
        // Only voters: a joiner still catching up may yet be re-added by its
        // own retried request. The last voter cannot be removed.
        let voters = voters(&view);
        if voters.len() < 2 {
            return;
        }
        let candidates: Vec<usize> = (0..self.members.len())
            .filter(|&i| voters.iter().any(|(_, addr)| *addr == self.members[i].addr))
            .collect();
        let leader = self.leader_in(&view).filter(|i| candidates.contains(i));
        let target = match leader {
            Some(leader) if rng.chance(LEADER_BIAS) => leader,
            _ => match candidates.get(rng.below(candidates.len().max(1))) {
                Some(&target) => target,
                None => return,
            },
        };
        // Removing an up voter must not leave too many of the rest out.
        let out = (0..self.members.len())
            .filter(|&i| i != target && self.members[i].state != State::Up)
            .count();
        if out > self.down_cap(settings, Some(target)) {
            return;
        }
        faults.removals += 1;
        faults.removed_leader += (Some(target) == leader) as u64;
        let member = &self.members[target];
        let (n, id) = (member.n, member.id);
        let leading = if Some(target) == leader { " (leader)" } else { "" };
        self.note(format!("remove node {n} (id {id}){leading}"));
        let task = tokio::spawn(remove_member(id, Arc::clone(&self.addrs)));
        self.leaving = Some((n, task));
    }

    /// Retire the leaving member once the cluster has acknowledged its
    /// removal.
    async fn settle_removal(&mut self) {
        if self.leaving.as_ref().is_some_and(|(_, task)| task.is_finished()) {
            let (n, task) = self.leaving.take().unwrap();
            self.removed(n, task.await.unwrap()).await;
        }
    }

    /// After healing, wait for a removal still in progress.
    async fn finish_removal(&mut self, limit: Duration) {
        let Some((n, mut task)) = self.leaving.take() else {
            return;
        };
        match tokio::time::timeout(limit, &mut task).await {
            Ok(result) => self.removed(n, result.unwrap()).await,
            Err(_) => {
                task.abort();
                self.violations.push(format!(
                    "removing node {n} was not acknowledged {}s after healing",
                    limit.as_secs()
                ));
            }
        }
    }

    async fn removed(&mut self, n: usize, result: Result<(), String>) {
        match result {
            Ok(()) => {
                let index = self.members.iter().position(|m| m.n == n).unwrap();
                self.retire(index).await;
            }
            Err(err) => {
                self.note(format!("remove node {n} failed: {err}"));
                self.violations.push(format!("node {n}: {err}"));
            }
        }
    }

    async fn heal(&mut self) {
        self.reap();
        for index in 0..self.members.len() {
            self.revive(index);
        }
    }

    /// Write a marker until one is acknowledged; returns how long that took.
    async fn wait_writable(&self, limit: Duration) -> Option<Duration> {
        let started = Instant::now();
        let mut attempt = 0;
        while started.elapsed() < limit {
            let addrs = self.addrs();
            let addr = &addrs[attempt % addrs.len()];
            attempt += 1;
            let marker = json!({ "attempt": attempt });
            if let Reply::Status(201, _) = http(addr, "PUT", "/chaos-marker", Some(&marker), CLIENT_TIMEOUT).await {
                return Some(started.elapsed());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        None
    }

    /// Wait for the leader's committed configuration to hold exactly the
    /// current members, all promoted to voters.
    async fn wait_membership(&mut self, limit: Duration) -> Result<(), String> {
        let started = Instant::now();
        let want: BTreeSet<u64> = self.members.iter().map(|m| m.id).collect();
        let retired: BTreeSet<u64> = self.retired.iter().map(|m| m.id).collect();
        let mut seen = None;
        loop {
            for member in &self.members {
                let Reply::Status(200, body) = http(&member.addr, "GET", "/cluster/members", None, PROBE_TIMEOUT).await
                else {
                    continue;
                };
                let view = json_of(&body);
                if view["id"] != view["leader"] {
                    continue;
                }
                let ids = |list: &str| -> BTreeSet<u64> {
                    view[list].as_array().into_iter().flatten().filter_map(|p| p["id"].as_u64()).collect()
                };
                let (voters, learners) = (ids("voters"), ids("learners"));
                if voters == want && learners.is_empty() && view["committed"] == true {
                    return Ok(());
                }
                // A joiner's retried request can re-add it as a learner just
                // after it was removed and stopped. Not a fault in the
                // cluster; drop it again so the check can finish.
                for &id in learners.intersection(&retired) {
                    let _ = http(&member.addr, "DELETE", &format!("/cluster/members/{id}"), None, CLIENT_TIMEOUT).await;
                }
                seen = Some(format!("voters {voters:?}, learners {learners:?}"));
                break;
            }
            if started.elapsed() > limit {
                return Err(format!(
                    "configuration never held exactly the members {want:?} as voters (removed: {retired:?}); \
                     the leader last had {}",
                    seen.unwrap_or_else(|| "no leader found".into())
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Wait for every member to apply the same log and hold the same keys.
    async fn wait_converged(&self, limit: Duration) -> Result<(), String> {
        let started = Instant::now();
        loop {
            let mut views = Vec::new();
            for member in &self.members {
                let view = match http(&member.addr, "GET", "/cluster/metrics", None, PROBE_TIMEOUT).await {
                    Reply::Status(200, body) => {
                        let text = String::from_utf8_lossy(&body).into_owned();
                        (gauge(&text, "raft_applied_index"), gauge(&text, "kv_keys"))
                    }
                    _ => (None, None),
                };
                views.push((member.n, view));
            }
            let first = views[0].1;
            if first.0.is_some() && views.iter().all(|(_, view)| *view == first) {
                return Ok(());
            }
            if started.elapsed() > limit {
                return Err(format!("members never converged (id, (applied, keys)): {views:?}"));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Stop members gracefully so their state and journals reach disk.
    async fn stop_all(&mut self) {
        for member in &mut self.members {
            if member.state == State::Frozen {
                signal(member.child.as_ref(), "CONT");
            }
            signal(member.child.as_ref(), "INT");
        }
        // Retired members were told to stop when they were retired.
        for member in self.members.iter_mut().chain(&mut self.retired) {
            if let Some(mut child) = member.child.take() {
                if tokio::time::timeout(STOP_TIMEOUT, child.wait()).await.is_err() {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                }
            }
            member.state = State::Down;
        }
    }
}

/// Voters in a membership view, as (id, address).
fn voters(view: &Value) -> Vec<(u64, String)> {
    view["voters"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|peer| Some((peer["id"].as_u64()?, peer["addr"].as_str()?.to_string())))
        .collect()
}

/// Ask the cluster through its current members to remove `id` until one
/// acknowledges. Removal is idempotent, so a lost reply is simply retried.
async fn remove_member(id: u64, addrs: Addrs) -> Result<(), String> {
    let path = format!("/cluster/members/{id}");
    for attempt in 0.. {
        let addr = {
            let addrs = addrs.read().unwrap();
            addrs[attempt % addrs.len()].clone()
        };
        match http(&addr, "DELETE", &path, None, CLIENT_TIMEOUT).await {
            Reply::Status(200, _) => return Ok(()),
            Reply::Status(409, body) => {
                return Err(format!("removal rejected: {}", String::from_utf8_lossy(&body)));
            }
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    unreachable!()
}

fn signal(child: Option<&Child>, name: &str) {
    if let Some(pid) = child.and_then(|child| child.id()) {
        let _ = std::process::Command::new("kill")
            .arg(format!("-{name}"))
            .arg(pid.to_string())
            .status();
    }
}

/// What one client did and saw.
struct ClientLog {
    id: u64,
    /// Outcome counts keyed like "PUT 201", "GET 503", "PUT lost".
    outcomes: BTreeMap<String, u64>,
    /// Sequence numbers of acknowledged writes to this client's own keys.
    acked: Vec<u64>,
    /// Last acknowledged write to this client's register key.
    register: Option<u64>,
    /// Register writes with unknown outcome; any of them may apply later.
    maybe: HashSet<u64>,
    /// Per acknowledged write: when it was sent (from the start of the
    /// chaos phase) and its round-trip time.
    acked_at: Vec<(Duration, Duration)>,
    /// When each write that got a 503 or no answer was sent.
    failed_at: Vec<Duration>,
    violations: Vec<String>,
}

impl ClientLog {
    fn count(&mut self, method: &str, reply: &Reply) {
        let outcome = match reply {
            Reply::Status(code, _) => code.to_string(),
            Reply::Refused => "refused".into(),
            Reply::Lost => "lost".into(),
        };
        *self.outcomes.entry(format!("{method} {outcome}")).or_default() += 1;
    }
}

/// Each client owns keys `u{id}-{seq}` (written once) and a register
/// `r{id}` (overwritten with rising seq), so every check is per client and
/// needs no coordination between clients.
async fn client(id: u64, addrs: Addrs, seed: u64, stop: Arc<AtomicBool>, origin: Instant) -> ClientLog {
    let mut rng = Rng::new(seed);
    let mut log = ClientLog {
        id,
        outcomes: BTreeMap::new(),
        acked: Vec::new(),
        register: None,
        maybe: HashSet::new(),
        acked_at: Vec::new(),
        failed_at: Vec::new(),
        violations: Vec::new(),
    };
    let register_path = format!("/r{id}");
    let mut seq = 0u64;
    let mut burst = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let addr = {
            let addrs = addrs.read().unwrap();
            addrs[rng.below(addrs.len())].clone()
        };
        let roll = rng.below(100);
        if roll < 10 {
            let floor = log.register;
            let reply = http(&addr, "GET", &register_path, None, CLIENT_TIMEOUT).await;
            log.count("GET", &reply);
            match reply {
                Reply::Status(200, body) => {
                    let seen = json_of(&body)["seq"].as_u64();
                    let fresh = seen.is_some_and(|v| {
                        v <= seq && (floor.is_none_or(|floor| v >= floor) || log.maybe.contains(&v))
                    });
                    if !fresh {
                        log.violations.push(format!(
                            "client {id}: read {register_path} = {seen:?} via {addr} after seq {floor:?} was acknowledged"
                        ));
                    }
                }
                Reply::Status(404, _) if floor.is_some() => log.violations.push(format!(
                    "client {id}: read {register_path} = 404 via {addr} after seq {floor:?} was acknowledged"
                )),
                _ => {}
            }
        } else {
            seq += 1;
            let register = roll < 30;
            let path = if register { register_path.clone() } else { format!("/u{id}-{seq}") };
            let value = json!({ "client": id, "seq": seq });
            let sent = Instant::now();
            let reply = http(&addr, "PUT", &path, Some(&value), CLIENT_TIMEOUT).await;
            log.count("PUT", &reply);
            match reply {
                Reply::Status(201, _) => log.acked_at.push((sent - origin, sent.elapsed())),
                Reply::Refused => {}
                _ => log.failed_at.push(sent - origin),
            }
            match reply {
                Reply::Status(201, _) if register => log.register = Some(seq),
                Reply::Status(201, _) => log.acked.push(seq),
                Reply::Refused => {}
                _ if register => {
                    log.maybe.insert(seq);
                }
                _ => {}
            }
        }

        // Sporadic pacing: mostly short gaps, sometimes a burst of
        // back-to-back requests, occasionally a lull.
        if burst > 0 {
            burst -= 1;
        } else if rng.chance(0.05) {
            burst = rng.between(5, 30);
        } else if rng.chance(0.03) {
            tokio::time::sleep(rng.millis(200, 1000)).await;
        } else {
            tokio::time::sleep(rng.millis(0, 50)).await;
        }
    }
    log
}

/// GET through members in turn until one gives an answer other than 503.
async fn settled_get(addrs: &[String], path: &str, salt: usize) -> Option<(u16, Value)> {
    for attempt in 0..20 {
        let addr = &addrs[(salt + attempt) % addrs.len()];
        if let Reply::Status(code, body) = http(addr, "GET", path, None, CLIENT_TIMEOUT).await {
            if code != 503 {
                return Some((code, json_of(&body)));
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

/// Read back every acknowledged write and every register.
async fn verify(addrs: &Arc<Vec<String>>, logs: &[ClientLog]) -> Vec<String> {
    let mut checks = tokio::task::JoinSet::new();
    for log in logs {
        let addrs = Arc::clone(addrs);
        let (id, acked, register, maybe) = (log.id, log.acked.clone(), log.register, log.maybe.clone());
        checks.spawn(async move {
            let mut violations = Vec::new();
            for (n, seq) in acked.into_iter().enumerate() {
                let path = format!("/u{id}-{seq}");
                let expected = json!({ "client": id, "seq": seq });
                match settled_get(&addrs, &path, n).await {
                    Some((200, value)) if value == expected => {}
                    other => violations.push(format!("acknowledged {path} reads back as {other:?}")),
                }
            }
            let path = format!("/r{id}");
            let fine = match settled_get(&addrs, &path, id as usize).await {
                Some((200, value)) => value["seq"]
                    .as_u64()
                    .is_some_and(|seq| Some(seq) == register || maybe.contains(&seq)),
                Some((404, _)) => register.is_none(),
                _ => false,
            };
            if !fine {
                violations.push(format!("{path} does not hold the last acknowledged seq {register:?}"));
            }
            violations
        });
    }
    let mut violations = Vec::new();
    while let Some(result) = checks.join_next().await {
        violations.extend(result.unwrap());
    }
    violations
}

/// A member's persisted log: its id, commit index, base and entries.
struct SavedLog {
    id: u64,
    commit: usize,
    base: usize,
    entries: Vec<Value>,
}

fn load_log(dir: &Path, id: u64) -> Result<SavedLog, String> {
    let path = dir.join(format!("{id}.json"));
    let Some(state) = std::fs::read(&path).ok().and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok()) else {
        return Err(format!("node {id} left no readable state at {}", path.display()));
    };
    // The commit index is in ID.commit.json, or in ID.json in older versions.
    let commit = std::fs::read(dir.join(format!("{id}.commit.json")))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .and_then(|saved| saved["commit_index"].as_u64())
        .or_else(|| state["commit_index"].as_u64())
        .unwrap_or(0) as usize;
    // The log is in ID.log.jsonl, one entry per line (a line torn by a kill
    // ends it), or inside ID.json in older versions. The first line stands for
    // the entry at the log's base: the "base" it carries after compaction,
    // else the sentinel entry 0. Line i is entry base + i.
    let entries: Vec<Value> = match state["log"].as_array() {
        Some(log) => log.clone(),
        None => std::fs::read_to_string(dir.join(format!("{id}.log.jsonl")))
            .unwrap_or_default()
            .lines()
            .map_while(|line| serde_json::from_str(line).ok())
            .collect(),
    };
    let base = entries.first().and_then(|first| first["base"].as_u64()).unwrap_or(0) as usize;
    Ok(SavedLog { id, commit, base, entries })
}

/// Compare persisted logs up to the lowest commit index any member saved,
/// over the entries each pair still holds. A removed member stopped
/// receiving entries when it left, so it is compared up to its own commit
/// index. Returns the members' common commit index and any divergence found.
fn check_logs(dir: &Path, ids: &[u64], retired: &[u64]) -> (usize, Vec<String>) {
    let load = |ids: &[u64]| ids.iter().map(|&id| load_log(dir, id)).collect::<Result<Vec<_>, _>>();
    let (logs, gone) = match (load(ids), load(retired)) {
        (Ok(logs), Ok(gone)) => (logs, gone),
        (Err(err), _) | (_, Err(err)) => return (0, vec![err]),
    };
    let commit = logs.iter().map(|log| log.commit).min().unwrap_or(0);
    let mut violations = Vec::new();
    let first = &logs[0];
    for other in &logs[1..] {
        violations.extend(compare_logs(first, other, commit));
    }
    for other in &gone {
        violations.extend(compare_logs(first, other, commit.min(other.commit)));
    }
    (commit, violations)
}

fn compare_logs(first: &SavedLog, other: &SavedLog, commit: usize) -> Option<String> {
    let (first_id, first_base, first) = (first.id, first.base, &first.entries);
    let (id, base, log) = (other.id, other.base, &other.entries);
    if base + log.len() <= commit || first_base + first.len() <= commit {
        return Some(format!("node {id} or {first_id} saved fewer than {commit} committed entries"));
    }
    // Entries at or below either base are compacted into a snapshot.
    let from = base.max(first_base) + 1;
    let index = (from..=commit).find(|&i| log[i - base] != first[i - first_base])?;
    Some(format!(
        "committed entry {index} differs: node {first_id} has {}, node {id} has {}",
        first[index - first_base],
        log[index - base]
    ))
}

/// Counts of selected journal events across members, keyed like
/// "stepped_down:removed" or "election_started".
fn journal_counts(dir: &Path, ids: &[u64]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for id in ids {
        let journal = std::fs::read_to_string(dir.join(format!("{id}.events.jsonl"))).unwrap_or_default();
        for event in journal.lines().filter_map(|line| serde_json::from_str::<Value>(line).ok()) {
            let key = match event["event"].as_str() {
                Some("stepped_down") => format!("stepped_down:{}", event["reason"].as_str().unwrap_or("?")),
                Some(kind @ ("election_started" | "log_truncated")) => kind.to_string(),
                _ => continue,
            };
            *counts.entry(key).or_default() += 1;
        }
    }
    counts
}

/// Leaders each member saw per term, from the event journals. A member killed
/// mid-write can leave a torn last line, which is skipped.
fn leaders_by_term(dir: &Path, ids: &[u64]) -> BTreeMap<u64, BTreeSet<u64>> {
    let mut terms: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();
    for id in ids {
        let journal = std::fs::read_to_string(dir.join(format!("{id}.events.jsonl"))).unwrap_or_default();
        for event in journal.lines().filter_map(|line| serde_json::from_str::<Value>(line).ok()) {
            if event["event"] == "leader_changed" {
                if let (Some(term), Some(leader)) = (event["term"].as_u64(), event["leader"].as_u64()) {
                    terms.entry(term).or_default().insert(leader);
                }
            }
        }
    }
    terms
}

fn report(
    logs: &[ClientLog],
    faults: &Faults,
    elapsed: Duration,
    recovery: Option<Duration>,
    agreed: usize,
    terms: &BTreeMap<u64, BTreeSet<u64>>,
    journals: &BTreeMap<String, u64>,
) -> Value {
    let mut outcomes: BTreeMap<&str, u64> = BTreeMap::new();
    for log in logs {
        for (outcome, count) in &log.outcomes {
            *outcomes.entry(outcome).or_default() += count;
        }
    }
    let writes: u64 = outcomes.iter().filter(|(k, _)| k.starts_with("PUT")).map(|(_, n)| n).sum();
    let acked = outcomes.get("PUT 201").copied().unwrap_or(0);
    let secs = elapsed.as_secs_f64();
    eprintln!("chaos: ran {secs:.1}s");
    eprintln!(
        "chaos: faults: {} kills, {} freezes ({} aimed at the leader)",
        faults.kills, faults.freezes, faults.aimed_at_leader
    );
    eprintln!(
        "chaos: membership: {} joins, {} removals ({} of the leader)",
        faults.joins, faults.removals, faults.removed_leader
    );
    eprintln!(
        "chaos: writes: {writes} attempted, {acked} acknowledged ({:.0}%), {:.0}/s acknowledged",
        100.0 * acked as f64 / writes.max(1) as f64,
        acked as f64 / secs
    );
    for (outcome, count) in &outcomes {
        eprintln!("chaos:   {outcome:<12} {count}");
    }
    eprintln!(
        "chaos: {} terms had a leader (highest {}); {agreed} committed entries agree on every member",
        terms.len(),
        terms.keys().last().copied().unwrap_or(0)
    );
    let mut latency: Vec<Duration> = logs.iter().flat_map(|log| log.acked_at.iter().map(|(_, rtt)| *rtt)).collect();
    latency.sort_unstable();
    let (p50, p90, p99, max) = (
        percentile_ms(&latency, 0.5),
        percentile_ms(&latency, 0.9),
        percentile_ms(&latency, 0.99),
        percentile_ms(&latency, 1.0),
    );
    eprintln!("chaos: acknowledged write latency ms: p50 {p50:.1}, p90 {p90:.1}, p99 {p99:.1}, max {max:.1}");
    for (event, count) in journals {
        eprintln!("chaos:   {event:<24} {count}");
    }
    match recovery {
        Some(time) => eprintln!("chaos: writable {:.2}s after healing", time.as_secs_f64()),
        None => eprintln!("chaos: never writable after healing"),
    }
    json!({
        "secs": secs,
        "kills": faults.kills,
        "freezes": faults.freezes,
        "joins": faults.joins,
        "removals": faults.removals,
        "removed_leader": faults.removed_leader,
        "writes": writes,
        "acked": acked,
        "acked_per_s": acked as f64 / secs,
        "unavailable": outcomes.get("PUT 503").copied().unwrap_or(0),
        "lost": outcomes.get("PUT lost").copied().unwrap_or(0),
        "p50_ms": p50,
        "p90_ms": p90,
        "p99_ms": p99,
        "max_ms": max,
        "terms": terms.len(),
        "journal": journals,
        "recovery_s": recovery.map(|time| time.as_secs_f64()),
        "windows": windows(logs, faults, elapsed),
    })
}

/// Latency at quantile `q` of sorted samples, in milliseconds.
fn percentile_ms(sorted: &[Duration], q: f64) -> f64 {
    let index = ((sorted.len() as f64 * q) as usize).min(sorted.len().saturating_sub(1));
    sorted.get(index).map_or(0.0, |d| d.as_secs_f64() * 1000.0)
}

/// Writes, failures, faults and latency per CHAOS_WINDOW seconds (default
/// 10), to show how a long run changes over time.
fn windows(logs: &[ClientLog], faults: &Faults, elapsed: Duration) -> Value {
    let width = Duration::from_secs(env_or("CHAOS_WINDOW", 10u64).max(1));
    let count = (elapsed.as_secs_f64() / width.as_secs_f64()).ceil() as usize;
    let slot = |at: Duration| ((at.as_secs_f64() / width.as_secs_f64()) as usize).min(count.saturating_sub(1));
    let mut latency = vec![Vec::new(); count];
    let mut failed = vec![0u64; count];
    let mut injected = vec![0u64; count];
    for log in logs {
        for (at, rtt) in &log.acked_at {
            latency[slot(*at)].push(*rtt);
        }
        for at in &log.failed_at {
            failed[slot(*at)] += 1;
        }
    }
    for at in &faults.at {
        injected[slot(*at)] += 1;
    }
    let rows = (0..count).map(|i| {
        latency[i].sort_unstable();
        json!({
            "start_s": i as u64 * width.as_secs(),
            "acked": latency[i].len(),
            "failed": failed[i],
            "faults": injected[i],
            "p50_ms": percentile_ms(&latency[i], 0.5),
            "p99_ms": percentile_ms(&latency[i], 0.99),
        })
    });
    Value::Array(rows.collect())
}
