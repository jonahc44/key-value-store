//! Chaos stress test. The real binary runs as a multi-process cluster while a
//! fault injector kills (SIGKILL) and freezes (SIGSTOP) random members at
//! random, frequent intervals, targeting the leader more often than chance,
//! and brings them back from their data directory after a random outage.
//! Concurrent clients meanwhile write in sporadic bursts through whichever
//! member they pick, dead or alive, as real clients would.
//!
//! Afterwards the cluster is healed and checked:
//! - every acknowledged write is readable with the value that was written;
//! - no read returned a value older than one already acknowledged;
//! - all members converge on the same applied state;
//! - all members' persisted logs agree up to the lowest commit index;
//! - no term ever had two leaders.
//!
//! It runs for a while, so it is ignored by default:
//!
//!     cargo test --release --test chaos -- --ignored --nocapture
//!
//! Environment knobs: CHAOS_SECS (default 30), CHAOS_NODES (5), CHAOS_CLIENTS
//! (8), CHAOS_MAX_DOWN (members out at once; the default keeps a majority up,
//! larger values allow full outages) and CHAOS_SEED. The seed fixes the fault
//! and client decisions, not thread timing, so a rerun is similar, not
//! identical. The data directory (raft state keyed by the random node ids
//! printed at start, event journals, nodeN.log per member, fault timeline)
//! is kept when the run fails, or always with
//! CHAOS_KEEP=1.
#![cfg(unix)]

use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "runs for CHAOS_SECS (default 30 s); use --ignored"]
async fn turbulent_cluster_keeps_acknowledged_writes() {
    let settings = Settings::from_env();
    let dir = std::env::temp_dir().join(format!("kv-chaos-{}-{}", std::process::id(), settings.seed));
    std::fs::create_dir_all(&dir).unwrap();
    eprintln!(
        "chaos: seed={} secs={} nodes={} clients={} max_down={} dir={}",
        settings.seed,
        settings.secs,
        settings.nodes,
        settings.clients,
        settings.max_down,
        dir.display()
    );

    let mut cluster = Cluster::launch(&settings, &dir);
    let addrs: Arc<Vec<String>> = Arc::new(cluster.members.iter().map(|m| m.addr.clone()).collect());
    cluster.learn_ids(HEAL_TIMEOUT).await;
    let ids: Vec<String> = cluster.members.iter().map(|m| format!("{}={}", m.n, m.id)).collect();
    eprintln!("chaos: node ids {}", ids.join(" "));
    if cluster.wait_writable(&addrs, HEAL_TIMEOUT).await.is_none() {
        cluster.stop_all().await;
        panic!("cluster never became writable; see {}", dir.display());
    }

    let mut rng = Rng::new(settings.seed);
    let stop = Arc::new(AtomicBool::new(false));
    let clients: Vec<_> = (1..=settings.clients)
        .map(|id| {
            let addrs = Arc::clone(&addrs);
            let stop = Arc::clone(&stop);
            tokio::spawn(client(id, addrs, rng.next(), stop))
        })
        .collect();

    let faults = cluster.rampage(&settings, &mut rng).await;
    stop.store(true, Ordering::Relaxed);
    let mut logs = Vec::new();
    for handle in clients {
        logs.push(handle.await.unwrap());
    }
    let chaos_elapsed = cluster.started.elapsed();

    let mut violations: Vec<String> = logs.iter().flat_map(|log| log.violations.clone()).collect();
    violations.extend(cluster.unexpected_exits.iter().cloned());

    cluster.heal().await;
    violations.extend(cluster.learn_ids(HEAL_TIMEOUT).await);
    let recovery = cluster.wait_writable(&addrs, HEAL_TIMEOUT).await;
    match recovery {
        Some(_) => {
            if let Err(err) = cluster.wait_converged(HEAL_TIMEOUT).await {
                violations.push(err);
            }
            violations.extend(verify(&addrs, &logs).await);
        }
        None => violations.push(format!(
            "cluster not writable {}s after every member came back",
            HEAL_TIMEOUT.as_secs()
        )),
    }
    cluster.stop_all().await;
    std::fs::write(dir.join("chaos.timeline"), cluster.timeline.join("\n")).unwrap();

    let ids: Vec<u64> = cluster.members.iter().map(|m| m.id).collect();
    let (agreed, log_violations) = check_logs(&dir, &ids);
    violations.extend(log_violations);
    let terms = leaders_by_term(&dir, &ids);
    for (term, leaders) in &terms {
        if leaders.len() > 1 {
            violations.push(format!("term {term} had leaders {leaders:?}"));
        }
    }

    report(&logs, &faults, chaos_elapsed, recovery, agreed, &terms);
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
    clients: u64,
    max_down: usize,
    seed: u64,
    keep: bool,
}

impl Settings {
    fn from_env() -> Self {
        let nodes: u64 = env_or("CHAOS_NODES", 5);
        assert!(nodes >= 1, "CHAOS_NODES must be at least 1");
        let clock = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        Self {
            secs: env_or("CHAOS_SECS", 30),
            nodes,
            clients: env_or("CHAOS_CLIENTS", 8),
            max_down: env_or("CHAOS_MAX_DOWN", ((nodes - 1) / 2) as usize),
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
    /// from `GET /cluster/id`; 0 until then.
    id: u64,
    addr: String,
    child: Option<Child>,
    state: State,
    /// When a frozen or downed member is brought back.
    back_at: Instant,
}

#[derive(Default)]
struct Faults {
    kills: u64,
    freezes: u64,
    aimed_at_leader: u64,
}

struct Cluster {
    dir: PathBuf,
    members: Vec<Member>,
    started: Instant,
    timeline: Vec<String>,
    unexpected_exits: Vec<String>,
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
            })
            .collect();
        drop(listeners);
        let mut cluster = Self {
            dir: dir.to_path_buf(),
            members,
            started: Instant::now(),
            timeline: Vec::new(),
            unexpected_exits: Vec::new(),
        };
        for index in 0..cluster.members.len() {
            cluster.spawn(index);
        }
        cluster
    }

    fn note(&mut self, text: String) {
        let line = format!("{:>9.3}s {text}", self.started.elapsed().as_secs_f64());
        self.timeline.push(line);
    }

    fn spawn(&mut self, index: usize) {
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
            .arg(&self.dir);
        for peer in self.members.iter().filter(|m| m.n != member.n) {
            command.arg("--peer").arg(&peer.addr);
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
        let n = member.n;
        let member = &mut self.members[index];
        member.child = Some(child);
        member.state = State::Up;
        self.note(format!("start  node {n}"));
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
                self.unexpected_exits.push(text);
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

    /// The leader according to the first live member that answers.
    async fn leader(&self) -> Option<usize> {
        for member in self.members.iter().filter(|m| m.state == State::Up) {
            if let Reply::Status(200, body) = http(&member.addr, "GET", "/cluster/members", None, PROBE_TIMEOUT).await {
                let leader = json_of(&body)["leader"].as_u64();
                return self.members.iter().position(|m| Some(m.id) == leader);
            }
        }
        None
    }

    /// Inject faults until the configured duration is up.
    async fn rampage(&mut self, settings: &Settings, rng: &mut Rng) -> Faults {
        let mut faults = Faults::default();
        let deadline = Instant::now() + Duration::from_secs(settings.secs);
        let mut next_fault = Instant::now() + rng.millis(100, 1200);
        while Instant::now() < deadline {
            self.reap();
            let now = Instant::now();
            for index in 0..self.members.len() {
                if self.members[index].state != State::Up && self.members[index].back_at <= now {
                    self.revive(index);
                }
            }
            let out = self.members.iter().filter(|m| m.state != State::Up).count();
            if now >= next_fault && out < settings.max_down {
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

    async fn heal(&mut self) {
        self.reap();
        for index in 0..self.members.len() {
            self.revive(index);
        }
    }

    /// Write a marker until one is acknowledged; returns how long that took.
    async fn wait_writable(&self, addrs: &[String], limit: Duration) -> Option<Duration> {
        let started = Instant::now();
        let mut attempt = 0;
        while started.elapsed() < limit {
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
        for member in &mut self.members {
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
async fn client(id: u64, addrs: Arc<Vec<String>>, seed: u64, stop: Arc<AtomicBool>) -> ClientLog {
    let mut rng = Rng::new(seed);
    let mut log = ClientLog {
        id,
        outcomes: BTreeMap::new(),
        acked: Vec::new(),
        register: None,
        maybe: HashSet::new(),
        violations: Vec::new(),
    };
    let register_path = format!("/r{id}");
    let mut seq = 0u64;
    let mut burst = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let addr = &addrs[rng.below(addrs.len())];
        let roll = rng.below(100);
        if roll < 10 {
            let floor = log.register;
            let reply = http(addr, "GET", &register_path, None, CLIENT_TIMEOUT).await;
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
            let reply = http(addr, "PUT", &path, Some(&value), CLIENT_TIMEOUT).await;
            log.count("PUT", &reply);
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

/// Compare persisted logs up to the lowest commit index any member saved.
/// Returns that index and any divergence found.
fn check_logs(dir: &Path, ids: &[u64]) -> (usize, Vec<String>) {
    let mut states = Vec::new();
    for id in ids {
        let path = dir.join(format!("{id}.json"));
        match std::fs::read(&path).ok().and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok()) {
            Some(state) => states.push((*id, state)),
            None => return (0, vec![format!("node {id} left no readable state at {}", path.display())]),
        }
    }
    let commit = states
        .iter()
        .map(|(_, state)| state["commit_index"].as_u64().unwrap_or(0) as usize)
        .min()
        .unwrap_or(0);
    let empty = Vec::new();
    let logs: Vec<(u64, &Vec<Value>)> = states
        .iter()
        .map(|(id, state)| (*id, state["log"].as_array().unwrap_or(&empty)))
        .collect();
    let mut violations = Vec::new();
    let (first_id, first) = logs[0];
    for &(id, log) in &logs[1..] {
        if log.len() < commit || first.len() < commit {
            violations.push(format!("node {id} or {first_id} saved fewer than {commit} committed entries"));
            continue;
        }
        if let Some(index) = (0..commit).find(|&i| log[i] != first[i]) {
            violations.push(format!(
                "committed entry {} differs: node {first_id} has {}, node {id} has {}",
                index + 1,
                first[index],
                log[index]
            ));
        }
    }
    (commit, violations)
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
) {
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
    match recovery {
        Some(time) => eprintln!("chaos: writable {:.2}s after healing", time.as_secs_f64()),
        None => eprintln!("chaos: never writable after healing"),
    }
}
