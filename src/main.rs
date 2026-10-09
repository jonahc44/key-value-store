use key_value_store::{DEFAULT_SNAPSHOT_EVERY, Options, run};

#[tokio::main]
async fn main() {
    init_tracing();
    let config = match parse_args() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(2);
        }
    };
    if let Err(err) = run(config).await {
        eprintln!("server error: {err}");
        std::process::exit(1);
    }
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("key_value_store=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()
        .ok();
}

fn parse_args() -> Result<Options, String> {
    let mut args = std::env::args().skip(1);
    let mut listen = "127.0.0.1:2379".to_string();
    let mut peers = Vec::new();
    let mut data_dir = None;
    let mut join = None;
    let mut snapshot_every = DEFAULT_SNAPSHOT_EVERY;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            "--id" => {
                return Err("--id was removed: node ids are now assigned at random".to_string());
            }
            "--listen" => {
                listen = args
                    .next()
                    .ok_or_else(|| "--listen needs an address".to_string())?;
            }
            "--peer" => {
                let addr = args
                    .next()
                    .filter(|addr| !addr.is_empty())
                    .ok_or_else(|| "--peer needs an address".to_string())?;
                if addr.contains('=') {
                    return Err(format!(
                        "--peer takes HOST:PORT only; ids are assigned at random (got {addr})"
                    ));
                }
                peers.push(addr);
            }
            "--data-dir" => {
                data_dir = Some(
                    args.next()
                        .ok_or_else(|| "--data-dir needs a path".to_string())?
                        .into(),
                );
            }
            "--join" => {
                join = Some(
                    args.next()
                        .ok_or_else(|| "--join needs an address".to_string())?,
                );
            }
            "--snapshot-every" => {
                snapshot_every = args
                    .next()
                    .and_then(|n| n.parse().ok())
                    .ok_or_else(|| "--snapshot-every needs a number of entries".to_string())?;
            }
            other => return Err(format!("unknown argument {other}\n{}", usage())),
        }
    }
    if join.is_some() && !peers.is_empty() {
        return Err("--join and --peer cannot be combined".to_string());
    }
    Ok(Options {
        listen,
        peers,
        data_dir,
        join,
        snapshot_every,
    })
}

fn usage() -> &'static str {
    "usage: key-value-store [--listen ADDR] [--peer ADDR]... [--join ADDR] [--data-dir PATH]\n\
                       [--snapshot-every N]\n\
     \n\
     One process is one member of a Raft cluster. To bootstrap a cluster, pass\n\
     every initial member, including this one, via --peer. With no --peer flags\n\
     the process is a one-node cluster.\n\
     \n\
     Each member picks a random id on first start and logs it. Bootstrap\n\
     members learn each other's ids from their --peer addresses, so they wait\n\
     until every peer is up. With --data-dir, the id and peers are saved in\n\
     DATA_DIR/LISTEN.node.json and reused on restart.\n\
     \n\
     To grow a running cluster, start a new process with --join ADDR, where ADDR is any existing member. It joins as a non-voting\n\
     learner, receives the log, and is promoted to voter once caught up.\n\
     --listen must be an address the other members can reach. After the first\n\
     membership change, the log, not --peer, defines the cluster.\n\
     \n\
     GET    /{key}   200 and the JSON object, or 404\n\
     PUT    /{key}   JSON object body, 201 once a majority commits it\n\
     DELETE /{key}   204 once a majority commits the delete\n\
     \n\
     GET    /cluster/id        {\"id\": N} for this member\n\
     GET    /cluster/members   this member's view of voters and learners\n\
     POST   /cluster/members   {\"id\": N, \"addr\": \"HOST:PORT\"} adds a member\n\
     GET    /cluster/metrics   Prometheus metrics for this member and its peers\n\
     GET    /cluster/events    recent events: peers dropping out, elections, ...\n\
     \n\
     The log is compacted: every N applied entries (--snapshot-every, default\n\
     8192; 0 never compacts) a member snapshots its keys and drops the log\n\
     before them, so memory, disk and restart time track the key space\n\
     rather than the write history. With --data-dir the snapshot is saved in\n\
     DATA_DIR/ID.snapshot.json. A member too far behind gets the snapshot.\n\
     \n\
     With --data-dir, events and a stats snapshot every minute are also\n\
     appended to DATA_DIR/ID.events.jsonl for analysis over time.\n\
     \n\
     Followers forward to the leader. A member that cannot reach a majority\n\
     returns 503, so a partition does not create two successful writers.\n\
     \n\
     Diagnostics go to stderr. RUST_LOG selects the level, for example\n\
     RUST_LOG=key_value_store=debug. The default is info for this crate."
}
