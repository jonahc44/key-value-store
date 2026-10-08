use key_value_store::{Peer, ServerConfig, serve};

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
    if let Err(err) = serve(config).await {
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

fn parse_args() -> Result<ServerConfig, String> {
    let mut args = std::env::args().skip(1);
    let mut id = 1u64;
    let mut listen = "127.0.0.1:2379".to_string();
    let mut peers = Vec::new();
    let mut data_dir = None;
    let mut join = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            "--id" => {
                id = args
                    .next()
                    .ok_or_else(|| "--id needs a number".to_string())?
                    .parse()
                    .map_err(|_| "--id needs a number".to_string())?;
            }
            "--listen" => {
                listen = args
                    .next()
                    .ok_or_else(|| "--listen needs an address".to_string())?;
            }
            "--peer" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--peer needs ID=ADDR".to_string())?;
                let (id_text, addr) = raw
                    .split_once('=')
                    .ok_or_else(|| "--peer must look like ID=HOST:PORT".to_string())?;
                let peer_id = id_text
                    .parse()
                    .map_err(|_| format!("bad peer id in {raw}"))?;
                if addr.is_empty() {
                    return Err(format!("missing address in {raw}"));
                }
                peers.push(Peer {
                    id: peer_id,
                    addr: addr.to_string(),
                });
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
            other => return Err(format!("unknown argument {other}\n{}", usage())),
        }
    }
    if join.is_some() && !peers.is_empty() {
        return Err("--join and --peer cannot be combined".to_string());
    }
    Ok(ServerConfig {
        id,
        listen,
        peers,
        data_dir,
        join,
    })
}

fn usage() -> &'static str {
    "usage: key-value-store [--id N] [--listen ADDR] [--peer ID=ADDR]... [--join ADDR] [--data-dir PATH]\n\
     \n\
     One process is one member of a Raft cluster. To bootstrap a cluster, pass\n\
     every initial member, including this one, via --peer. With no --peer flags\n\
     the process is a one-node cluster.\n\
     \n\
     To grow a running cluster, start a new process with a fresh id and\n\
     --join ADDR, where ADDR is any existing member. It joins as a non-voting\n\
     learner, receives the log, and is promoted to voter once caught up.\n\
     --listen must be an address the other members can reach. After the first\n\
     membership change, the log, not --peer, defines the cluster.\n\
     \n\
     GET    /{key}   200 and the JSON object, or 404\n\
     PUT    /{key}   JSON object body, 201 once a majority commits it\n\
     DELETE /{key}   204 once a majority commits the delete\n\
     \n\
     GET    /cluster/members   this member's view of voters and learners\n\
     POST   /cluster/members   {\"id\": N, \"addr\": \"HOST:PORT\"} adds a member\n\
     \n\
     Followers forward to the leader. A member that cannot reach a majority\n\
     returns 503, so a partition does not create two successful writers.\n\
     \n\
     Diagnostics go to stderr. RUST_LOG selects the level, for example\n\
     RUST_LOG=key_value_store=debug. The default is info for this crate."
}
