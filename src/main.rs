use key_value_store::{Peer, ServerConfig, serve};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::try_init().ok();
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

fn parse_args() -> Result<ServerConfig, String> {
    let mut args = std::env::args().skip(1);
    let mut id = 1u64;
    let mut listen = "127.0.0.1:2379".to_string();
    let mut peers = Vec::new();
    let mut data_dir = None;
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
            other => return Err(format!("unknown argument {other}\n{}", usage())),
        }
    }
    Ok(ServerConfig {
        id,
        listen,
        peers,
        data_dir,
    })
}

fn usage() -> &'static str {
    "usage: key-value-store [--id N] [--listen ADDR] [--peer ID=ADDR]... [--data-dir PATH]\n\
     \n\
     One process is one member of a Raft cluster. Pass every member, including\n\
     this one, via --peer. With no --peer flags the process is a one-node cluster.\n\
     \n\
     GET    /{key}   200 and the JSON object, or 404\n\
     PUT    /{key}   JSON object body, 201 once a majority commits it\n\
     DELETE /{key}   204 once a majority commits the delete\n\
     \n\
     Followers forward to the leader. A member that cannot reach a majority\n\
     returns 503, so a partition does not create two successful writers."
}
