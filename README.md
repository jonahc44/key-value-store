# key-value-store

A Raft-replicated key-value store in Rust. This guide covers how to build, run and test it.

## Overview

The system is a single Rust binary, `key-value-store`; each process is one member of a Raft cluster that serves JSON values over HTTP. Writes are acknowledged once a majority commits (and, with `--data-dir`, fsyncs) them; followers forward client requests to the leader.

| Path | What it holds |
| --- | --- |
| `src/main.rs` | Command-line flags and `--help` text |
| `src/api.rs` | HTTP routes: client API, `/cluster/*` admin, `/raft/*` peer RPCs |
| `src/raft.rs` | Elections, log replication, membership changes, snapshots, fsync and group commit |
| `src/keyspace.rs` | Copy-on-write key space the log is applied to |
| `src/identity.rs` | Random node ids, saved in `DATA_DIR/LISTEN.node.json` |
| `src/net.rs` | Peer HTTP client |
| `src/telemetry.rs` | Prometheus metrics and the event journal |
| `src/lib.rs` | `run()` entry point plus most unit and integration tests |
| `tests/chaos.rs` | Multi-process fault-injection stress test |

## Building

```bash
cargo build --release          # binary at target/release/key-value-store
./target/release/key-value-store --help
```

## Running the system

With no `--peer` flags a process is a one-node cluster on `127.0.0.1:2379`. For a real cluster, start every initial member with the same full `--peer` list, including itself; members wait until all peers are up, then elect a leader within about a second.

**Single node (in-memory):**

```bash
./target/release/key-value-store
```

**Three-node cluster with persistence** (run from bash; one terminal per node also works):

```bash
B=./target/release/key-value-store
PEERS="--peer 127.0.0.1:27001 --peer 127.0.0.1:27002 --peer 127.0.0.1:27003"
mkdir -p data
for i in 1 2 3; do
  $B --listen 127.0.0.1:2700$i $PEERS --data-dir data > node$i.log 2>&1 &
done
```

**Grow the cluster at runtime:** start a new process with `--join` pointing at any member. It joins as a learner and is promoted to voter once caught up (a few seconds on a small cluster).

```bash
$B --listen 127.0.0.1:27004 --join 127.0.0.1:27001 --data-dir data > node4.log 2>&1 &
```

**Stop everything:** `pkill -f key-value-store` (or Ctrl-C each process).

| Flag | Default | Meaning |
| --- | --- | --- |
| `--listen ADDR` | `127.0.0.1:2379` | Address for clients and peers; must be reachable by other members |
| `--peer ADDR` | none | Initial member (repeat once per member, including self). Takes `HOST:PORT` only |
| `--join ADDR` | none | Join a running cluster through any member. Cannot be combined with `--peer` |
| `--data-dir PATH` | none (in-memory) | Persist identity, log, term/vote, snapshots and events; survives power loss |
| `--snapshot-every N` | 8192 | Applied entries between snapshots; 0 never compacts |
| `--snapshot-chunk BYTES` | 1048576 | Max bytes per snapshot transfer request |

The removed `--id` flag is rejected: node ids are random and logged at start. Set the log level with `RUST_LOG`, e.g. `RUST_LOG=key_value_store=debug`; logs go to stderr.

Files in `--data-dir`, per member:

- `HOST_PORT.node.json` — the member's id and peers, reused on restart
- `ID.json`, `ID.commit.json`, `ID.log.jsonl` — Raft term/vote, commit index and log
- `ID.snapshot.json` — the latest snapshot, once compaction has run
- `ID.events.jsonl` — event journal plus a stats line every minute

## Using the store

Values must be JSON objects. Send requests to any member; followers forward to the leader. Against the three-node cluster above:

```bash
curl -i -X PUT -d '{"name":"ada","n":1}' localhost:27002/user1   # 201 Created
curl -i localhost:27003/user1                                  # 200 {"n":1,"name":"ada"}
curl -i -X DELETE localhost:27001/user1                        # 204 No Content
curl -i localhost:27001/user1                                  # 404 {"error":"not found"}
curl -i -X PUT -d '42' localhost:27001/x                       # 400 body must be a JSON object
```

| Endpoint | Returns |
| --- | --- |
| `GET /{key}` | 200 with the object, or 404 |
| `PUT /{key}` | 201 once a majority commits; 400 for a non-object body |
| `DELETE /{key}` | 204 once a majority commits |
| `GET /cluster/id` | `{"id": N}` for this member |
| `GET /cluster/members` | Leader, voters and learners as this member sees them |
| `POST /cluster/members` | Add a member: body `{"id": N, "addr": "HOST:PORT"}` (`--join` does this for you) |
| `DELETE /cluster/members/{id}` | Remove a member, including the leader itself |
| `GET /cluster/metrics` | Prometheus text: role, term, leader, key count, request counters |
| `GET /cluster/events` | Recent events as JSON: elections, leader changes, peers dropping out |

Any request may return 503 when the member cannot reach a majority; that is the intended behavior during a partition, not a bug. To find the leader quickly:

```bash
curl -s localhost:27001/cluster/members | python3 -m json.tool
```

## Testing

`cargo test --release` runs the default suite: 23 tests, which take about 6 seconds. Two long-running tests are marked `#[ignore]` and run only on request.

```bash
cargo test --release                               # default suite
cargo test --release <name>                        # one test or a name filter, e.g. snapshot
cargo test --release -- --nocapture                # show test output
RUST_LOG=key_value_store=debug cargo test --release <name> -- --nocapture   # with server logs
```

| Suite | Location | Covers |
| --- | --- | --- |
| Key space | `src/keyspace.rs` | Copy-on-write clones, shard-level copying |
| Identity | `src/identity.rs` | Random ids, peer id discovery, identity reuse on restart |
| Raft internals | `src/raft.rs` | Snapshot text round-trip, chunked snapshot resume |
| Cluster integration | `src/lib.rs` | Put/get/delete, forwarding, minority writes refused, restart from `--data-dir`, torn log lines, joins and removals, leader hand-off, compaction, snapshot catch-up, group commit |
| Chaos (ignored) | `tests/chaos.rs` | Real multi-process cluster under random SIGKILL/SIGSTOP |
| Snapshot stall (ignored) | `src/lib.rs` | Benchmark: hundreds of thousands of entries while snapshotting |

### Chaos test

The chaos test runs five real processes while a fault injector kills and freezes members, biased toward the leader, and eight clients write in bursts. After healing, it checks that every acknowledged write is readable, no read went backwards, all members converge, persisted logs agree, and no term had two leaders.

```bash
cargo test --release --test chaos -- --ignored --nocapture             # 30 s default
CHAOS_SECS=300 CHAOS_SEED=917605269 cargo test --release --test chaos -- --ignored --nocapture
```

A 10-second run typically injects around ten faults and verifies over a thousand acknowledged writes. Many 503s and refused connections in its output are expected under faults.

| Variable | Default | Effect |
| --- | --- | --- |
| `CHAOS_SECS` | 30 | Run length in seconds |
| `CHAOS_NODES` | 5 | Cluster size |
| `CHAOS_CLIENTS` | 8 | Concurrent writers |
| `CHAOS_MAX_DOWN` | (nodes-1)/2 | Members out at once; higher allows full outages |
| `CHAOS_SNAPSHOT_EVERY` | 500 | Low so compaction happens under faults |
| `CHAOS_SNAPSHOT_CHUNK` | 4096 | Small so kills land mid-transfer |
| `CHAOS_SEED` | from clock | Reproduces fault and client decisions (not thread timing) |
| `CHAOS_KEEP` | 0 | 1 keeps the data dir; it is always kept on failure |
| `CHAOS_SUMMARY` / `CHAOS_LABEL` | unset | Append a JSON summary line to a file, tagged with a label |
| `CHAOS_WINDOW` | 10 | Seconds per window in the summary's time series |

The run prints its seed and data directory (under `/tmp/kv-chaos-*`) first. On failure, that directory holds `nodeN.log` per member, event journals and the fault timeline. Rerun with the same `CHAOS_SEED` to get a similar run.

### Snapshot stall benchmark

```bash
cargo test --release --lib snapshot_stall -- --ignored --nocapture
```

`KV_BENCH_KEYS` sets how many keys it writes.

## Manual fault testing

Use the three-node cluster from [Running the system](#running-the-system), with `--data-dir`.

1. **Kill the leader.** Find it with `GET /cluster/members` (`leader` is an id; match it to `voters[].addr`), then `kill -9` its process. A survivor elects a new leader and `PUT` succeeds again within seconds.
2. **Lose the majority.** Kill a second member. The last one returns `503 {"error":"leader unreachable"}` for both reads and writes rather than serving stale data.
3. **Restart from disk.** Start the killed members again with the same `--listen`, `--peer` and `--data-dir` flags. They reuse their saved ids, rejoin, and every member returns the last acknowledged value.
4. **Freeze instead of kill.** `kill -STOP <pid>` then `kill -CONT <pid>` simulates a long pause; the others elect a new leader, and the resumed member rejoins as a follower.
5. **Exercise snapshots.** Start with `--snapshot-every 100 --snapshot-chunk 4096`, write a few hundred keys while one member is down, then restart it. It catches up by chunked snapshot; `ID.snapshot.json` appears in the data dir.
6. **Remove a member.** `curl -X DELETE localhost:27001/cluster/members/<id>` and check `GET /cluster/members`.

Watch `GET /cluster/events` on any member to see the elections, leader changes and drop-outs each step causes.

## Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `unknown argument --peer A --peer B ...` | Your shell passed the whole peer list as one argument. zsh does not split `$PEERS`; run the script with bash or write the flags out |
| `--id was removed` | Ids are random now. Drop `--id`, and use `--peer HOST:PORT` without `N=` |
| `--join and --peer cannot be combined` | Use `--peer` only to bootstrap; use `--join` only to add to a running cluster |
| `/cluster/members` shows `"leader": null` and empty voters | Bootstrap members are still waiting for a peer that is not up yet. Start all of them |
| `503 no leader` or `leader unreachable` | No majority is reachable. Bring members back; this is expected during outages |
| A restarted node gets a new id | It was started with a different `--listen` or `--data-dir`. Identity is keyed by both |
| `Address already in use` | A previous run is still alive: `pkill -f key-value-store` |
| Strange state after changing the cluster layout | Old files in `--data-dir` define membership after the first change. Delete the directory for a clean start |
| Chaos test failed | Rerun with the printed `CHAOS_SEED` and read `nodeN.log` and the fault timeline in the kept `/tmp/kv-chaos-*` directory |
