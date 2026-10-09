# Chaos campaign, 2026-10-09

Code under test: `cf0b9a5` (main `88d9db5`: replication variant C, plus the
chaos test with per-window reporting). 52 runs of `tests/chaos.rs`, one at a
time on a 16-core Linux machine, about 82 minutes of chaos in all.

- `summary.jsonl`: one JSON line per run (`CHAOS_SUMMARY` output), including
  per-window series for the long runs.
- `runs.log`: exit status, wall time and violations per run.
- `run.sh`: reruns the campaign; `analyze.py summary.jsonl` prints the tables.

## Headline

- **Safety held everywhere.** 52/52 runs passed with 0 violations, across 3,068
  injected faults (2,290 kills, 778 freezes), 1,138 leader terms, 360 log
  truncations and 657,382 client writes (208,246 acknowledged). Every
  acknowledged write was read back, no read was stale, members converged,
  committed logs agreed and no term had two leaders. No member crashed on its
  own. After faults stopped, the cluster was writable again within 0.8 s in
  every run.
- **Throughput collapses as the log grows, faults or not.** In the 15-minute
  no-fault soak, acknowledged writes fell from 124/s to about 12/s (-90%) and
  median latency rose from 33 ms to about 600 ms. Throughput times log length
  stayed roughly constant (230k-316k) for the whole run: each write costs time
  proportional to the log's size. The likely cause is that every append
  (`src/raft.rs:1402` on the leader, `:709` on followers) calls `persist()`,
  whose `write_state` serializes the entire log to JSON and rewrites the
  file, while holding the node's `inner` lock. This was not profiled.
- Because of that, the 60 s results below are partly a measure of how big the
  log got. Runs that commit less, such as heavy-fault or 2-client runs, have
  smaller logs and so look faster per write.

## Fault intensity (5 nodes, 8 clients, 60 s, 5 seeds; median [min-max])

"Served" = acknowledged / (acknowledged + 503 + no answer). It leaves out
requests sent to a member that was down, which are refused outright.

| Members down at once | Acked/s | Served | p50 ms | p99 ms | Terms | Step-downs |
|---|---|---|---|---|---|---|
| 0 | 91.8 [90.2-92.7] | 100% | 53.8 | 153.7 | 1 | 0 |
| ≤1 | 76.1 [56.4-78.1] | 66.7% | 35.3 | 100.2 | 13 | 2 |
| ≤2 | 53.3 [40.2-55.8] | 55.6% | 21.9 | 85.1 | 18 | 2 |
| ≤3 (quorum lost at times) | 23.5 [22.3-27.3] | 25.7% | 8.9 | 403.7 | 17 | 6 |
| ≤4 | 24.4 [14.1-26.9] | 31.4% | 8.8 | 190.5 | 13 | 4 |

With faults on, the slowest acknowledged write in a run usually reaches
1.4-3.0 s (median 2.2-2.7 s at ≤1 and ≤2 down). The ceiling is the clients'
3 s timeout: these are writes held up through a leader failover.

## Client load (≤2 down, 60 s, 5 seeds)

| Clients | Acked/s | Served | p50 ms | p99 ms | Log truncations |
|---|---|---|---|---|---|
| 2 | 22.2 | 64.0% | 5.2 | 17.6 | 2 |
| 8 | 53.3 | 55.6% | 21.9 | 85.1 | 4 |
| 16 | 69.2 | 42.1% | 45.8 | 307.9 | 8 |
| 32 | 68.8 | 32.5% | 79.3 | 1041.5 | 13 |

Throughput saturates near 70/s by 16 clients. More clients only add latency,
lower the served share and cause more truncated (uncommitted, overwritten)
entries.

## Cluster size (8 clients, a minority down, 60 s, 5 seeds)

| Members (max down) | Acked/s | Served | p50 ms | p99 ms | Terms |
|---|---|---|---|---|---|
| 3 (≤1) | 61.8 | 60.4% | 20.8 | 86.6 | 16 |
| 5 (≤2) | 53.3 | 55.6% | 21.9 | 85.1 | 18 |
| 7 (≤3) | 48.1 | 51.0% | 21.3 | 199.4 | 22 |

## Long runs (15 min, 30 s windows)

| Minute | No faults: acked/s | p50 ms | p99 ms | ≤2 down: acked/s | p50 ms |
|---|---|---|---|---|---|
| 0-0.5 | 124.4 | 33 | 93 | 71.6 | 13 |
| 2.5-3 | 31.0 | 213 | 316 | 10.4 | 73 |
| 5-5.5 | 22.6 | 285 | 445 | 14.0 | 132 |
| 10-10.5 | 16.0 | 419 | 675 | 11.5 | 199 |
| 14.5-15 | 12.0 | 567 | 1157 | 6.8 | 256 |

By the end the largest state file was 1.5 MB (no faults) and 1.3 MB (with
faults), and it is rewritten in full on every append.
