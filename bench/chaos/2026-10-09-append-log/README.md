# Append-only log vs. full rewrite, 2026-10-09

Code under test: `0cf35c3` on `append-only-log`. Raft entries are appended to
`ID.log.jsonl` instead of the whole log being re-serialized into `ID.json` on
every persist. It is compared with the baseline campaign in
[`../2026-10-09`](../2026-10-09) (`cf0b9a5`), pairing runs by label and seed:
the fault sweep (seeds 1-3), 32 clients (seeds 1-3) and both 15-minute soaks.

- `summary.jsonl`: the 20 measured runs. `runs.log`: their exit status.
- `recheck.jsonl`: the three no-fault runs repeated after the startup fix below.
- `compare.py BASE NEW`: prints the paired comparison, e.g.
  `python3 compare.py ../2026-10-09/summary.jsonl summary.jsonl`.

## Results (median of paired runs, old -> new)

| Run | Acked writes/s | Served | p50 ms | p99 ms |
|---|---|---|---|---|
| 15 min, no faults | 25.4 -> **314.0** | 100% -> 100% | 244 -> **0.5** | 739 -> **7.2** |
| 15 min, ≤2 down | 19.2 -> **95.4** | 25% -> 68% | 114 -> **0.6** | 970 -> **7.5** |
| 60 s, 0 down | 91.9 -> **328.3** | 100% -> 100% | 54.7 -> 0.5 | 154 -> 7.1 |
| 60 s, ≤1 down | 76.3 -> 180.3 | 55% -> 81% | 35.7 -> 0.5 | 97 -> 7.2 |
| 60 s, ≤2 down | 53.3 -> 93.3 | 62% -> 71% | 21.9 -> 0.5 | 85 -> 7.5 |
| 60 s, ≤3 down | 24.9 -> 46.8 | 23% -> 41% | 9.2 -> 0.5 | 404 -> 13.0 |
| 60 s, ≤4 down | 24.4 -> 31.4 | 31% -> 35% | 8.8 -> 0.5 | 308 -> 50.7 |
| 60 s, 32 clients, ≤2 down | 67.7 -> **323.8** | 28% -> 66% | 79.3 -> 0.6 | 1042 -> 7.7 |

"Served" = acked / (acked + 503 + no answer), so requests to downed members
are left out.

- **The slowdown over time is gone.** In the no-fault soak, every 30 s window
  ran at 286-346 writes/s with p50 0.5 ms and p99 7.0-7.4 ms, from the first
  window to the last. Before, throughput fell from 124/s to 12/s and p50 rose
  from 33 ms to 600 ms. That soak acknowledged 282,955 writes, against 22,873
  before (12x).
- Under faults the remaining limit is availability, not disk: throughput
  follows how often a majority and a leader are up. More of the requests that
  reach a live member now succeed, because proposals no longer time out
  queued behind a slow persist.
- State size is about 1.3 MB after a 60 s no-fault run (20k entries). It is
  appended to, not rewritten. The log still grows without bound; snapshots or
  compaction are the next step for long-lived clusters. (The 20-run summary
  shows 0 kB because the test then measured only `ID.json`; fixed in
  `e0f120d`.)

## A real bug found on the way: 404 during startup

One run failed: `faults:down=0`, seed 1, with 2 violations. Clients read
`/r1` and `/r2` as 404 through member 1 after writes to them were
acknowledged. The cause was not the log change. Member 1 was still
discovering its peers' ids, about 1 s longer than the others, and during that
time its port is served by the discovery router. That router answers
`/cluster/id` and returned a bare 404 for every other path, which reads as
"no such key". The cluster was already writable through the other members,
so the chaos phase had started. No handler on any member logged a 404,
which pointed to this.

- Fixed in `e20450f`: the discovery router now answers 503 "starting up",
  so clients retry.
- Fixed in `e0f120d`: the chaos test waits until every member serves the
  client API before starting.
- Rerun after the fixes (`recheck.jsonl`): all three no-fault seeds pass,
  100% acknowledged, 310-331 writes/s.

This affects only a member's first start, since restarts load the saved id
and skip discovery. Every other run, 19 of 20, passed all checks: no lost
acknowledged writes, no stale reads, logs agree on every committed entry, and
one leader per term. That includes the 15-minute soak with ≤2 down, which
had 768 faults and 317 terms.
