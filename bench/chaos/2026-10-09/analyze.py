import json, statistics, sys
from collections import defaultdict

rows = [json.loads(line) for line in open(sys.argv[1])]
groups = defaultdict(list)
for r in rows:
    groups[r["label"]].append(r)


def stats(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return "-"
    return f"{statistics.median(xs):.1f} [{min(xs):.1f}-{max(xs):.1f}]"


def step_downs(r):
    return sum(n for k, n in r["journal"].items() if k.startswith("stepped_down"))


order = sorted(groups, key=lambda l: (l.split(":")[0] != "faults", l.split(":")[0] != "load", l.split(":")[0] != "size", l))
print("median [min-max] across seeds")
cols = [
    ("runs", lambda rs: str(len(rs))),
    ("pass", lambda rs: f"{sum(r['violations'] == 0 for r in rs)}/{len(rs)}"),
    ("acked/s", lambda rs: stats([r["acked_per_s"] for r in rs])),
    ("ack%", lambda rs: stats([100 * r["acked"] / max(r["writes"], 1) for r in rs])),
    ("p50ms", lambda rs: stats([r["p50_ms"] for r in rs])),
    ("p99ms", lambda rs: stats([r["p99_ms"] for r in rs])),
    ("maxms", lambda rs: stats([r["max_ms"] for r in rs])),
    ("faults", lambda rs: stats([r["kills"] + r["freezes"] for r in rs])),
    ("terms", lambda rs: stats([r["terms"] for r in rs])),
    ("stepdowns", lambda rs: stats([step_downs(r) for r in rs])),
    ("truncs", lambda rs: stats([r["journal"].get("log_truncated", 0) for r in rs])),
    ("recover_s", lambda rs: stats([r["recovery_s"] for r in rs])),
    ("state_kB", lambda rs: stats([(r.get("state_bytes_max") or 0) / 1024 for r in rs])),
]
for label in order:
    rs = groups[label]
    print(f"\n{label}")
    for name, f in cols:
        print(f"  {name:<10} {f(rs)}")
    bad = [r for r in rs if r["violations"]]
    for r in bad:
        print(f"  FAIL seed={r['seed']}: {r['violation_samples']}")

for label in [l for l in order if l.startswith("long:")]:
    for r in groups[label]:
        print(f"\n{label} windows (start_s acked failed faults p50 p99)")
        for w in r["windows"]:
            print(f"  {w['start_s']:>5} {w['acked']:>6} {w['failed']:>6} {w['faults']:>3} {w['p50_ms']:>8.1f} {w['p99_ms']:>8.1f}")
