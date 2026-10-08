import json
import statistics
import sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "results.jsonl"
runs = defaultdict(lambda: defaultdict(list))
extras = defaultdict(list)
for line in open(path):
    row = json.loads(line)
    runs[row["workload"]][row["version"]].append(row["result"])
    if row["version"] == "new":
        extras[row["workload"]].append((row["events_dropped"], row["journal_bytes"]))


def med(results, key):
    return statistics.median(r[key] for r in results)


def spread(results, key):
    values = [r[key] for r in results]
    return (max(values) - min(values)) / statistics.median(values) * 100


print(f"{'workload':<14}{'metric':<8}{'baseline':>10}{'new':>10}{'change':>9}{'base spread':>13}{'new spread':>12}")
for workload, versions in runs.items():
    base, new = versions["base"], versions["new"]
    for key, label, scale in [("rps", "req/s", 1), ("p50_us", "p50 ms", 1000), ("p99_us", "p99 ms", 1000)]:
        b, n = med(base, key), med(new, key)
        print(
            f"{workload:<14}{label:<8}{b / scale:>10.2f}{n / scale:>10.2f}{(n - b) / b * 100:>+8.1f}%"
            f"{spread(base, key):>12.1f}%{spread(new, key):>11.1f}%"
        )
    errors = sum(r["non2xx"] + r["socket_errors"] for r in base + new)
    dropped = sum(d or 0 for d, _ in extras[workload])
    journal = [j for _, j in extras[workload] if j is not None]
    note = f"  errors={errors} events_dropped={dropped}"
    if journal:
        note += f" journal_bytes/run={statistics.median(journal):.0f}"
    print(note)
