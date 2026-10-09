"""Compare append-only log runs with the baseline campaign, paired by label and seed."""
import json, statistics, sys

base = [json.loads(l) for l in open(sys.argv[1])]
new = [json.loads(l) for l in open(sys.argv[2])]
key = lambda r: (r["label"], r["seed"])
base_by = {key(r): r for r in base}
pairs = [(base_by[key(r)], r) for r in new if key(r) in base_by]


def served(r):
    return 100 * r["acked"] / max(r["acked"] + r["unavailable"] + r["lost"], 1)


def med(xs):
    return statistics.median(xs) if xs else 0


print(f"new runs: {len(new)}, paired: {len(pairs)}, violations in new: {sum(r['violations'] for r in new)}")
labels = []
for b, n in pairs:
    if b["label"] not in labels:
        labels.append(b["label"])
print(f"\n{'label':<18}{'acked/s old->new':>22}{'served% old->new':>20}{'p50ms old->new':>20}{'p99ms old->new':>22}{'state kB old->new':>22}")
for label in labels:
    ps = [(b, n) for b, n in pairs if b["label"] == label]
    def col(f, fmt="{:.1f}"):
        return f"{fmt.format(med([f(b) for b, _ in ps]))} -> {fmt.format(med([f(n) for _, n in ps]))}"
    print(f"{label:<18}{col(lambda r: r['acked_per_s']):>22}{col(served):>20}{col(lambda r: r['p50_ms']):>20}{col(lambda r: r['p99_ms']):>22}{col(lambda r: (r.get('state_bytes_max') or 0) / 1024, '{:.0f}'):>22}")

for label in ["long:steady", "long:chaos"]:
    ps = [(b, n) for b, n in pairs if b["label"] == label]
    if not ps:
        continue
    b, n = ps[0]
    print(f"\n{label}: per 30 s window, acked/s and p50 ms (old -> new)")
    for wb, wn in zip(b["windows"][:-1], n["windows"][:-1]):
        print(f"  t={wb['start_s']:>4}s  {wb['acked'] / 30:6.1f} -> {wn['acked'] / 30:6.1f}/s   p50 {wb['p50_ms']:7.1f} -> {wn['p50_ms']:6.1f}   p99 {wb['p99_ms']:7.1f} -> {wn['p99_ms']:6.1f}")
    for r, name in [(b, "old"), (n, "new")]:
        print(f"  {name}: total acked {r['acked']}, acked/s {r['acked_per_s']:.1f}, terms {r['terms']}, step-downs {sum(v for k, v in r['journal'].items() if k.startswith('stepped_down'))}")
