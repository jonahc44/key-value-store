#!/bin/bash
# Chaos campaign: fault, load and cluster-size sweeps (60 s x 5 seeds each)
# plus two 15-minute soaks. Runs one at a time; about 90 minutes in all.
# Appends to summary.jsonl and runs.log next to this script; summarize with
#   python3 analyze.py summary.jsonl
C=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-$C/summary.jsonl}
LOG=${LOG:-$C/runs.log}
cd "$C/../../.." || exit 1
cargo test --release --test chaos --no-run 2>&1 | tail -1
run() {
    local label=$1; shift
    local started=$(date +%s)
    env CHAOS_SUMMARY="$OUT" CHAOS_LABEL="$label" "$@" \
        cargo test --release --test chaos -- --ignored --nocapture > "$C/last.out" 2>&1
    local status=$?
    echo "$(date -Is) $label $* exit=$status wall=$(( $(date +%s) - started ))s $(grep -c VIOLATION "$C/last.out") violations" >> "$LOG"
    grep VIOLATION "$C/last.out" | head -5 >> "$LOG"
}
for seed in 1 2 3 4 5; do
    for down in 0 1 2 3 4; do
        run "faults:down=$down" CHAOS_SECS=60 CHAOS_MAX_DOWN=$down CHAOS_SEED=$seed
    done
    for clients in 2 16 32; do
        run "load:clients=$clients" CHAOS_SECS=60 CHAOS_MAX_DOWN=2 CHAOS_CLIENTS=$clients CHAOS_SEED=$seed
    done
    run "size:nodes=3" CHAOS_SECS=60 CHAOS_NODES=3 CHAOS_MAX_DOWN=1 CHAOS_SEED=$seed
    run "size:nodes=7" CHAOS_SECS=60 CHAOS_NODES=7 CHAOS_MAX_DOWN=3 CHAOS_SEED=$seed
done
run "long:steady" CHAOS_SECS=900 CHAOS_MAX_DOWN=0 CHAOS_WINDOW=30 CHAOS_SEED=100
run "long:chaos" CHAOS_SECS=900 CHAOS_MAX_DOWN=2 CHAOS_WINDOW=30 CHAOS_SEED=101
echo "$(date -Is) done" >> "$LOG"
