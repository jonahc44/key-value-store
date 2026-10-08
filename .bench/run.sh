#!/usr/bin/env bash
# Alternating A/B overhead benchmark: baseline (HEAD) vs telemetry build.
set -u
cd "$(dirname "$0")"
DURATION=${DURATION:-15s}
ROUNDS=${ROUNDS:-3}
OUT=results.jsonl
: > "$OUT"
declare -A BIN=([base]=target-base/release/key-value-store [new]=target-new/release/key-value-store)
P="--peer 1=127.0.0.1:24791 --peer 2=127.0.0.1:24792 --peer 3=127.0.0.1:24793"
PIDS=()

start_cluster() { # version persist
  local dir=""
  rm -rf data && mkdir -p data
  [ "$2" = yes ] && dir="--data-dir data"
  PIDS=()
  for i in 1 2 3; do
    ${BIN[$1]} --id $i --listen 127.0.0.1:2479$i $P $dir </dev/null >/dev/null 2>&1 &
    PIDS+=($!)
  done
  for _ in $(seq 100); do
    LEADER=$(curl -s -m1 localhost:24791/cluster/members 2>/dev/null |
      python3 -c 'import json,sys;l=json.load(sys.stdin)["leader"];print(l if l else "")' 2>/dev/null)
    [ -n "$LEADER" ] && break
    sleep 0.1
  done
  FOLLOWER=$(( LEADER % 3 + 1 ))
  curl -s -X PUT -d '{"v":1}' localhost:2479$LEADER/hot >/dev/null
}

stop_cluster() {
  kill -INT "${PIDS[@]}" 2>/dev/null
  wait "${PIDS[@]}" 2>/dev/null
}

run() { # workload version
  local workload=$1 version=$2 persist=no script=put.lua target
  case $workload in
    get_leader) script=get.lua ;;
    put_persist) persist=yes ;;
  esac
  start_cluster "$version" "$persist"
  target=$LEADER
  [ "$workload" = put_follower ] && target=$FOLLOWER
  local result
  result=$(wrk -t4 -c32 -d"$DURATION" -s "$script" "http://127.0.0.1:2479$target" | sed -n 's/^RESULT //p')
  local dropped=null journal=null
  if [ "$version" = new ]; then
    dropped=$(curl -s localhost:2479$LEADER/cluster/metrics | awk '/^kv_events_dropped_total/ {print $2}')
  fi
  stop_cluster
  if [ "$version" = new ] && [ "$persist" = yes ]; then
    journal=$(cat data/*.events.jsonl | wc -c)
  fi
  echo "{\"workload\":\"$workload\",\"version\":\"$version\",\"result\":$result,\"events_dropped\":${dropped:-null},\"journal_bytes\":$journal}" | tee -a "$OUT"
}

for workload in get_leader put_leader put_follower put_persist; do
  for round in $(seq "$ROUNDS"); do
    # Alternate which version goes first so drift does not favour one side.
    if (( round % 2 )); then order="base new"; else order="new base"; fi
    for version in $order; do run "$workload" "$version"; done
  done
done
rm -rf data
echo DONE
