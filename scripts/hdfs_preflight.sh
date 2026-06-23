#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose)
report=$("${compose[@]}" exec -T namenode hdfs dfsadmin -report)
live_nodes=$(awk '/Live datanodes \(/ {gsub(/[^0-9]/, "", $0); print $0; exit}' <<<"${report}")

if [[ ${live_nodes:-0} -lt 2 ]]; then
  echo "HDFS preflight failed: ${live_nodes:-0} live DataNodes; 2 required" >&2
  exit 1
fi

safe_mode=$("${compose[@]}" exec -T namenode hdfs dfsadmin -safemode get)
if [[ ${safe_mode} != *"OFF"* ]]; then
  echo "HDFS preflight failed: ${safe_mode}" >&2
  exit 1
fi

while read -r used_percent; do
  used_integer=${used_percent%.*}
  if (( used_integer >= 85 )); then
    echo "HDFS preflight failed: DataNode usage ${used_percent}% is at or above 85%" >&2
    exit 1
  fi
done < <(awk '/DFS Used%:/ {gsub(/%/, "", $3); print $3}' <<<"${report}")

fsck=$("${compose[@]}" exec -T namenode hdfs fsck /promo -blocks 2>&1)
if [[ ${fsck} != *"Status: HEALTHY"* ]]; then
  echo "HDFS preflight failed: /promo is not healthy" >&2
  echo "${fsck}" >&2
  exit 1
fi
under_replicated=$(awk -F: '/Under replicated blocks:/ {gsub(/[^0-9]/, "", $2); print $2; exit}' <<<"${fsck}")
if [[ ${under_replicated:-0} -ne 0 ]]; then
  echo "HDFS preflight failed: ${under_replicated} under-replicated blocks" >&2
  exit 1
fi

echo "HDFS preflight passed"
