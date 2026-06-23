#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose)
timeout_seconds=${HDFS_BOOTSTRAP_TIMEOUT_SECONDS:-180}
deadline=$((SECONDS + timeout_seconds))

while (( SECONDS < deadline )); do
  report=$("${compose[@]}" exec -T namenode hdfs dfsadmin -report 2>/dev/null || true)
  live_nodes=$(awk '/Live datanodes \(/ {gsub(/[^0-9]/, "", $0); print $0; exit}' <<<"${report}")
  if [[ ${live_nodes:-0} -ge 2 ]]; then
    break
  fi
  sleep 2
done

if [[ ${live_nodes:-0} -lt 2 ]]; then
  echo "HDFS bootstrap failed: expected at least 2 live DataNodes" >&2
  exit 1
fi

"${compose[@]}" exec -T namenode hdfs dfsadmin -safemode wait
"${compose[@]}" exec -T namenode hdfs dfs -mkdir -p /promo/bronze /promo/tmp
"${compose[@]}" exec -T namenode hdfs dfs -chown -R promo:supergroup /promo
"${compose[@]}" exec -T namenode hdfs dfs -chmod -R 0775 /promo
"${compose[@]}" exec -T namenode hdfs fsck /promo -blocks -locations

echo "HDFS is ready with ${live_nodes} live DataNodes"
