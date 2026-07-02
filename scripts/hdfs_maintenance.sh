#!/usr/bin/env bash
set -euo pipefail

keep_snapshots=${GOLD_SNAPSHOTS_TO_KEEP:-2}
backup_dir=${NAMENODE_BACKUP_DIR:-/var/backups/namenode}

mapfile -t entities < <(hdfs dfs -ls /promo/gold 2>/dev/null | awk '$1 ~ /^d/ {print $8}')
for entity in "${entities[@]}"; do
  mapfile -t snapshots < <(
    hdfs dfs -ls "${entity}" 2>/dev/null \
      | awk '$1 ~ /^d/ && $8 ~ /snapshot_date=/ {print $8}' \
      | sort
  )
  remove_count=$(( ${#snapshots[@]} - keep_snapshots ))
  if (( remove_count > 0 )); then
    for ((index = 0; index < remove_count; index++)); do
      hdfs dfs -rm -r -skipTrash "${snapshots[$index]}"
    done
  fi
done

mkdir -p "${backup_dir}"
hdfs dfsadmin -fetchImage "${backup_dir}"
