#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
project_name="promo-hdfs-test-${$}"
export COMPOSE_PROJECT_NAME=${project_name}
export RAW_DATA_DIR="${root_dir}/tests/fixtures"
ingest_date=2026-07-01

cleanup() {
  docker compose --profile full --profile tools down --volumes --remove-orphans
}
trap cleanup EXIT

cd "${root_dir}"
docker compose --profile full up -d namenode datanode-1 datanode-2 spark-master spark-worker
./scripts/bootstrap_hdfs.sh
./scripts/hdfs_preflight.sh

submit=(
  docker compose --profile full --profile tools run --rm spark-submit
  /opt/spark/bin/spark-submit
  --master spark://spark-master:7077
  --conf spark.executor.cores=2
  --conf spark.executor.memory=1g
  --conf spark.sql.shuffle.partitions=4
)

"${submit[@]}" /workspace/spark_jobs/ingest_bronze.py \
  --data-dir /data/raw/raw_small \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --ingest-date "${ingest_date}"

"${submit[@]}" /workspace/spark_jobs/ingest_purchases.py \
  --data-dir /data/raw/raw_small \
  --hdfs-base-uri hdfs://namenode:9000/promo

"${submit[@]}" /workspace/tests/integration/verify_bronze.py \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --ingest-date "${ingest_date}"

# Re-running the same partition must replace it rather than duplicate rows.
"${submit[@]}" /workspace/spark_jobs/ingest_bronze.py \
  --data-dir /data/raw/raw_small \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --ingest-date "${ingest_date}"
"${submit[@]}" /workspace/spark_jobs/ingest_purchases.py \
  --data-dir /data/raw/raw_small \
  --hdfs-base-uri hdfs://namenode:9000/promo
"${submit[@]}" /workspace/tests/integration/verify_bronze.py \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --ingest-date "${ingest_date}"

# A selected-month backfill must leave all other monthly partitions intact.
"${submit[@]}" /workspace/spark_jobs/ingest_purchases.py \
  --data-dir /data/raw/raw_small \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --purchase-month 2019-01
"${submit[@]}" /workspace/tests/integration/verify_bronze.py \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --ingest-date "${ingest_date}"

# Invalid typed input must fail before replacing an existing month.
if "${submit[@]}" /workspace/spark_jobs/ingest_purchases.py \
  --data-dir /data/raw/raw_invalid_purchases \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --purchase-month 2019-01; then
  echo "Invalid purchases fixture unexpectedly succeeded" >&2
  exit 1
fi
"${submit[@]}" /workspace/tests/integration/verify_bronze.py \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --ingest-date "${ingest_date}"

fsck=$(docker compose exec -T namenode hdfs fsck /promo/bronze -blocks -locations)
under_replicated=$(awk -F: '/Under replicated blocks:/ {gsub(/[^0-9]/, "", $2); print $2; exit}' <<<"${fsck}")
if [[ ${fsck} != *"Status: HEALTHY"* ]] || [[ ${under_replicated:-0} -ne 0 ]]; then
  echo "Bronze replication check failed" >&2
  echo "${fsck}" >&2
  exit 1
fi

# One DataNode still permits reads, but the write preflight must reject degradation.
docker compose stop datanode-2
docker compose exec -T namenode hdfs dfs -ls /promo/bronze >/dev/null
deadline=$((SECONDS + 60))
while ./scripts/hdfs_preflight.sh; do
  if (( SECONDS >= deadline )); then
    echo "Preflight did not detect the stopped DataNode" >&2
    exit 1
  fi
  sleep 2
done

docker compose start datanode-2
deadline=$((SECONDS + 120))
until ./scripts/hdfs_preflight.sh; do
  if (( SECONDS >= deadline )); then
    echo "HDFS did not recover replication before timeout" >&2
    exit 1
  fi
  sleep 3
done

echo "HDFS and Bronze ingestion smoke test passed"
