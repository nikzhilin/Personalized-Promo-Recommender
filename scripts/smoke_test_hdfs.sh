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
docker compose --profile full --profile tools build trainer
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

# Build the complete Silver fixture and verify data, rejects, and lineage.
"${submit[@]}" /workspace/spark_jobs/build_silver_dimensions.py \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --bronze-ingest-date "${ingest_date}" \
  --snapshot-date "${ingest_date}"
"${submit[@]}" /workspace/spark_jobs/clean_silver_purchases.py \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --dimensions-snapshot-date "${ingest_date}" \
  --snapshot-date "${ingest_date}"
"${submit[@]}" /workspace/tests/integration/verify_silver.py \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --snapshot-date "${ingest_date}"

# A Silver month backfill must not remove other published months.
"${submit[@]}" /workspace/spark_jobs/clean_silver_purchases.py \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --dimensions-snapshot-date "${ingest_date}" \
  --snapshot-date "${ingest_date}" \
  --purchase-month 2019-01
"${submit[@]}" /workspace/tests/integration/verify_silver.py \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --snapshot-date "${ingest_date}"

docker compose exec -T namenode hdfs dfs -test -e \
  "/promo/silver/metadata/dimensions/snapshot_date=${ingest_date}/_metadata.json"
docker compose exec -T namenode hdfs dfs -test -e \
  "/promo/silver/metadata/purchases/snapshot_date=${ingest_date}/purchase_month=2019-01/_metadata.json"

# Gold user features must honor the half-open cutoff and replace a rerun atomically.
feature_cutoff=2019-03-01T00:00:00
feature_snapshot=2019-03-01
"${submit[@]}" /workspace/tests/integration/seed_feedback_events.py \
  --hdfs-base-uri hdfs://namenode:9000/promo
for _ in 1 2; do
  "${submit[@]}" /workspace/spark_jobs/build_feedback_features.py \
    --hdfs-base-uri hdfs://namenode:9000/promo \
    --dimensions-snapshot-date "${ingest_date}" \
    --feature-cutoff "${feature_cutoff}" \
    --lookback-days 180
  "${submit[@]}" /workspace/spark_jobs/build_user_features.py \
    --hdfs-base-uri hdfs://namenode:9000/promo \
    --dimensions-snapshot-date "${ingest_date}" \
    --feature-cutoff "${feature_cutoff}" \
    --lookback-days 180
  "${submit[@]}" /workspace/tests/integration/verify_user_features.py \
    --hdfs-base-uri hdfs://namenode:9000/promo \
    --snapshot-date "${feature_snapshot}"
done

# Complete Gold features must cover all clients without a client-product Cartesian join.
for gold_job in build_item_features generate_candidates build_user_item_features; do
  "${submit[@]}" "/workspace/spark_jobs/${gold_job}.py" \
    --hdfs-base-uri hdfs://namenode:9000/promo \
    --dimensions-snapshot-date "${ingest_date}" \
    --feature-cutoff "${feature_cutoff}" \
    --lookback-days 180
done
"${submit[@]}" /workspace/tests/integration/verify_gold_features.py \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --snapshot-date "${feature_snapshot}"

# Uplift dataset uses an isolated balanced campaign fixture and publishes atomically.
uplift_dimensions_snapshot=2026-07-04
uplift_feature_cutoff=2019-03-05T00:00:00
uplift_feature_snapshot=2019-03-05
"${submit[@]}" /workspace/tests/integration/seed_uplift_dataset.py \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --source-dimensions-snapshot-date "${ingest_date}" \
  --source-feature-snapshot-date "${feature_snapshot}" \
  --target-dimensions-snapshot-date "${uplift_dimensions_snapshot}" \
  --target-feature-cutoff "${uplift_feature_cutoff}"
for _ in 1 2; do
  "${submit[@]}" /workspace/training/build_uplift_dataset.py \
    --hdfs-base-uri hdfs://namenode:9000/promo \
    --dimensions-snapshot-date "${uplift_dimensions_snapshot}" \
    --feature-cutoff "${uplift_feature_cutoff}" \
    --lookback-days 180
  "${submit[@]}" /workspace/tests/integration/verify_uplift_dataset.py \
    --hdfs-base-uri hdfs://namenode:9000/promo \
    --snapshot-date "${uplift_feature_snapshot}"
done
if "${submit[@]}" /workspace/training/build_uplift_dataset.py \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --dimensions-snapshot-date "${uplift_dimensions_snapshot}" \
  --feature-cutoff 2019-03-06T00:00:00 \
  --lookback-days 180; then
  echo "Uplift dataset unexpectedly accepted a missing Gold snapshot" >&2
  exit 1
fi
if docker compose exec -T namenode hdfs dfs -test -e \
  "/promo/gold/uplift_dataset/snapshot_date=2019-03-06"; then
  echo "Failed uplift dataset run published a partial snapshot" >&2
  exit 1
fi

# T-learner publishes two models atomically and retains only the latest successful run.
train_submit=(
  docker compose --profile full --profile tools run --rm trainer
  /opt/spark/bin/spark-submit
  --master spark://spark-master:7077
  --conf spark.executor.cores=2
  --conf spark.executor.memory=1g
  --conf spark.sql.shuffle.partitions=4
)
for _ in 1 2; do
  "${train_submit[@]}" /workspace/training/train_uplift.py \
    --hdfs-base-uri hdfs://namenode:9000/promo \
    --dataset-snapshot-date "${uplift_feature_snapshot}" \
    --iterations 30 \
    --depth 4 \
    --learning-rate 0.1 \
    --early-stopping-rounds 5
  "${submit[@]}" /workspace/tests/integration/verify_uplift_model.py \
    --hdfs-base-uri hdfs://namenode:9000/promo \
    --dataset-snapshot-date "${uplift_feature_snapshot}"
done
if "${train_submit[@]}" /workspace/training/train_uplift.py \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --dataset-snapshot-date 2019-03-06 \
  --iterations 10; then
  echo "Uplift training unexpectedly accepted a missing dataset" >&2
  exit 1
fi
"${submit[@]}" /workspace/tests/integration/verify_uplift_model.py \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --dataset-snapshot-date "${uplift_feature_snapshot}"

# A different dimensions snapshot must not be mixed with existing Silver purchases.
mismatch_snapshot=2026-07-03
"${submit[@]}" /workspace/spark_jobs/build_silver_dimensions.py \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --bronze-ingest-date "${ingest_date}" \
  --snapshot-date "${mismatch_snapshot}"
if "${submit[@]}" /workspace/spark_jobs/build_user_features.py \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --dimensions-snapshot-date "${mismatch_snapshot}" \
  --feature-cutoff 2019-03-02T00:00:00; then
  echo "Gold features unexpectedly mixed Silver snapshots" >&2
  exit 1
fi
if docker compose exec -T namenode hdfs dfs -test -e \
  "/promo/gold/user_features/snapshot_date=2019-03-02"; then
  echo "Failed Gold feature run published a partial snapshot" >&2
  exit 1
fi

# Missing uplift FK must fail before publishing any dimension snapshot.
missing_fk_ingest_date=2026-07-02
"${submit[@]}" /workspace/spark_jobs/ingest_bronze.py \
  --data-dir /data/raw/raw_missing_uplift \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --ingest-date "${missing_fk_ingest_date}"
if "${submit[@]}" /workspace/spark_jobs/build_silver_dimensions.py \
  --hdfs-base-uri hdfs://namenode:9000/promo \
  --bronze-ingest-date "${missing_fk_ingest_date}" \
  --snapshot-date "${missing_fk_ingest_date}"; then
  echo "Silver dimensions unexpectedly accepted a missing uplift FK" >&2
  exit 1
fi
if docker compose exec -T namenode hdfs dfs -test -e \
  "/promo/silver/clients/snapshot_date=${missing_fk_ingest_date}"; then
  echo "Failed Silver dimension run published a partial snapshot" >&2
  exit 1
fi

fsck=$(docker compose exec -T namenode hdfs fsck /promo -blocks -locations)
under_replicated=$(awk -F: '/Under replicated blocks:/ {gsub(/[^0-9]/, "", $2); print $2; exit}' <<<"${fsck}")
if [[ ${fsck} != *"Status: HEALTHY"* ]] || [[ ${under_replicated:-0} -ne 0 ]]; then
  echo "HDFS Bronze/Silver replication check failed" >&2
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
