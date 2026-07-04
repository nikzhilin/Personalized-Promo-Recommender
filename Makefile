.PHONY: install-dev lint test validate-data data-up data-down hdfs-bootstrap \
	ingest-bronze ingest-purchases build-silver-dimensions clean-silver-purchases \
	build-silver build-feedback-features build-user-features build-item-features generate-candidates \
	build-user-item-features build-gold-features build-propensity-dataset \
	build-uplift-dataset train-propensity train-uplift build-trainer test-training \
	score-models build-simulation optimize-discounts rank-recommendations db-up db-migrate online-up \
	publish-redis reload-api-cache airflow-up export-feedback trigger-feedback-export \
	evaluate-offline trigger-daily-pipeline monitoring-up monitoring-down test-e2e \
	test-hdfs test-all

PYTHON ?= python3
DATA_DIR ?= data/raw
FEATURE_CUTOFF ?= 2019-03-01T00:00:00
INGEST_DATE ?= $(shell date -u +%F)
PURCHASE_MONTHS ?=
PURCHASE_MONTH_ARGS = $(foreach month,$(PURCHASE_MONTHS),--purchase-month $(month))
BRONZE_INGEST_DATE ?= $(INGEST_DATE)
SNAPSHOT_DATE ?= $(shell date -u +%F)
DIMENSIONS_SNAPSHOT_DATE ?= $(SNAPSHOT_DATE)
LOOKBACK_DAYS ?= 180
MARGIN_CONFIG ?= /workspace/configs/margin_seed.csv
PROPENSITY_CUTOFFS ?= 2019-02-01T00:00:00 2019-03-01T00:00:00
PROPENSITY_CUTOFF_ARGS = $(foreach cutoff,$(PROPENSITY_CUTOFFS),--feature-cutoff $(cutoff))
LABEL_WINDOW_DAYS ?= 30
NEGATIVE_RATIO ?= 3
RANDOM_SEED ?= 42
DATASET_SNAPSHOT_DATE ?= 2019-03-01
MAX_TRAINING_ROWS ?= 2000000
PROPENSITY_ITERATIONS ?= 500
PROPENSITY_DEPTH ?= 7
PROPENSITY_LEARNING_RATE ?= 0.05
EARLY_STOPPING_ROUNDS ?= 50
CATBOOST_THREADS ?= 2
UPLIFT_VALIDATION_RATIO ?= 0.2
SMD_WARNING_THRESHOLD ?= 0.1
UPLIFT_CLASS_WEIGHT_THRESHOLD ?= 0.1
PROPENSITY_MODEL_RUN_ID ?=
UPLIFT_MODEL_RUN_ID ?=
SIMULATION_CONFIG ?= /workspace/configs/simulation.yaml
OPTIMIZER_CONFIG ?= /workspace/configs/optimizer.yaml
RANKING_CONFIG ?= /workspace/configs/ranking.yaml
ONLINE_CONFIG ?= /workspace/configs/online_store.yaml
PUBLISH_SNAPSHOT_DATE ?= $(word 1,$(subst T, ,$(FEATURE_CUTOFF)))
REDIS_URL ?= redis://redis:6379/0
FEEDBACK_EXPORT_CONFIG ?= /workspace/configs/feedback_export.yaml
API_URL ?= http://localhost:8000
ADMIN_API_KEY ?=

install-dev:
	$(PYTHON) -m pip install -e '.[dev]'

lint:
	$(PYTHON) -m ruff check .

test:
	$(PYTHON) -m pytest

validate-data:
	$(PYTHON) -m spark_jobs.validate_raw_data \
		--data-dir $(DATA_DIR) \
		--feature-cutoff $(FEATURE_CUTOFF)

data-up:
	RAW_DATA_DIR=$(abspath $(DATA_DIR)) docker compose --profile full up -d \
		namenode datanode-1 datanode-2 spark-master spark-worker

data-down:
	docker compose --profile full --profile tools down

hdfs-bootstrap:
	./scripts/bootstrap_hdfs.sh

ingest-bronze: hdfs-bootstrap
	./scripts/hdfs_preflight.sh
	RAW_DATA_DIR=$(abspath $(DATA_DIR)) docker compose --profile full --profile tools run --rm \
		spark-submit /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=4 \
		/workspace/spark_jobs/ingest_bronze.py \
		--data-dir /data/raw \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--ingest-date $(INGEST_DATE)

ingest-purchases: hdfs-bootstrap
	./scripts/hdfs_preflight.sh
	RAW_DATA_DIR=$(abspath $(DATA_DIR)) docker compose --profile full --profile tools run --rm \
		spark-submit /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=12 \
		/workspace/spark_jobs/ingest_purchases.py \
		--data-dir /data/raw \
		--hdfs-base-uri hdfs://namenode:9000/promo $(PURCHASE_MONTH_ARGS)

build-silver-dimensions: hdfs-bootstrap
	./scripts/hdfs_preflight.sh
	docker compose --profile full --profile tools run --rm \
		spark-submit /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=4 \
		/workspace/spark_jobs/build_silver_dimensions.py \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--bronze-ingest-date $(BRONZE_INGEST_DATE) \
		--snapshot-date $(SNAPSHOT_DATE)

clean-silver-purchases: hdfs-bootstrap
	./scripts/hdfs_preflight.sh
	docker compose --profile full --profile tools run --rm \
		spark-submit /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=12 \
		/workspace/spark_jobs/clean_silver_purchases.py \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--dimensions-snapshot-date $(DIMENSIONS_SNAPSHOT_DATE) \
		--snapshot-date $(SNAPSHOT_DATE) $(PURCHASE_MONTH_ARGS)

build-silver: build-silver-dimensions clean-silver-purchases

build-feedback-features: hdfs-bootstrap
	./scripts/hdfs_preflight.sh
	docker compose --profile full --profile tools run --rm \
		spark-submit /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=12 \
		/workspace/spark_jobs/build_feedback_features.py \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--dimensions-snapshot-date $(DIMENSIONS_SNAPSHOT_DATE) \
		--feature-cutoff $(FEATURE_CUTOFF) \
		--lookback-days $(LOOKBACK_DAYS)

build-user-features: hdfs-bootstrap
	./scripts/hdfs_preflight.sh
	docker compose --profile full --profile tools run --rm \
		spark-submit /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=12 \
		/workspace/spark_jobs/build_user_features.py \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--dimensions-snapshot-date $(DIMENSIONS_SNAPSHOT_DATE) \
		--feature-cutoff $(FEATURE_CUTOFF) \
		--lookback-days $(LOOKBACK_DAYS)

build-item-features: hdfs-bootstrap
	./scripts/hdfs_preflight.sh
	docker compose --profile full --profile tools run --rm \
		spark-submit /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=12 \
		/workspace/spark_jobs/build_item_features.py \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--dimensions-snapshot-date $(DIMENSIONS_SNAPSHOT_DATE) \
		--feature-cutoff $(FEATURE_CUTOFF) \
		--lookback-days $(LOOKBACK_DAYS) \
		--margin-config $(MARGIN_CONFIG) \
		--simulation-config $(SIMULATION_CONFIG)

generate-candidates: hdfs-bootstrap
	./scripts/hdfs_preflight.sh
	docker compose --profile full --profile tools run --rm \
		spark-submit /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=12 \
		/workspace/spark_jobs/generate_candidates.py \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--dimensions-snapshot-date $(DIMENSIONS_SNAPSHOT_DATE) \
		--feature-cutoff $(FEATURE_CUTOFF) \
		--lookback-days $(LOOKBACK_DAYS)

build-user-item-features: hdfs-bootstrap
	./scripts/hdfs_preflight.sh
	docker compose --profile full --profile tools run --rm \
		spark-submit /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=12 \
		/workspace/spark_jobs/build_user_item_features.py \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--dimensions-snapshot-date $(DIMENSIONS_SNAPSHOT_DATE) \
		--feature-cutoff $(FEATURE_CUTOFF) \
		--lookback-days $(LOOKBACK_DAYS)

build-gold-features:
	$(MAKE) build-feedback-features
	$(MAKE) build-user-features
	$(MAKE) build-item-features
	$(MAKE) generate-candidates
	$(MAKE) build-user-item-features

build-propensity-dataset: hdfs-bootstrap
	./scripts/hdfs_preflight.sh
	docker compose --profile full --profile tools run --rm \
		spark-submit /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=12 \
		/workspace/training/build_propensity_dataset.py \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--dimensions-snapshot-date $(DIMENSIONS_SNAPSHOT_DATE) \
		--lookback-days $(LOOKBACK_DAYS) \
		--label-window-days $(LABEL_WINDOW_DAYS) \
		--negative-ratio $(NEGATIVE_RATIO) \
		--random-seed $(RANDOM_SEED) $(PROPENSITY_CUTOFF_ARGS)

build-uplift-dataset: hdfs-bootstrap
	./scripts/hdfs_preflight.sh
	docker compose --profile full --profile tools run --rm \
		spark-submit /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=12 \
		/workspace/training/build_uplift_dataset.py \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--dimensions-snapshot-date $(DIMENSIONS_SNAPSHOT_DATE) \
		--feature-cutoff $(FEATURE_CUTOFF) \
		--lookback-days $(LOOKBACK_DAYS) \
		--validation-ratio $(UPLIFT_VALIDATION_RATIO) \
		--smd-warning-threshold $(SMD_WARNING_THRESHOLD) \
		--random-seed $(RANDOM_SEED)

build-trainer:
	docker compose --profile full --profile tools build trainer

train-propensity: hdfs-bootstrap build-trainer
	./scripts/hdfs_preflight.sh
	docker compose --profile full --profile tools run --rm \
		trainer /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=12 \
		/workspace/training/train_propensity.py \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--dataset-snapshot-date $(DATASET_SNAPSHOT_DATE) \
		--max-training-rows $(MAX_TRAINING_ROWS) \
		--iterations $(PROPENSITY_ITERATIONS) \
		--depth $(PROPENSITY_DEPTH) \
		--learning-rate $(PROPENSITY_LEARNING_RATE) \
		--early-stopping-rounds $(EARLY_STOPPING_ROUNDS) \
		--random-seed $(RANDOM_SEED) \
		--thread-count $(CATBOOST_THREADS)

train-uplift: hdfs-bootstrap build-trainer
	./scripts/hdfs_preflight.sh
	docker compose --profile full --profile tools run --rm \
		trainer /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=12 \
		/workspace/training/train_uplift.py \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--dataset-snapshot-date $(DATASET_SNAPSHOT_DATE) \
		--iterations $(PROPENSITY_ITERATIONS) \
		--depth $(PROPENSITY_DEPTH) \
		--learning-rate $(PROPENSITY_LEARNING_RATE) \
		--early-stopping-rounds $(EARLY_STOPPING_ROUNDS) \
		--random-seed $(RANDOM_SEED) \
		--thread-count $(CATBOOST_THREADS) \
		--class-weight-threshold $(UPLIFT_CLASS_WEIGHT_THRESHOLD)

score-models: hdfs-bootstrap build-trainer
	@test -n "$(PROPENSITY_MODEL_RUN_ID)" || \
		(echo "PROPENSITY_MODEL_RUN_ID is required" >&2; exit 2)
	@test -n "$(UPLIFT_MODEL_RUN_ID)" || \
		(echo "UPLIFT_MODEL_RUN_ID is required" >&2; exit 2)
	./scripts/hdfs_preflight.sh
	docker compose --profile full --profile tools run --rm \
		trainer /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=12 \
		--conf spark.sql.execution.arrow.maxRecordsPerBatch=10000 \
		/workspace/training/score_models.py \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--dimensions-snapshot-date $(DIMENSIONS_SNAPSHOT_DATE) \
		--feature-cutoff $(FEATURE_CUTOFF) \
		--lookback-days $(LOOKBACK_DAYS) \
		--propensity-model-run-id $(PROPENSITY_MODEL_RUN_ID) \
		--uplift-model-run-id $(UPLIFT_MODEL_RUN_ID)

build-simulation: hdfs-bootstrap build-trainer
	./scripts/hdfs_preflight.sh
	docker compose --profile full --profile tools run --rm \
		spark-submit /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=12 \
		/workspace/spark_jobs/build_simulation.py \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--dimensions-snapshot-date $(DIMENSIONS_SNAPSHOT_DATE) \
		--feature-cutoff $(FEATURE_CUTOFF) \
		--lookback-days $(LOOKBACK_DAYS) \
		--simulation-config $(SIMULATION_CONFIG)

optimize-discounts: hdfs-bootstrap build-trainer
	./scripts/hdfs_preflight.sh
	docker compose --profile full --profile tools run --rm \
		spark-submit /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=12 \
		/workspace/spark_jobs/optimize_discounts.py \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--feature-cutoff $(FEATURE_CUTOFF) \
		--optimizer-config $(OPTIMIZER_CONFIG)

rank-recommendations: hdfs-bootstrap build-trainer
	./scripts/hdfs_preflight.sh
	docker compose --profile full --profile tools run --rm \
		spark-submit /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=12 \
		/workspace/spark_jobs/rank_recommendations.py \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--feature-cutoff $(FEATURE_CUTOFF) \
		--ranking-config $(RANKING_CONFIG)

online-up:
	docker compose --profile full up -d redis postgres db-migrate api

db-up:
	docker compose --profile full up -d postgres

db-migrate: db-up
	docker compose --profile full run --rm db-migrate

publish-redis: hdfs-bootstrap build-trainer
	./scripts/hdfs_preflight.sh
	docker compose --profile full up -d redis postgres db-migrate
	docker compose --profile full --profile tools run --rm \
		publisher /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		--conf spark.sql.shuffle.partitions=12 \
		/workspace/services/publisher/publish_redis.py \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--snapshot-date $(PUBLISH_SNAPSHOT_DATE) \
		--redis-url $(REDIS_URL) \
		--online-config $(ONLINE_CONFIG)

reload-api-cache:
	curl --fail --silent --show-error -X POST \
		-H "X-Admin-API-Key: $(ADMIN_API_KEY)" \
		$(API_URL)/v1/admin/cache/reload

airflow-up:
	docker compose --profile full --profile airflow up -d \
		postgres namenode datanode-1 datanode-2
	./scripts/bootstrap_hdfs.sh
	docker compose --profile full --profile airflow up -d \
		db-migrate \
		airflow-db-bootstrap airflow-init airflow-webserver airflow-scheduler

export-feedback:
	./scripts/hdfs_preflight.sh
	docker compose --profile full --profile airflow --profile tools run --rm \
		airflow-cli /bin/bash -eu -c \
		'python -m services.feedback.export_feedback \
		--database-url "$$PROMO_DATABASE_URL" \
		--webhdfs-url "$$WEBHDFS_URL" \
		--hdfs-user "$$HDFS_USER" \
		--config $(FEEDBACK_EXPORT_CONFIG)'

trigger-feedback-export:
	docker compose --profile full --profile airflow exec airflow-scheduler \
		airflow dags trigger feedback_export_pipeline

evaluate-offline: hdfs-bootstrap build-trainer
	./scripts/hdfs_preflight.sh
	docker compose --profile full --profile tools run --rm \
		trainer /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.executor.cores=2 \
		--conf spark.executor.memory=1g \
		/workspace/training/evaluate_offline.py \
		--hdfs-base-uri hdfs://namenode:9000/promo \
		--feature-cutoff $(FEATURE_CUTOFF)

trigger-daily-pipeline:
	docker compose --profile full --profile airflow exec airflow-scheduler \
		airflow dags trigger daily_discount_pipeline

monitoring-up:
	docker compose --profile full --profile monitoring up -d \
		pipeline-metrics airflow-statsd-exporter redis-exporter prometheus grafana

monitoring-down:
	docker compose --profile full --profile monitoring stop \
		grafana prometheus redis-exporter airflow-statsd-exporter pipeline-metrics

test-training: build-trainer
	docker compose --profile full --profile tools run --rm --no-deps \
		trainer python3 /workspace/tests/training/smoke_train_propensity.py
	docker compose --profile full --profile tools run --rm --no-deps \
		trainer python3 /workspace/tests/training/smoke_train_uplift.py
	docker compose --profile full --profile tools run --rm --no-deps \
		trainer python3 /workspace/tests/training/smoke_score_models.py

test-hdfs:
	./scripts/smoke_test_hdfs.sh

test-all: lint test test-training test-hdfs

test-e2e: test-all
	docker compose --profile full config --quiet
