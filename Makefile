.PHONY: install-dev lint test validate-data data-up data-down hdfs-bootstrap \
	ingest-bronze ingest-purchases build-silver-dimensions clean-silver-purchases \
	build-silver test-hdfs test-all

PYTHON ?= python3
DATA_DIR ?= data/raw
FEATURE_CUTOFF ?= 2019-03-01T00:00:00
INGEST_DATE ?= $(shell date -u +%F)
PURCHASE_MONTHS ?=
PURCHASE_MONTH_ARGS = $(foreach month,$(PURCHASE_MONTHS),--purchase-month $(month))
BRONZE_INGEST_DATE ?= $(INGEST_DATE)
SNAPSHOT_DATE ?= $(shell date -u +%F)
DIMENSIONS_SNAPSHOT_DATE ?= $(SNAPSHOT_DATE)

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

test-hdfs:
	./scripts/smoke_test_hdfs.sh

test-all: lint test test-hdfs
