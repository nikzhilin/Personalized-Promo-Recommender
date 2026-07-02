"""Manual, single-run end-to-end recommendation pipeline."""

from __future__ import annotations

import os
import uuid
from datetime import timedelta
from pathlib import Path

from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from airflow import DAG
from services.pipeline.state import config_fingerprint, start_run, update_run

SPARK = (
    "/opt/spark/bin/spark-submit --master spark://spark-master:7077 "
    "--conf spark.executor.cores=2 --conf spark.executor.memory=1g "
    "--conf spark.sql.shuffle.partitions=12"
)
BASE = "--hdfs-base-uri hdfs://namenode:9000/promo"
DATES = (
    "--dimensions-snapshot-date {{ params.dimensions_snapshot_date }} "
    "--feature-cutoff {{ params.feature_cutoff }} --lookback-days {{ params.lookback_days }}"
)
CONFIGS = [
    Path("/workspace/configs/simulation.yaml"),
    Path("/workspace/configs/optimizer.yaml"),
    Path("/workspace/configs/ranking.yaml"),
    Path("/workspace/configs/online_store.yaml"),
]


def pipeline_uuid(airflow_run_id: str) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"daily_discount_pipeline:{airflow_run_id}")


def record_start(**context: object) -> None:
    params = context["params"]
    dag_run = context["dag_run"]
    start_run(
        os.environ["PROMO_DATABASE_URL"],
        run_id=pipeline_uuid(dag_run.run_id),
        dag_id="daily_discount_pipeline",
        airflow_run_id=dag_run.run_id,
        feature_cutoff=params["feature_cutoff"],
        fingerprint=config_fingerprint(CONFIGS),
    )


def record_success(**context: object) -> None:
    dag_run = context["dag_run"]
    update_run(
        os.environ["PROMO_DATABASE_URL"],
        run_id=pipeline_uuid(dag_run.run_id),
        status="SUCCEEDED",
        snapshot_id=context["params"]["feature_cutoff"][:10],
    )


def record_failure(context: dict[str, object]) -> None:
    dag_run = context.get("dag_run")
    if dag_run is None:
        return
    update_run(
        os.environ["PROMO_DATABASE_URL"],
        run_id=pipeline_uuid(dag_run.run_id),
        status="FAILED",
        current_task=context["task_instance"].task_id,
        error_summary=str(context.get("exception", "Airflow task failed"))[:2000],
    )


def spark_task(
    task_id: str, module: str, arguments: str = "", *, push: bool = False
) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        pool="heavy_compute",
        bash_command=f"{SPARK} /workspace/{module} {BASE} {arguments}",
        do_xcom_push=push,
    )


with DAG(
    dag_id="daily_discount_pipeline",
    description="Build, evaluate and atomically publish a recommendation snapshot",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
    on_failure_callback=record_failure,
    params={
        "ingest_date": Param("2026-07-01", type="string", format="date"),
        "dimensions_snapshot_date": Param("2026-07-01", type="string", format="date"),
        "feature_cutoff": Param("2019-03-01T00:00:00", type="string"),
        "propensity_cutoffs": Param(
            ["2019-02-01T00:00:00", "2019-03-01T00:00:00"],
            type="array",
            minItems=2,
        ),
        "lookback_days": Param(180, type="integer", minimum=1),
        "label_window_days": Param(30, type="integer", minimum=1),
        "random_seed": Param(42, type="integer"),
    },
    tags=["mvp", "batch", "recommendations"],
) as dag:
    audit_start = PythonOperator(task_id="audit_start", python_callable=record_start)
    hdfs_gate = BashOperator(
        task_id="hdfs_gate",
        bash_command="/workspace/scripts/hdfs_preflight.sh --require-replication",
        do_xcom_push=False,
    )
    validate = BashOperator(
        task_id="validate_raw_files",
        bash_command=(
            "python -m spark_jobs.validate_raw_data --data-dir /data/raw "
            "--feature-cutoff {{ params.feature_cutoff }}"
        ),
        do_xcom_push=False,
    )
    ingest_dimensions = spark_task(
        "ingest_bronze_dimensions",
        "spark_jobs/ingest_bronze.py",
        "--data-dir /data/raw --ingest-date {{ params.ingest_date }}",
    )
    ingest_purchases = spark_task(
        "ingest_bronze_purchases", "spark_jobs/ingest_purchases.py", "--data-dir /data/raw"
    )
    silver_dimensions = spark_task(
        "build_silver_dimensions",
        "spark_jobs/build_silver_dimensions.py",
        "--bronze-ingest-date {{ params.ingest_date }} "
        "--snapshot-date {{ params.dimensions_snapshot_date }}",
    )
    silver_purchases = spark_task(
        "clean_silver_purchases",
        "spark_jobs/clean_silver_purchases.py",
        "--dimensions-snapshot-date {{ params.dimensions_snapshot_date }} "
        "--snapshot-date {{ params.dimensions_snapshot_date }}",
    )
    feedback = spark_task(
        "build_feedback_features", "spark_jobs/build_feedback_features.py", DATES
    )
    users = spark_task("build_user_features", "spark_jobs/build_user_features.py", DATES)
    items = spark_task(
        "build_item_features",
        "spark_jobs/build_item_features.py",
        f"{DATES} --margin-config /workspace/configs/margin_seed.csv "
        "--simulation-config /workspace/configs/simulation.yaml",
    )
    candidates = spark_task(
        "generate_candidates", "spark_jobs/generate_candidates.py", DATES
    )
    user_items = spark_task(
        "build_user_item_features", "spark_jobs/build_user_item_features.py", DATES
    )
    propensity_dataset = spark_task(
        "build_propensity_dataset",
        "training/build_propensity_dataset.py",
        "--dimensions-snapshot-date {{ params.dimensions_snapshot_date }} "
        "--lookback-days {{ params.lookback_days }} "
        "--label-window-days {{ params.label_window_days }} --negative-ratio 3 "
        "--random-seed {{ params.random_seed }} "
        "{% for cutoff in params.propensity_cutoffs %}--feature-cutoff {{ cutoff }} {% endfor %}",
    )
    uplift_dataset = spark_task(
        "build_uplift_dataset", "training/build_uplift_dataset.py", DATES
    )
    train_propensity = spark_task(
        "train_propensity",
        "training/train_propensity.py",
        "--dataset-snapshot-date {{ params.feature_cutoff[:10] }} "
        "--random-seed {{ params.random_seed }}; "
        "hdfs dfs -ls /promo/models/propensity | sed -n 's/.*run_id=//p' | tail -1",
        push=True,
    )
    train_uplift = spark_task(
        "train_uplift",
        "training/train_uplift.py",
        "--dataset-snapshot-date {{ params.feature_cutoff[:10] }} "
        "--random-seed {{ params.random_seed }}; "
        "hdfs dfs -ls /promo/models/uplift | sed -n 's/.*run_id=//p' | tail -1",
        push=True,
    )
    score = spark_task(
        "score_models",
        "training/score_models.py",
        f"{DATES} --propensity-model-run-id "
        "{{ ti.xcom_pull(task_ids='train_propensity') }} --uplift-model-run-id "
        "{{ ti.xcom_pull(task_ids='train_uplift') }}",
    )
    simulation = spark_task(
        "build_simulation",
        "spark_jobs/build_simulation.py",
        f"{DATES} --simulation-config /workspace/configs/simulation.yaml",
    )
    optimize = spark_task(
        "optimize_discounts",
        "spark_jobs/optimize_discounts.py",
        "--feature-cutoff {{ params.feature_cutoff }} "
        "--optimizer-config /workspace/configs/optimizer.yaml",
    )
    rank = spark_task(
        "rank_recommendations",
        "spark_jobs/rank_recommendations.py",
        "--feature-cutoff {{ params.feature_cutoff }} "
        "--ranking-config /workspace/configs/ranking.yaml",
    )
    evaluate = spark_task(
        "evaluate_offline",
        "training/evaluate_offline.py",
        "--feature-cutoff {{ params.feature_cutoff }}",
    )
    replication_gate = BashOperator(
        task_id="replication_gate",
        bash_command="/workspace/scripts/hdfs_preflight.sh --require-replication",
        do_xcom_push=False,
    )
    publish = spark_task(
        "publish_redis",
        "services/publisher/publish_redis.py",
        "--snapshot-date {{ params.feature_cutoff[:10] }} --redis-url ${REDIS_URL} "
        "--online-config /workspace/configs/online_store.yaml",
    )
    maintenance = BashOperator(
        task_id="snapshot_retention_and_namenode_backup",
        bash_command="/workspace/scripts/hdfs_maintenance.sh",
        do_xcom_push=False,
    )
    audit_success = PythonOperator(task_id="audit_success", python_callable=record_success)

    audit_start >> hdfs_gate >> validate >> [ingest_dimensions, ingest_purchases]
    [ingest_dimensions, ingest_purchases] >> silver_dimensions >> silver_purchases
    silver_purchases >> feedback >> users >> items >> candidates >> user_items
    user_items >> [propensity_dataset, uplift_dataset]
    propensity_dataset >> train_propensity
    uplift_dataset >> train_uplift
    [train_propensity, train_uplift] >> score
    score >> simulation >> optimize >> rank >> evaluate >> replication_gate >> publish
    publish >> maintenance >> audit_success
