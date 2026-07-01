"""Manual bounded export of validated PostgreSQL feedback into HDFS."""

from __future__ import annotations

from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from airflow import DAG

with DAG(
    dag_id="feedback_export_pipeline",
    description="Export one bounded feedback batch into canonical HDFS partitions",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=["feedback", "hdfs"],
) as dag:
    export_feedback = BashOperator(
        task_id="export_postgres_feedback_to_hdfs",
        bash_command=(
            "python -m services.feedback.export_feedback "
            "--database-url \"${PROMO_DATABASE_URL}\" "
            "--webhdfs-url \"${WEBHDFS_URL}\" "
            "--hdfs-user \"${HDFS_USER}\" "
            "--config /workspace/configs/feedback_export.yaml"
        ),
        do_xcom_push=False,
    )
