from __future__ import annotations

import ast
from pathlib import Path


def test_feedback_export_dag_is_manual_bounded_and_does_not_push_xcom() -> None:
    source = Path("airflow/dags/feedback_export_pipeline.py").read_text(encoding="utf-8")
    ast.parse(source)
    assert 'dag_id="feedback_export_pipeline"' in source
    assert "schedule=None" in source
    assert "max_active_runs=1" in source
    assert 'task_id="export_postgres_feedback_to_hdfs"' in source
    assert "do_xcom_push=False" in source
