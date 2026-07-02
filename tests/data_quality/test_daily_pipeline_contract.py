import ast
from pathlib import Path


def test_daily_pipeline_is_manual_serial_and_has_required_gates() -> None:
    source = Path("airflow/dags/daily_discount_pipeline.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    assert 'dag_id="daily_discount_pipeline"' in source
    assert "schedule=None" in source
    assert "max_active_runs=1" in source
    assert 'pool="heavy_compute"' in source
    assert 'task_id="hdfs_gate"' in source
    assert 'task_id="replication_gate"' in source
    assert '"publish_redis"' in source
    assert "DockerOperator" not in source
    assert tree is not None
