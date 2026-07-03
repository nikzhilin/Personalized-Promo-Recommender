import json
from pathlib import Path

import yaml


def test_prometheus_scrapes_required_components_and_loads_alerts() -> None:
    config = yaml.safe_load(
        Path("monitoring/prometheus/prometheus.yml").read_text(encoding="utf-8")
    )
    jobs = {item["job_name"] for item in config["scrape_configs"]}
    assert {"api", "pipeline", "airflow", "hdfs-namenode", "hdfs-datanodes"} <= jobs
    assert config["rule_files"] == ["/etc/prometheus/alerts.yml"]


def test_grafana_dashboard_is_provisioned_json() -> None:
    dashboard = json.loads(
        Path("monitoring/grafana/dashboards/mvp-overview.json").read_text(encoding="utf-8")
    )
    assert dashboard["uid"] == "promo-mvp-overview"
    assert len(dashboard["panels"]) >= 6
