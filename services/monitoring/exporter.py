"""Expose the latest pipeline and business metrics stored in PostgreSQL."""

from __future__ import annotations

import argparse
import time
from typing import Any

import psycopg
from prometheus_client import REGISTRY, start_http_server
from prometheus_client.core import GaugeMetricFamily


def nested_number(payload: dict[str, Any], *path: str) -> float:
    value: Any = payload
    for part in path:
        if not isinstance(value, dict):
            return 0.0
        value = value.get(part)
    return float(value) if isinstance(value, int | float) else 0.0


class PipelineCollector:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url

    def collect(self):  # type: ignore[no-untyped-def]
        with psycopg.connect(self.database_url) as connection:
            row = connection.execute(
                """
                SELECT status, extract(epoch FROM started_at),
                       extract(epoch FROM finished_at), metrics
                FROM pipeline_runs ORDER BY started_at DESC LIMIT 1
                """
            ).fetchone()
        if row is None:
            return
        status, started, finished, metrics = row
        success = GaugeMetricFamily(
            "promo_pipeline_last_run_success", "Whether the latest pipeline succeeded"
        )
        success.add_metric([], 1.0 if status == "SUCCEEDED" else 0.0)
        yield success
        last_success = GaugeMetricFamily(
            "promo_pipeline_last_success_timestamp_seconds",
            "Completion timestamp of the latest successful pipeline",
        )
        last_success.add_metric([], float(finished or 0) if status == "SUCCEEDED" else 0.0)
        yield last_success
        duration = GaugeMetricFamily(
            "promo_pipeline_run_duration_seconds", "Latest pipeline duration"
        )
        duration.add_metric([], max(0.0, float(finished or time.time()) - float(started)))
        yield duration
        paths = {
            "promo_expected_profit_total": ("evaluation", "business", "expected_profit_total"),
            "promo_incremental_profit_total": (
                "evaluation",
                "business",
                "incremental_profit_total",
            ),
            "promo_discount_spend_total": ("evaluation", "business", "discount_spend"),
            "promo_business_roi": ("evaluation", "business", "promo_roi"),
            "promo_selected_discount_share": (
                "evaluation",
                "business",
                "selected_discount_share",
            ),
            "promo_recommendation_coverage": ("evaluation", "recsys", "coverage"),
        }
        for name, path in paths.items():
            metric = GaugeMetricFamily(name, f"Latest pipeline metric {'.'.join(path)}")
            metric.add_metric([], nested_number(metrics or {}, *path))
            yield metric


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--port", type=int, default=9101)
    args = parser.parse_args(argv)
    REGISTRY.register(PipelineCollector(args.database_url))
    start_http_server(args.port)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    raise SystemExit(main())
