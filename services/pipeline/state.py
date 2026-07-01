"""Persist bounded pipeline state and evaluation metrics in PostgreSQL."""

from __future__ import annotations

import argparse
import hashlib
import json
import uuid
from pathlib import Path
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

FINAL_STATUSES = frozenset({"SUCCEEDED", "FAILED"})


def config_fingerprint(paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(path.name.encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def start_run(
    database_url: str,
    *,
    run_id: uuid.UUID,
    dag_id: str,
    airflow_run_id: str | None,
    feature_cutoff: str,
    fingerprint: str,
) -> None:
    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            INSERT INTO pipeline_runs(
                run_id, dag_id, airflow_run_id, status, feature_cutoff,
                config_fingerprint
            ) VALUES (%s, %s, %s, 'RUNNING', %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                airflow_run_id = EXCLUDED.airflow_run_id,
                status = 'RUNNING', error_summary = NULL, finished_at = NULL,
                updated_at = now()
            """,
            (run_id, dag_id, airflow_run_id, feature_cutoff, fingerprint),
        )


def update_run(
    database_url: str,
    *,
    run_id: uuid.UUID,
    status: str | None = None,
    current_task: str | None = None,
    snapshot_id: str | None = None,
    propensity_run_id: str | None = None,
    uplift_run_id: str | None = None,
    metrics: dict[str, Any] | None = None,
    error_summary: str | None = None,
) -> None:
    if status is not None and status not in FINAL_STATUSES | {"RUNNING"}:
        raise ValueError(f"unsupported pipeline status: {status}")
    with psycopg.connect(database_url) as connection:
        cursor = connection.execute(
            """
            UPDATE pipeline_runs SET
                status = COALESCE(%s, status),
                current_task = COALESCE(%s, current_task),
                snapshot_id = COALESCE(%s, snapshot_id),
                propensity_run_id = COALESCE(%s, propensity_run_id),
                uplift_run_id = COALESCE(%s, uplift_run_id),
                metrics = metrics || COALESCE(%s, '{}'::jsonb),
                error_summary = COALESCE(%s, error_summary),
                finished_at = CASE WHEN %s IN ('SUCCEEDED', 'FAILED') THEN now()
                                   ELSE finished_at END,
                updated_at = now()
            WHERE run_id = %s
            """,
            (
                status,
                current_task,
                snapshot_id,
                propensity_run_id,
                uplift_run_id,
                Jsonb(metrics or {}),
                error_summary,
                status,
                run_id,
            ),
        )
        if cursor.rowcount != 1:
            raise LookupError(f"pipeline run does not exist: {run_id}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    subparsers = parser.add_subparsers(dest="command", required=True)
    start = subparsers.add_parser("start")
    start.add_argument("--run-id", type=uuid.UUID, required=True)
    start.add_argument("--dag-id", required=True)
    start.add_argument("--airflow-run-id")
    start.add_argument("--feature-cutoff", required=True)
    start.add_argument("--config", action="append", type=Path, default=[])
    update = subparsers.add_parser("update")
    update.add_argument("--run-id", type=uuid.UUID, required=True)
    update.add_argument("--status", choices=["RUNNING", "SUCCEEDED", "FAILED"])
    update.add_argument("--current-task")
    update.add_argument("--snapshot-id")
    update.add_argument("--propensity-run-id")
    update.add_argument("--uplift-run-id")
    update.add_argument("--metrics-json", type=Path)
    update.add_argument("--error-summary")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "start":
        start_run(
            args.database_url,
            run_id=args.run_id,
            dag_id=args.dag_id,
            airflow_run_id=args.airflow_run_id,
            feature_cutoff=args.feature_cutoff,
            fingerprint=config_fingerprint(args.config),
        )
    else:
        metrics = (
            json.loads(args.metrics_json.read_text(encoding="utf-8"))
            if args.metrics_json
            else None
        )
        update_run(
            args.database_url,
            run_id=args.run_id,
            status=args.status,
            current_task=args.current_task,
            snapshot_id=args.snapshot_id,
            propensity_run_id=args.propensity_run_id,
            uplift_run_id=args.uplift_run_id,
            metrics=metrics,
            error_summary=args.error_summary,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
