"""Shared cutoff, lineage, and path helpers for Gold Spark jobs."""

from __future__ import annotations

import csv
from calendar import monthrange
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from spark_jobs.bronze_common import filesystem, hadoop_path, normalize_hdfs_base_uri

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

FEEDBACK_FEATURE_COLUMNS = (
    "feedback_clicks_30d",
    "feedback_clicks_90d",
    "feedback_carts_30d",
    "feedback_carts_90d",
    "feedback_purchases_30d",
    "feedback_purchases_90d",
    "feedback_discounted_purchase_share_180d",
    "feedback_organic_purchase_share_180d",
    "feedback_avg_shown_discount_180d",
    "feedback_avg_purchase_value_180d",
)


def gold_data_uri(base_uri: str, entity: str, snapshot_date: date) -> str:
    base = normalize_hdfs_base_uri(base_uri)
    return f"{base}/gold/{entity}/snapshot_date={snapshot_date.isoformat()}"


def gold_metadata_uri(base_uri: str, entity: str, snapshot_date: date) -> str:
    base = normalize_hdfs_base_uri(base_uri)
    return f"{base}/gold/metadata/{entity}/snapshot_date={snapshot_date.isoformat()}"


def overlapping_purchase_months(
    feature_cutoff: datetime, lookback_days: int, available_months: list[str]
) -> list[str]:
    interval_start = feature_cutoff - timedelta(days=lookback_days)
    selected: list[str] = []
    for month in sorted(available_months):
        month_start = datetime.strptime(f"{month}-01", "%Y-%m-%d")
        month_end = month_start.replace(
            day=monthrange(month_start.year, month_start.month)[1]
        ) + timedelta(days=1)
        if month_start < feature_cutoff and month_end > interval_start:
            selected.append(month)
    return selected


def read_eligible_purchases(
    spark: SparkSession,
    base_uri: str,
    feature_cutoff: datetime,
    lookback_days: int,
    dimensions_snapshot_date: date,
) -> tuple[DataFrame, list[str], int]:
    from pyspark.sql import functions as functions

    root = f"{normalize_hdfs_base_uri(base_uri)}/silver/purchases"
    hdfs = filesystem(spark, root)
    statuses = hdfs.globStatus(hadoop_path(spark, f"{root}/purchase_month=*")) or []
    available = [status.getPath().getName().split("=", maxsplit=1)[1] for status in statuses]
    selected = overlapping_purchase_months(feature_cutoff, lookback_days, available)
    if not selected:
        raise ValueError("no Silver purchase partitions overlap the feature interval")
    purchases = spark.read.option("basePath", root).parquet(
        *[f"{root}/purchase_month={month}" for month in selected]
    )
    snapshots = {
        row["source_dimensions_snapshot_date"]
        for row in purchases.select("source_dimensions_snapshot_date").distinct().collect()
    }
    expected = dimensions_snapshot_date.isoformat()
    if snapshots != {expected}:
        rendered = sorted("NULL" if value is None else str(value) for value in snapshots)
        raise ValueError(
            f"Silver purchases dimensions snapshot mismatch: expected {expected}, found {rendered}"
        )
    source_rows = purchases.count()
    start = feature_cutoff - timedelta(days=lookback_days)
    eligible = purchases.where(
        (functions.col("transaction_datetime") >= functions.lit(start))
        & (functions.col("transaction_datetime") < functions.lit(feature_cutoff))
    )
    return eligible, selected, source_rows


def source_run_ids(frame: DataFrame, column: str = "silver_run_id") -> list[str]:
    return sorted(
        str(row[column])
        for row in frame.select(column).distinct().collect()
        if row[column] is not None
    )


def require_gold_contract(
    frame: DataFrame,
    *,
    feature_cutoff: datetime,
    lookback_days: int,
    dimensions_snapshot_date: date,
    entity: str,
) -> None:
    expected = {
        "feature_cutoff": feature_cutoff,
        "lookback_days": lookback_days,
        "source_dimensions_snapshot_date": dimensions_snapshot_date.isoformat(),
    }
    for column, value in expected.items():
        actual = {row[column] for row in frame.select(column).distinct().collect()}
        if actual != {value}:
            raise ValueError(f"{entity} {column} mismatch: expected {value}, found {actual}")


def read_margin_seed(path: str) -> tuple[float, dict[str, float]]:
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"margin config does not exist: {source}")
    with source.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != ["level_2", "margin_rate"]:
            raise ValueError("margin config columns must be: level_2,margin_rate")
        values: dict[str, float] = {}
        for line_number, row in enumerate(reader, start=2):
            category = (row["level_2"] or "").strip()
            if not category or category in values:
                raise ValueError(f"invalid or duplicate level_2 at line {line_number}")
            try:
                rate = float(row["margin_rate"])
            except (TypeError, ValueError) as error:
                raise ValueError(f"invalid margin_rate at line {line_number}") from error
            if not 0 < rate <= 1:
                raise ValueError(f"margin_rate must be in (0, 1] at line {line_number}")
            values[category] = rate
    if "__DEFAULT__" not in values:
        raise ValueError("margin config must contain __DEFAULT__")
    default = values.pop("__DEFAULT__")
    return default, values
