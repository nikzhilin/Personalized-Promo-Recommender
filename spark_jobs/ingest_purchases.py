"""Ingest purchases.csv into typed, monthly HDFS Bronze partitions."""

from __future__ import annotations

import argparse
import json
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from spark_jobs.bronze_common import (
    PreparedCsv,
    collect_csv_profiles,
    filesystem,
    hadoop_path,
    normalize_hdfs_base_uri,
    prepare_typed_csv,
    replace_hdfs_paths,
    require_valid_profiles,
)
from spark_jobs.time_compat import UTC

PURCHASES_FILE = "purchases.csv"
PURCHASE_MONTH_PATTERN = re.compile(r"^[0-9]{4}-(0[1-9]|1[0-2])$")
MAX_RECORDS_PER_FILE = 1_000_000


def parse_purchase_month(value: str) -> str:
    if not PURCHASE_MONTH_PATTERN.fullmatch(value):
        raise argparse.ArgumentTypeError("purchase month must use YYYY-MM")
    return value


def purchase_partition_uri(base_uri: str, purchase_month: str) -> str:
    normalized_base = normalize_hdfs_base_uri(base_uri)
    month = parse_purchase_month(purchase_month)
    return f"{normalized_base}/bronze/purchases/purchase_month={month}"


def select_purchase_months(
    requested_months: list[str] | None, discovered_months: list[str]
) -> list[str]:
    if not requested_months:
        return discovered_months

    selected = list(dict.fromkeys(requested_months))
    missing = sorted(set(selected) - set(discovered_months))
    if missing:
        raise ValueError(f"purchases.csv has no rows for months: {', '.join(missing)}")
    return selected


def _profile_purchases(prepared: PreparedCsv) -> tuple[PreparedCsv, dict[str, int]]:
    from pyspark.sql import functions as functions

    monthly_frame = prepared.frame.withColumn(
        "purchase_month", functions.date_format("transaction_datetime", "yyyy-MM")
    )
    monthly_prepared = PreparedCsv(monthly_frame, prepared.invalid_columns)
    profiles = collect_csv_profiles(monthly_prepared, group_column="purchase_month")
    require_valid_profiles(profiles, PURCHASES_FILE)

    month_counts = {
        profile.group: profile.rows for profile in profiles if profile.group is not None
    }
    if not month_counts:
        raise ValueError("purchases.csv contains no valid purchase months")
    return monthly_prepared, dict(sorted(month_counts.items()))


def ingest_purchases(
    *,
    data_dir: Path,
    hdfs_base_uri: str,
    purchase_months: list[str] | None = None,
) -> dict[str, Any]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as functions

    requested_months = (
        [parse_purchase_month(month) for month in purchase_months]
        if purchase_months
        else None
    )
    normalized_base = normalize_hdfs_base_uri(hdfs_base_uri)
    run_id = uuid.uuid4().hex
    staging_root = f"{normalized_base}/tmp/bronze-purchases/{run_id}"
    staged_dataset_root = f"{staging_root}/purchases"
    backup_root = f"{normalized_base}/tmp/bronze-purchases-backup/{run_id}"
    ingest_timestamp = datetime.now(UTC).replace(tzinfo=None)
    spark = (
        SparkSession.builder.appName(f"ingest-purchases-{run_id}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    hdfs = filesystem(spark, staging_root)

    try:
        prepared = prepare_typed_csv(
            spark,
            data_dir / PURCHASES_FILE,
            PURCHASES_FILE,
            ingest_timestamp,
        )
        monthly_prepared, month_counts = _profile_purchases(prepared)
        selected_months = select_purchase_months(requested_months, list(month_counts))

        selected_frame = monthly_prepared.data_frame().where(
            functions.col("purchase_month").isin(selected_months)
        )
        (
            selected_frame.write.mode("errorifexists")
            .option("compression", "snappy")
            .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
            .partitionBy("purchase_month")
            .parquet(staged_dataset_root)
        )

        staged_targets = [
            (
                f"{staged_dataset_root}/purchase_month={month}",
                purchase_partition_uri(hdfs_base_uri, month),
            )
            for month in selected_months
        ]
        missing_staged = [
            month
            for month, (staged_uri, _) in zip(
                selected_months, staged_targets, strict=True
            )
            if not hdfs.exists(hadoop_path(spark, staged_uri))
        ]
        if missing_staged:
            raise RuntimeError(
                f"Spark did not stage requested months: {', '.join(missing_staged)}"
            )

        replace_hdfs_paths(spark, staged_targets, backup_root)
        return {
            "run_id": run_id,
            "source_file": PURCHASES_FILE,
            "months": {
                month: {
                    "rows": month_counts[month],
                    "target": purchase_partition_uri(hdfs_base_uri, month),
                }
                for month in selected_months
            },
        }
    finally:
        hdfs.delete(hadoop_path(spark, staging_root), True)
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("/data/raw"))
    parser.add_argument(
        "--hdfs-base-uri", default="hdfs://namenode:9000/promo", help="HDFS /promo root"
    )
    parser.add_argument(
        "--purchase-month",
        dest="purchase_months",
        action="append",
        type=parse_purchase_month,
        help="Month to publish; repeat the option or omit it to publish all discovered months",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = ingest_purchases(
        data_dir=args.data_dir,
        hdfs_base_uri=args.hdfs_base_uri,
        purchase_months=args.purchase_months,
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
