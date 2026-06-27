"""Clean monthly Bronze purchases into validated Silver data and reject datasets."""

from __future__ import annotations

import argparse
import json
import uuid
from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any

from spark_jobs.bronze_common import (
    filesystem,
    hadoop_path,
    normalize_hdfs_base_uri,
    replace_hdfs_paths,
)
from spark_jobs.ingest_purchases import parse_purchase_month, select_purchase_months
from spark_jobs.silver_common import (
    add_lineage,
    add_reject_columns,
    parse_iso_date,
    silver_data_uri,
    silver_metadata_uri,
    silver_reject_uri,
    stage_parquet,
    write_json_metadata,
)
from spark_jobs.time_compat import UTC

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession


PURCHASE_SOURCE_COLUMNS = (
    "client_id",
    "transaction_id",
    "transaction_datetime",
    "regular_points_received",
    "express_points_received",
    "regular_points_spent",
    "express_points_spent",
    "purchase_sum",
    "store_id",
    "product_id",
    "product_quantity",
    "trn_sum_from_iss",
    "trn_sum_from_red",
)
REASON_CODES = (
    "NONPOSITIVE_QUANTITY",
    "UNKNOWN_CLIENT",
    "UNKNOWN_PRODUCT",
    "FUTURE_TRANSACTION",
)


def _reason_array(conditions: list[tuple[str, Column]]) -> Column:
    from pyspark.sql import functions as functions

    arrays = [
        functions.when(condition, functions.array(functions.lit(code))).otherwise(
            functions.array().cast("array<string>")
        )
        for code, condition in conditions
    ]
    return functions.concat(*arrays)


def _read_bronze_purchases(
    spark: SparkSession, base_uri: str, requested_months: list[str] | None
) -> tuple[DataFrame, list[str]]:
    root = f"{normalize_hdfs_base_uri(base_uri)}/bronze/purchases"
    hdfs = filesystem(spark, root)
    statuses = hdfs.globStatus(hadoop_path(spark, f"{root}/purchase_month=*")) or []
    discovered = sorted(
        status.getPath().getName().split("=", maxsplit=1)[1] for status in statuses
    )
    selected = select_purchase_months(requested_months, discovered)
    paths = [f"{root}/purchase_month={month}" for month in selected]
    frame = spark.read.option("basePath", root).parquet(*paths)
    return frame, selected


def _annotate_purchases(
    frame: DataFrame,
    clients: DataFrame,
    products: DataFrame,
    snapshot_date: date,
) -> DataFrame:
    from pyspark.sql import functions as functions

    client_keys = clients.select("client_id").withColumn("_known_client", functions.lit(True))
    product_keys = products.select("product_id").withColumn("_known_product", functions.lit(True))
    joined = frame.join(client_keys, "client_id", "left").join(product_keys, "product_id", "left")
    next_day = datetime.combine(snapshot_date + timedelta(days=1), time.min)
    conditions = [
        ("NONPOSITIVE_QUANTITY", functions.col("product_quantity") <= 0),
        ("UNKNOWN_CLIENT", functions.col("_known_client").isNull()),
        ("UNKNOWN_PRODUCT", functions.col("_known_product").isNull()),
        ("FUTURE_TRANSACTION", functions.col("transaction_datetime") >= functions.lit(next_day)),
    ]
    return (
        joined.withColumn("reason_codes", _reason_array(conditions))
        .withColumn(
            "is_valid_for_price",
            functions.col("trn_sum_from_iss").isNotNull()
            & (functions.col("trn_sum_from_iss") >= 0),
        )
        .drop("_known_client", "_known_product")
    )


def _monthly_metrics(annotated: DataFrame, selected_months: list[str]) -> dict[str, dict[str, Any]]:
    from pyspark.sql import functions as functions

    summary_rows = (
        annotated.groupBy("purchase_month")
        .agg(
            functions.count("*").alias("input_rows"),
            functions.sum(
                functions.when(functions.size("reason_codes") == 0, 1).otherwise(0)
            ).alias("output_rows"),
            functions.sum(
                functions.when(functions.size("reason_codes") > 0, 1).otherwise(0)
            ).alias("reject_rows"),
            functions.sum(
                functions.when(~functions.col("is_valid_for_price"), 1).otherwise(0)
            ).alias("invalid_for_price_rows"),
            functions.sum(
                functions.when(functions.col("trn_sum_from_red") < 0, 1).otherwise(0)
            ).alias("negative_redemption_sum_rows"),
        )
        .collect()
    )
    reason_rows = (
        annotated.select("purchase_month", functions.explode_outer("reason_codes").alias("reason"))
        .where(functions.col("reason").isNotNull())
        .groupBy("purchase_month", "reason")
        .count()
        .collect()
    )
    reason_counts: dict[str, dict[str, int]] = {month: {} for month in selected_months}
    for row in reason_rows:
        reason_counts[str(row["purchase_month"])][str(row["reason"])] = int(row["count"])

    exact_unique_rows = {
        str(row["purchase_month"]): int(row["unique_rows"])
        for row in annotated.select("purchase_month", *PURCHASE_SOURCE_COLUMNS)
        .dropDuplicates(["purchase_month", *PURCHASE_SOURCE_COLUMNS])
        .groupBy("purchase_month")
        .agg(functions.count("*").alias("unique_rows"))
        .collect()
    }
    metrics: dict[str, dict[str, Any]] = {}
    for row in summary_rows:
        month = str(row["purchase_month"])
        input_rows = int(row["input_rows"])
        metrics[month] = {
            "input_rows": input_rows,
            "output_rows": int(row["output_rows"] or 0),
            "reject_rows": int(row["reject_rows"] or 0),
            "invalid_for_price_rows": int(row["invalid_for_price_rows"] or 0),
            "negative_redemption_sum_rows": int(row["negative_redemption_sum_rows"] or 0),
            "exact_duplicate_rows": input_rows - exact_unique_rows.get(month, input_rows),
            "reason_counts": reason_counts[month],
        }
    return metrics


def _ensure_partition_path(
    spark: SparkSession, frame: DataFrame, root_uri: str, month: str
) -> str:
    uri = f"{root_uri}/purchase_month={month}"
    hdfs = filesystem(spark, uri)
    path = hadoop_path(spark, uri)
    if not hdfs.exists(path):
        stage_parquet(frame.limit(0).drop("purchase_month"), uri)
    return uri


def clean_silver_purchases(
    *,
    hdfs_base_uri: str,
    dimensions_snapshot_date: date,
    snapshot_date: date,
    purchase_months: list[str] | None = None,
) -> dict[str, Any]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as functions

    requested = (
        [parse_purchase_month(month) for month in purchase_months]
        if purchase_months
        else None
    )
    normalized_base = normalize_hdfs_base_uri(hdfs_base_uri)
    run_id = uuid.uuid4().hex
    silver_timestamp = datetime.now(UTC).replace(tzinfo=None)
    staging_root = f"{normalized_base}/tmp/silver-purchases/{run_id}"
    backup_root = f"{normalized_base}/tmp/silver-purchases-backup/{run_id}"
    spark = SparkSession.builder.appName(f"silver-purchases-{run_id}").config(
        "spark.sql.session.timeZone", "UTC"
    ).getOrCreate()
    hdfs = filesystem(spark, staging_root)

    try:
        snapshot_partition = f"snapshot_date={dimensions_snapshot_date.isoformat()}"
        clients = spark.read.parquet(silver_data_uri(hdfs_base_uri, "clients", snapshot_partition))
        products = spark.read.parquet(
            silver_data_uri(hdfs_base_uri, "products", snapshot_partition)
        )
        bronze, selected_months = _read_bronze_purchases(spark, hdfs_base_uri, requested)
        annotated = _annotate_purchases(bronze, clients, products, snapshot_date)
        metrics = _monthly_metrics(annotated, selected_months)

        valid = add_lineage(
            annotated.where(functions.size("reason_codes") == 0).drop("reason_codes"),
            run_id=run_id,
            silver_timestamp=silver_timestamp,
            source_dimensions_snapshot_date=dimensions_snapshot_date.isoformat(),
        )
        rejected = add_reject_columns(
            annotated.where(functions.size("reason_codes") > 0).drop("is_valid_for_price"),
            reason_codes=functions.col("reason_codes"),
            run_id=run_id,
            silver_timestamp=silver_timestamp,
        )

        staged_data_root = f"{staging_root}/data"
        staged_reject_root = f"{staging_root}/rejects"
        (
            valid.write.mode("errorifexists")
            .option("compression", "snappy")
            .partitionBy("purchase_month")
            .parquet(staged_data_root)
        )
        (
            rejected.write.mode("errorifexists")
            .option("compression", "snappy")
            .partitionBy("purchase_month")
            .parquet(staged_reject_root)
        )

        staged_targets: list[tuple[str, str]] = []
        metadata_snapshot = f"snapshot_date={snapshot_date.isoformat()}"
        for month in selected_months:
            staged_data = _ensure_partition_path(spark, valid, staged_data_root, month)
            staged_reject = _ensure_partition_path(spark, rejected, staged_reject_root, month)
            month_partition = f"purchase_month={month}"
            target_data = silver_data_uri(hdfs_base_uri, "purchases", month_partition)
            target_reject = silver_reject_uri(
                hdfs_base_uri, "purchases", f"{metadata_snapshot}/{month_partition}"
            )
            staged_metadata = f"{staging_root}/metadata/{month_partition}"
            metadata = {
                "job": "silver_purchases",
                "run_id": run_id,
                "snapshot_date": snapshot_date.isoformat(),
                "dimensions_snapshot_date": dimensions_snapshot_date.isoformat(),
                "purchase_month": month,
                "created_at": silver_timestamp.isoformat(),
                "metrics": metrics[month],
            }
            write_json_metadata(spark, staged_metadata, metadata)
            target_metadata = silver_metadata_uri(
                hdfs_base_uri, "purchases", f"{metadata_snapshot}/{month_partition}"
            )
            staged_targets.extend(
                [
                    (staged_data, target_data),
                    (staged_reject, target_reject),
                    (staged_metadata, target_metadata),
                ]
            )

        replace_hdfs_paths(spark, staged_targets, backup_root)
        return {
            "job": "silver_purchases",
            "run_id": run_id,
            "snapshot_date": snapshot_date.isoformat(),
            "dimensions_snapshot_date": dimensions_snapshot_date.isoformat(),
            "months": metrics,
        }
    finally:
        hdfs.delete(hadoop_path(spark, staging_root), True)
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    parser.add_argument("--dimensions-snapshot-date", required=True, type=parse_iso_date)
    parser.add_argument("--snapshot-date", required=True, type=parse_iso_date)
    parser.add_argument(
        "--purchase-month",
        dest="purchase_months",
        action="append",
        type=parse_purchase_month,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = clean_silver_purchases(
        hdfs_base_uri=args.hdfs_base_uri,
        dimensions_snapshot_date=args.dimensions_snapshot_date,
        snapshot_date=args.snapshot_date,
        purchase_months=args.purchase_months,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
