"""Build cutoff-safe Gold user features from validated Silver datasets."""

from __future__ import annotations

import argparse
import json
import uuid
from calendar import monthrange
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from spark_jobs.bronze_common import (
    filesystem,
    hadoop_path,
    normalize_hdfs_base_uri,
    replace_hdfs_paths,
)
from spark_jobs.gold_common import FEEDBACK_FEATURE_COLUMNS, gold_data_uri, gold_metadata_uri
from spark_jobs.silver_common import (
    parse_iso_date,
    silver_data_uri,
    stage_parquet,
    write_json_metadata,
)
from spark_jobs.time_compat import UTC

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def parse_feature_cutoff(value: str) -> datetime:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "feature cutoff must use timezone-free YYYY-MM-DDTHH:MM:SS"
        ) from error
    if parsed.tzinfo is not None:
        raise argparse.ArgumentTypeError("feature cutoff must be timezone-free UTC")
    return parsed


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be a positive integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def gold_user_features_uri(base_uri: str, snapshot_date: date) -> str:
    base = normalize_hdfs_base_uri(base_uri)
    return f"{base}/gold/user_features/snapshot_date={snapshot_date.isoformat()}"


def gold_user_metadata_uri(base_uri: str, snapshot_date: date) -> str:
    base = normalize_hdfs_base_uri(base_uri)
    return f"{base}/gold/metadata/user_features/snapshot_date={snapshot_date.isoformat()}"


def overlapping_purchase_months(
    feature_cutoff: datetime, lookback_days: int, available_months: list[str]
) -> list[str]:
    interval_start = feature_cutoff - timedelta(days=lookback_days)
    selected: list[str] = []
    for month in sorted(available_months):
        month_start = datetime.strptime(f"{month}-01", "%Y-%m-%d")
        last_day = monthrange(month_start.year, month_start.month)[1]
        month_end = month_start.replace(day=last_day) + timedelta(days=1)
        if month_start < feature_cutoff and month_end > interval_start:
            selected.append(month)
    return selected


def _read_purchases(
    spark: SparkSession,
    base_uri: str,
    feature_cutoff: datetime,
    lookback_days: int,
) -> tuple[DataFrame, list[str]]:
    root = f"{normalize_hdfs_base_uri(base_uri)}/silver/purchases"
    hdfs = filesystem(spark, root)
    statuses = hdfs.globStatus(hadoop_path(spark, f"{root}/purchase_month=*")) or []
    available = [
        status.getPath().getName().split("=", maxsplit=1)[1] for status in statuses
    ]
    selected = overlapping_purchase_months(feature_cutoff, lookback_days, available)
    if not selected:
        raise ValueError("no Silver purchase partitions overlap the feature interval")
    paths = [f"{root}/purchase_month={month}" for month in selected]
    return spark.read.option("basePath", root).parquet(*paths), selected


def _validate_dimensions_snapshot(purchases: DataFrame, expected: date) -> None:
    values = {
        row["source_dimensions_snapshot_date"]
        for row in purchases.select("source_dimensions_snapshot_date").distinct().collect()
    }
    expected_value = expected.isoformat()
    if values != {expected_value}:
        rendered = sorted("NULL" if value is None else str(value) for value in values)
        raise ValueError(
            "Silver purchases dimensions snapshot mismatch: "
            f"expected {expected_value}, found {rendered}"
        )


def _source_run_ids(frame: DataFrame) -> list[str]:
    return sorted(
        str(row["silver_run_id"])
        for row in frame.select("silver_run_id").distinct().collect()
        if row["silver_run_id"] is not None
    )


def _read_hdfs_json(spark: SparkSession, uri: str) -> dict[str, Any]:
    hdfs = filesystem(spark, uri)
    path = hadoop_path(spark, uri)
    stream = hdfs.open(path)
    reader = spark._jvm.java.io.BufferedReader(  # noqa: SLF001
        spark._jvm.java.io.InputStreamReader(stream)  # noqa: SLF001
    )
    try:
        payload = json.loads(reader.readLine())
    finally:
        reader.close()
    if not isinstance(payload, dict):
        raise ValueError("feedback feature metadata must be a JSON object")
    return payload


def _read_feedback_features(
    spark: SparkSession,
    base_uri: str,
    *,
    feature_cutoff: datetime,
    lookback_days: int,
    dimensions_snapshot_date: date,
) -> tuple[DataFrame | None, str | None]:
    snapshot_date = feature_cutoff.date()
    data_uri = gold_data_uri(base_uri, "feedback_features", snapshot_date)
    metadata_uri = (
        f"{gold_metadata_uri(base_uri, 'feedback_features', snapshot_date)}/_metadata.json"
    )
    hdfs = filesystem(spark, data_uri)
    data_exists = hdfs.exists(hadoop_path(spark, data_uri))
    metadata_exists = hdfs.exists(hadoop_path(spark, metadata_uri))
    if not data_exists and not metadata_exists:
        return None, None
    if data_exists != metadata_exists:
        raise ValueError("feedback feature snapshot data/metadata publication is incomplete")
    metadata = _read_hdfs_json(spark, metadata_uri)
    expected = {
        "feature_cutoff": feature_cutoff.isoformat(),
        "lookback_days": lookback_days,
        "dimensions_snapshot_date": dimensions_snapshot_date.isoformat(),
    }
    for key, value in expected.items():
        if metadata.get(key) != value:
            raise ValueError(
                f"feedback feature metadata {key} mismatch: "
                f"expected {value}, found {metadata.get(key)}"
            )
    run_id = metadata.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise ValueError("feedback feature metadata run_id is missing")
    frame = spark.read.parquet(data_uri)
    required = {"client_id", "feedback_feature_run_id", *FEEDBACK_FEATURE_COLUMNS}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"feedback feature snapshot is missing columns: {missing}")
    actual_run_ids = {
        row["feedback_feature_run_id"]
        for row in frame.select("feedback_feature_run_id").distinct().collect()
    }
    if actual_run_ids != {run_id}:
        raise ValueError(
            f"feedback feature run_id mismatch: expected {run_id}, found {actual_run_ids}"
        )
    return frame.select("client_id", *FEEDBACK_FEATURE_COLUMNS), run_id


def _build_features(
    clients: DataFrame,
    products: DataFrame,
    purchases: DataFrame,
    feedback_features: DataFrame | None = None,
    *,
    feature_cutoff: datetime,
    lookback_days: int,
    dimensions_snapshot_date: date,
    run_id: str,
    feature_timestamp: datetime,
) -> tuple[DataFrame, dict[str, Any]]:
    from pyspark.sql import Window
    from pyspark.sql import functions as functions

    interval_start = feature_cutoff - timedelta(days=lookback_days)
    eligible = purchases.where(
        (functions.col("transaction_datetime") >= functions.lit(interval_start))
        & (functions.col("transaction_datetime") < functions.lit(feature_cutoff))
    )
    source_rows = purchases.count()
    eligible_rows = eligible.count()
    leakage_rows = purchases.where(
        functions.col("transaction_datetime") >= functions.lit(feature_cutoff)
    ).count()
    ineligible_price_rows = eligible.where(~functions.col("is_valid_for_price")).count()

    product_categories = products.select("product_id", "level_2")
    lines = eligible.join(product_categories, "product_id", "left")
    missing_category_rows = lines.where(functions.col("level_2").isNull()).count()

    receipts = eligible.groupBy("client_id", "transaction_id").agg(
        functions.max("transaction_datetime").alias("transaction_datetime"),
        functions.max("purchase_sum").alias("receipt_sum"),
        functions.countDistinct("purchase_sum").alias("purchase_sum_variants"),
        functions.max(
            functions.coalesce(functions.col("regular_points_spent"), functions.lit(0.0))
            + functions.coalesce(functions.col("express_points_spent"), functions.lit(0.0))
        ).alias("points_spent"),
    )
    inconsistent_receipts = receipts.where(
        functions.col("purchase_sum_variants") > 1
    ).count()
    days_7 = feature_cutoff - timedelta(days=7)
    days_30 = feature_cutoff - timedelta(days=30)
    days_90 = feature_cutoff - timedelta(days=90)
    receipt_features = receipts.groupBy("client_id").agg(
        functions.count("*").alias("total_transactions"),
        functions.sum("receipt_sum").alias("total_spent"),
        functions.max("transaction_datetime").alias("last_purchase_at"),
        functions.sum(functions.greatest("points_spent", functions.lit(0.0))).alias(
            "total_points_spent"
        ),
        functions.sum(functions.greatest("receipt_sum", functions.lit(0.0))).alias(
            "positive_receipt_sum"
        ),
        functions.sum(
            functions.when(functions.col("transaction_datetime") >= days_7, 1).otherwise(0)
        ).alias("purchases_7d"),
        functions.sum(
            functions.when(functions.col("transaction_datetime") >= days_30, 1).otherwise(0)
        ).alias("purchases_30d"),
        functions.sum(
            functions.when(functions.col("transaction_datetime") >= days_90, 1).otherwise(0)
        ).alias("purchases_90d"),
        functions.countDistinct(
            functions.when(
                functions.col("transaction_datetime") >= days_30,
                functions.to_date("transaction_datetime"),
            )
        ).alias("active_days_30d"),
        functions.avg(
            functions.when(functions.col("points_spent") > 0, 1.0).otherwise(0.0)
        ).alias("promo_sensitivity_proxy"),
    ).withColumns(
        {
            "avg_check": functions.col("total_spent") / functions.col("total_transactions"),
            "days_since_last_purchase": functions.datediff(
                functions.to_date(functions.lit(feature_cutoff)),
                functions.to_date("last_purchase_at"),
            ),
            "purchase_frequency_30d": functions.when(
                functions.col("active_days_30d") > 0,
                functions.col("purchases_30d") / functions.col("active_days_30d"),
            ),
            "redeemed_points_share": functions.when(
                functions.col("positive_receipt_sum") + functions.col("total_points_spent") > 0,
                functions.least(
                    functions.lit(1.0),
                    functions.col("total_points_spent")
                    / (
                        functions.col("positive_receipt_sum")
                        + functions.col("total_points_spent")
                    ),
                ),
            ).otherwise(0.0),
        }
    )

    line_features = lines.groupBy("client_id").agg(
        functions.sum("product_quantity").alias("total_items"),
        functions.countDistinct("level_2").alias("category_diversity"),
        functions.sum(
            functions.when(
                functions.col("is_valid_for_price"), functions.col("trn_sum_from_iss")
            )
        ).alias("valid_price_sum"),
        functions.sum(
            functions.when(
                functions.col("is_valid_for_price"), functions.col("product_quantity")
            )
        ).alias("valid_price_quantity"),
    ).withColumn(
        "avg_item_price",
        functions.when(
            functions.col("valid_price_quantity") > 0,
            functions.col("valid_price_sum") / functions.col("valid_price_quantity"),
        ),
    )

    category_quantities = (
        lines.where(functions.col("level_2").isNotNull())
        .groupBy("client_id", "level_2")
        .agg(functions.sum("product_quantity").alias("category_quantity"))
    )
    favorite = (
        category_quantities.withColumn(
            "favorite_rank",
            functions.row_number().over(
                Window.partitionBy("client_id").orderBy(
                    functions.desc("category_quantity"), functions.asc("level_2")
                )
            ),
        )
        .where(functions.col("favorite_rank") == 1)
        .select("client_id", functions.col("level_2").alias("favorite_category_l2"))
    )

    age_bounds = clients.approxQuantile("age", [0.01, 0.99], 0.001)
    if len(age_bounds) != 2:
        age_bounds = [None, None]
    age_lower, age_upper = age_bounds
    age_expression = functions.col("age").cast("double")
    if age_lower is not None and age_upper is not None:
        age_expression = functions.when(
            functions.col("age").isNull(), functions.lit(None)
        ).otherwise(
            functions.greatest(
                functions.lit(age_lower),
                functions.least(functions.lit(age_upper), age_expression),
            )
        )

    features = (
        clients.select("client_id", "gender", "age")
        .join(receipt_features, "client_id", "left")
        .join(line_features, "client_id", "left")
        .join(favorite, "client_id", "left")
    )
    if feedback_features is not None:
        feedback_rows = feedback_features.count()
        duplicate_feedback_users = (
            feedback_features.groupBy("client_id").count().where("count > 1").limit(1).count()
        )
        if duplicate_feedback_users:
            raise ValueError("feedback feature snapshot contains duplicate client_id rows")
        features = features.join(feedback_features, "client_id", "left")
    else:
        feedback_rows = 0
    derived_columns = {
        "age": age_expression,
        "total_transactions": functions.coalesce("total_transactions", functions.lit(0)),
        "total_items": functions.coalesce("total_items", functions.lit(0.0)),
        "total_spent": functions.coalesce("total_spent", functions.lit(0.0)),
        "purchases_7d": functions.coalesce("purchases_7d", functions.lit(0)),
        "purchases_30d": functions.coalesce("purchases_30d", functions.lit(0)),
        "purchases_90d": functions.coalesce("purchases_90d", functions.lit(0)),
        "category_diversity": functions.coalesce(
            "category_diversity", functions.lit(0)
        ),
        "redeemed_points_share": functions.coalesce(
            "redeemed_points_share", functions.lit(0.0)
        ),
        "feature_cutoff": functions.lit(feature_cutoff).cast("timestamp"),
        "lookback_days": functions.lit(lookback_days),
        "source_dimensions_snapshot_date": functions.lit(
            dimensions_snapshot_date.isoformat()
        ),
        "feature_run_id": functions.lit(run_id),
        "feature_ts": functions.lit(feature_timestamp).cast("timestamp"),
    }
    for column in FEEDBACK_FEATURE_COLUMNS[:6]:
        derived_columns[column] = functions.coalesce(
            functions.col(column) if column in features.columns else functions.lit(None),
            functions.lit(0),
        ).cast("long")
    for column in FEEDBACK_FEATURE_COLUMNS[6:]:
        derived_columns[column] = (
            functions.col(column).cast("double")
            if column in features.columns
            else functions.lit(None).cast("double")
        )
    features = (
        features.withColumns(derived_columns)
        .drop(
            "last_purchase_at",
            "active_days_30d",
            "positive_receipt_sum",
            "total_points_spent",
            "valid_price_sum",
            "valid_price_quantity",
        )
    )
    output_rows = features.count()
    cold_start_users = features.where(functions.col("total_transactions") == 0).count()
    metrics = {
        "source_purchase_rows": source_rows,
        "eligible_purchase_rows": eligible_rows,
        "output_users": output_rows,
        "cold_start_users": cold_start_users,
        "inconsistent_receipts": inconsistent_receipts,
        "ineligible_price_rows": ineligible_price_rows,
        "missing_category_rows": missing_category_rows,
        "source_feedback_feature_rows": feedback_rows,
        "source_rows_on_or_after_cutoff": leakage_rows,
        "published_rows_on_or_after_cutoff": 0,
        "age_winsorization": {
            "lower_quantile": 0.01,
            "upper_quantile": 0.99,
            "lower_bound": age_lower,
            "upper_bound": age_upper,
        },
    }
    return features, metrics


def build_user_features(
    *,
    hdfs_base_uri: str,
    dimensions_snapshot_date: date,
    feature_cutoff: datetime,
    lookback_days: int = 180,
) -> dict[str, Any]:
    from pyspark.sql import SparkSession

    normalized_base = normalize_hdfs_base_uri(hdfs_base_uri)
    snapshot_date = feature_cutoff.date()
    run_id = uuid.uuid4().hex
    feature_timestamp = datetime.now(UTC).replace(tzinfo=None)
    staging_root = f"{normalized_base}/tmp/user-features/{run_id}"
    backup_root = f"{normalized_base}/tmp/user-features-backup/{run_id}"
    spark = SparkSession.builder.appName(f"user-features-{run_id}").config(
        "spark.sql.session.timeZone", "UTC"
    ).getOrCreate()
    hdfs = filesystem(spark, staging_root)
    try:
        partition = f"snapshot_date={dimensions_snapshot_date.isoformat()}"
        clients = spark.read.parquet(silver_data_uri(hdfs_base_uri, "clients", partition))
        products = spark.read.parquet(silver_data_uri(hdfs_base_uri, "products", partition))
        purchases, purchase_months = _read_purchases(
            spark, hdfs_base_uri, feature_cutoff, lookback_days
        )
        _validate_dimensions_snapshot(purchases, dimensions_snapshot_date)
        feedback_features, feedback_run_id = _read_feedback_features(
            spark,
            hdfs_base_uri,
            feature_cutoff=feature_cutoff,
            lookback_days=lookback_days,
            dimensions_snapshot_date=dimensions_snapshot_date,
        )
        source_run_ids = {
            "clients": _source_run_ids(clients),
            "products": _source_run_ids(products),
            "purchases": _source_run_ids(purchases),
        }
        features, metrics = _build_features(
            clients,
            products,
            purchases,
            feedback_features,
            feature_cutoff=feature_cutoff,
            lookback_days=lookback_days,
            dimensions_snapshot_date=dimensions_snapshot_date,
            run_id=run_id,
            feature_timestamp=feature_timestamp,
        )
        staged_data = f"{staging_root}/data"
        staged_metadata = f"{staging_root}/metadata"
        stage_parquet(features, staged_data)
        metadata = {
            "job": "user_features",
            "run_id": run_id,
            "created_at": feature_timestamp.isoformat(),
            "snapshot_date": snapshot_date.isoformat(),
            "feature_cutoff": feature_cutoff.isoformat(),
            "lookback_days": lookback_days,
            "dimensions_snapshot_date": dimensions_snapshot_date.isoformat(),
            "source_purchase_months": purchase_months,
            "source_silver_run_ids": source_run_ids,
            "source_feedback_feature_run_id": feedback_run_id,
            "metrics": metrics,
        }
        write_json_metadata(spark, staged_metadata, metadata)
        replace_hdfs_paths(
            spark,
            [
                (staged_data, gold_user_features_uri(hdfs_base_uri, snapshot_date)),
                (staged_metadata, gold_user_metadata_uri(hdfs_base_uri, snapshot_date)),
            ],
            backup_root,
        )
        return metadata
    finally:
        hdfs.delete(hadoop_path(spark, staging_root), True)
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    parser.add_argument("--dimensions-snapshot-date", required=True, type=parse_iso_date)
    parser.add_argument("--feature-cutoff", required=True, type=parse_feature_cutoff)
    parser.add_argument("--lookback-days", default=180, type=positive_int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_user_features(
        hdfs_base_uri=args.hdfs_base_uri,
        dimensions_snapshot_date=args.dimensions_snapshot_date,
        feature_cutoff=args.feature_cutoff,
        lookback_days=args.lookback_days,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
