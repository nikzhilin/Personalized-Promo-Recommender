"""Build cutoff-safe Gold user feedback features from exported HDFS events."""

from __future__ import annotations

import argparse
import json
import uuid
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from spark_jobs.bronze_common import (
    filesystem,
    hadoop_path,
    normalize_hdfs_base_uri,
    replace_hdfs_paths,
)
from spark_jobs.build_user_features import parse_feature_cutoff, positive_int
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

REQUIRED_EVENT_COLUMNS = {
    "event_id",
    "client_id",
    "event_type",
    "shown_discount",
    "purchase_value",
    "created_at",
    "received_at",
    "verification_status",
    "event_fingerprint",
}


def overlapping_event_dates(
    feature_cutoff: datetime, lookback_days: int, available_dates: list[str]
) -> list[str]:
    start = feature_cutoff.date() - timedelta(days=lookback_days)
    end = feature_cutoff.date()
    selected: list[str] = []
    for value in sorted(available_dates):
        parsed = datetime.strptime(value, "%Y-%m-%d").date()
        if start <= parsed <= end:
            selected.append(value)
    return selected


def _read_events(
    spark: SparkSession,
    base_uri: str,
    feature_cutoff: datetime,
    lookback_days: int,
) -> tuple[DataFrame | None, list[str]]:
    root = f"{normalize_hdfs_base_uri(base_uri)}/feedback/events"
    hdfs = filesystem(spark, root)
    if not hdfs.exists(hadoop_path(spark, root)):
        return None, []
    statuses = hdfs.globStatus(hadoop_path(spark, f"{root}/event_date=*")) or []
    available = [status.getPath().getName().split("=", maxsplit=1)[1] for status in statuses]
    selected = overlapping_event_dates(feature_cutoff, lookback_days, available)
    if not selected:
        return None, []
    frame = spark.read.option("basePath", root).parquet(
        *[f"{root}/event_date={value}" for value in selected]
    )
    missing = sorted(REQUIRED_EVENT_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"feedback events are missing required columns: {missing}")
    return frame, selected


def _build_features(
    clients: DataFrame,
    events: DataFrame | None,
    *,
    feature_cutoff: datetime,
    lookback_days: int,
    dimensions_snapshot_date: date,
    run_id: str,
    feature_timestamp: datetime,
) -> tuple[DataFrame, dict[str, Any]]:
    from pyspark.sql import functions as functions

    client_ids = clients.select("client_id")
    start = feature_cutoff - timedelta(days=lookback_days)
    day_30 = feature_cutoff - timedelta(days=30)
    day_90 = feature_cutoff - timedelta(days=90)
    metrics: dict[str, Any] = {
        "source_event_rows": 0,
        "verified_event_rows": 0,
        "unverified_event_rows": 0,
        "outside_event_time_rows": 0,
        "late_received_rows": 0,
        "unknown_client_rows": 0,
        "duplicate_event_rows": 0,
    }

    aggregates = None
    if events is not None:
        metrics["source_event_rows"] = events.count()
        conflicts = (
            events.groupBy("event_id")
            .agg(functions.countDistinct("event_fingerprint").alias("fingerprints"))
            .where(functions.col("fingerprints") > 1)
            .limit(1)
            .count()
        )
        if conflicts:
            raise ValueError("feedback event_id has conflicting fingerprints")
        distinct_events = events.dropDuplicates(["event_id", "event_fingerprint"])
        metrics["duplicate_event_rows"] = metrics["source_event_rows"] - distinct_events.count()
        metrics["outside_event_time_rows"] = distinct_events.where(
            (functions.col("created_at") < functions.lit(start))
            | (functions.col("created_at") >= functions.lit(feature_cutoff))
        ).count()
        in_event_window = distinct_events.where(
            (functions.col("created_at") >= functions.lit(start))
            & (functions.col("created_at") < functions.lit(feature_cutoff))
        )
        metrics["late_received_rows"] = in_event_window.where(
            functions.col("received_at") >= functions.lit(feature_cutoff)
        ).count()
        cutoff_safe = in_event_window.where(
            functions.col("received_at") < functions.lit(feature_cutoff)
        )
        metrics["unverified_event_rows"] = cutoff_safe.where(
            functions.col("verification_status") != "VERIFIED"
        ).count()
        verified = cutoff_safe.where(functions.col("verification_status") == "VERIFIED")
        metrics["verified_event_rows"] = verified.count()
        metrics["unknown_client_rows"] = verified.join(
            client_ids, "client_id", "left_anti"
        ).count()
        known = verified.join(client_ids, "client_id", "inner")
        purchases_180 = functions.sum(
            functions.when(functions.col("event_type") == "purchase", 1).otherwise(0)
        )
        discounted_180 = functions.sum(
            functions.when(
                (functions.col("event_type") == "purchase")
                & (functions.col("shown_discount") > 0),
                1,
            ).otherwise(0)
        )
        organic_180 = functions.sum(
            functions.when(
                (functions.col("event_type") == "purchase")
                & (functions.col("shown_discount") == 0),
                1,
            ).otherwise(0)
        )

        def event_count(event_type: str, since: datetime) -> Any:
            return functions.sum(
                functions.when(
                    (functions.col("event_type") == event_type)
                    & (functions.col("created_at") >= functions.lit(since)),
                    1,
                ).otherwise(0)
            )

        aggregates = known.groupBy("client_id").agg(
            event_count("click", day_30).alias("feedback_clicks_30d"),
            event_count("click", day_90).alias("feedback_clicks_90d"),
            event_count("cart", day_30).alias("feedback_carts_30d"),
            event_count("cart", day_90).alias("feedback_carts_90d"),
            event_count("purchase", day_30).alias("feedback_purchases_30d"),
            event_count("purchase", day_90).alias("feedback_purchases_90d"),
            functions.when(purchases_180 > 0, discounted_180 / purchases_180).alias(
                "feedback_discounted_purchase_share_180d"
            ),
            functions.when(purchases_180 > 0, organic_180 / purchases_180).alias(
                "feedback_organic_purchase_share_180d"
            ),
            functions.avg("shown_discount").alias("feedback_avg_shown_discount_180d"),
            functions.avg(
                functions.when(
                    functions.col("event_type") == "purchase",
                    functions.col("purchase_value"),
                )
            ).alias("feedback_avg_purchase_value_180d"),
        )

    features = client_ids
    if aggregates is not None:
        features = features.join(aggregates, "client_id", "left")
    for column in FEEDBACK_FEATURE_COLUMNS[:6]:
        if column not in features.columns:
            features = features.withColumn(column, functions.lit(0).cast("long"))
        else:
            features = features.withColumn(column, functions.coalesce(column, functions.lit(0)))
    for column in FEEDBACK_FEATURE_COLUMNS[6:]:
        if column not in features.columns:
            features = features.withColumn(column, functions.lit(None).cast("double"))
    features = features.withColumns(
        {
            "feature_cutoff": functions.lit(feature_cutoff).cast("timestamp"),
            "lookback_days": functions.lit(lookback_days),
            "source_dimensions_snapshot_date": functions.lit(
                dimensions_snapshot_date.isoformat()
            ),
            "feedback_feature_run_id": functions.lit(run_id),
            "feedback_feature_ts": functions.lit(feature_timestamp).cast("timestamp"),
        }
    )
    metrics["output_users"] = features.count()
    metrics["users_with_verified_feedback"] = (
        0 if aggregates is None else aggregates.select("client_id").distinct().count()
    )
    return features, metrics


def build_feedback_features(
    *,
    hdfs_base_uri: str,
    dimensions_snapshot_date: date,
    feature_cutoff: datetime,
    lookback_days: int = 180,
) -> dict[str, Any]:
    from pyspark.sql import SparkSession

    base = normalize_hdfs_base_uri(hdfs_base_uri)
    snapshot_date = feature_cutoff.date()
    run_id = uuid.uuid4().hex
    feature_timestamp = datetime.now(UTC).replace(tzinfo=None)
    staging_root = f"{base}/tmp/feedback-features/{run_id}"
    backup_root = f"{base}/tmp/feedback-features-backup/{run_id}"
    spark = SparkSession.builder.appName(f"feedback-features-{run_id}").config(
        "spark.sql.session.timeZone", "UTC"
    ).getOrCreate()
    hdfs = filesystem(spark, staging_root)
    try:
        clients = spark.read.parquet(
            silver_data_uri(
                base,
                "clients",
                f"snapshot_date={dimensions_snapshot_date.isoformat()}",
            )
        )
        events, source_partitions = _read_events(
            spark, base, feature_cutoff, lookback_days
        )
        features, metrics = _build_features(
            clients,
            events,
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
            "job": "feedback_features",
            "run_id": run_id,
            "created_at": feature_timestamp.isoformat(),
            "snapshot_date": snapshot_date.isoformat(),
            "feature_cutoff": feature_cutoff.isoformat(),
            "lookback_days": lookback_days,
            "dimensions_snapshot_date": dimensions_snapshot_date.isoformat(),
            "source_feedback_partitions": source_partitions,
            "verification_policy": "VERIFIED_ONLY",
            "metrics": metrics,
        }
        write_json_metadata(spark, staged_metadata, metadata)
        replace_hdfs_paths(
            spark,
            [
                (staged_data, gold_data_uri(base, "feedback_features", snapshot_date)),
                (
                    staged_metadata,
                    gold_metadata_uri(base, "feedback_features", snapshot_date),
                ),
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
    result = build_feedback_features(
        hdfs_base_uri=args.hdfs_base_uri,
        dimensions_snapshot_date=args.dimensions_snapshot_date,
        feature_cutoff=args.feature_cutoff,
        lookback_days=args.lookback_days,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
