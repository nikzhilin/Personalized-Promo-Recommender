"""Build Gold user-item features only for published RecSys candidate pairs."""

from __future__ import annotations

import argparse
import json
import uuid
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from spark_jobs.bronze_common import (
    filesystem,
    hadoop_path,
    normalize_hdfs_base_uri,
    replace_hdfs_paths,
)
from spark_jobs.build_user_features import parse_feature_cutoff, positive_int
from spark_jobs.gold_common import (
    gold_data_uri,
    gold_metadata_uri,
    read_eligible_purchases,
    require_gold_contract,
)
from spark_jobs.silver_common import parse_iso_date, stage_parquet, write_json_metadata
from spark_jobs.time_compat import UTC

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _build_features(
    candidates: DataFrame,
    users: DataFrame,
    items: DataFrame,
    purchases: DataFrame,
    *,
    feature_cutoff: datetime,
    lookback_days: int,
    dimensions_snapshot_date: date,
    run_id: str,
    feature_timestamp: datetime,
) -> tuple[DataFrame, dict[str, Any]]:
    from pyspark.sql import functions as functions

    history = purchases.groupBy("client_id", "product_id").agg(
        functions.countDistinct("transaction_id").alias("times_bought"),
        functions.max("transaction_datetime").alias("last_item_purchase_at"),
    )
    product_categories = items.select("product_id", "level_2")
    category_qty = (
        purchases.join(product_categories, "product_id")
        .where(functions.col("level_2").isNotNull())
        .groupBy("client_id", "level_2")
        .agg(functions.sum("product_quantity").alias("category_qty"))
    )
    user_qty = category_qty.groupBy("client_id").agg(
        functions.sum("category_qty").alias("user_qty")
    )
    affinities = category_qty.join(user_qty, "client_id").select(
        "client_id",
        "level_2",
        (functions.col("category_qty") / functions.col("user_qty")).alias("user_category_affinity"),
    )
    category_sizes = items.groupBy("level_2").agg(
        functions.max("popularity_rank_l2").alias("category_item_count")
    )
    item_values = (
        items.join(category_sizes, "level_2", "left")
        .withColumn(
            "item_popularity_norm",
            functions.when(functions.col("category_item_count") <= 1, 1.0).otherwise(
                1.0
                - (functions.col("popularity_rank_l2") - 1)
                / (functions.col("category_item_count") - 1)
            ),
        )
        .select("product_id", "level_2", "median_unit_price", "item_popularity_norm")
    )

    features = (
        candidates.select(
            "client_id", "product_id", "candidate_sources", "candidate_rank", "recsys_score"
        )
        .join(history, ["client_id", "product_id"], "left")
        .join(users.select("client_id", "avg_item_price", "favorite_category_l2"), "client_id")
        .join(item_values, "product_id")
        .join(affinities, ["client_id", "level_2"], "left")
        .withColumns(
            {
                "bought_before": functions.when(
                    functions.col("times_bought").isNotNull(), 1
                ).otherwise(0),
                "times_bought": functions.coalesce("times_bought", functions.lit(0)),
                "days_since_last_item_purchase": functions.datediff(
                    functions.to_date(functions.lit(feature_cutoff)),
                    functions.to_date("last_item_purchase_at"),
                ),
                "user_category_affinity": functions.coalesce(
                    "user_category_affinity", functions.lit(0.0)
                ),
                "price_vs_user_avg": functions.when(
                    functions.col("median_unit_price").isNotNull()
                    & (functions.col("avg_item_price") > 0),
                    functions.col("median_unit_price") / functions.col("avg_item_price"),
                ),
            }
        )
        .withColumn(
            "category_match_score",
            functions.least(
                functions.lit(1.0),
                functions.col("user_category_affinity")
                + functions.when(
                    functions.col("favorite_category_l2") == functions.col("level_2"), 0.25
                ).otherwise(0.0),
            ),
        )
        .withColumns(
            {
                "feature_cutoff": functions.lit(feature_cutoff).cast("timestamp"),
                "lookback_days": functions.lit(lookback_days),
                "source_dimensions_snapshot_date": functions.lit(
                    dimensions_snapshot_date.isoformat()
                ),
                "feature_run_id": functions.lit(run_id),
                "feature_ts": functions.lit(feature_timestamp).cast("timestamp"),
            }
        )
        .drop(
            "last_item_purchase_at", "avg_item_price", "favorite_category_l2", "category_item_count"
        )
    )
    features = features.cache()
    output_count = features.count()
    duplicates = (
        features.groupBy("client_id", "product_id")
        .count()
        .where(functions.col("count") != 1)
        .count()
    )
    if duplicates:
        raise ValueError(f"user_item_features contains {duplicates} duplicate candidate keys")
    candidate_count = candidates.count()
    if output_count != candidate_count:
        raise ValueError(
            f"candidate coverage mismatch: expected {candidate_count}, produced {output_count}"
        )
    metrics = {
        "input_candidates": candidate_count,
        "output_pairs": output_count,
        "previously_bought_pairs": features.where(functions.col("bought_before") == 1).count(),
        "missing_item_price_pairs": features.where(
            functions.col("median_unit_price").isNull()
        ).count(),
        "missing_user_average_price_pairs": features.where(
            functions.col("price_vs_user_avg").isNull()
        ).count(),
    }
    return features, metrics


def build_user_item_features(
    *,
    hdfs_base_uri: str,
    dimensions_snapshot_date: date,
    feature_cutoff: datetime,
    lookback_days: int = 180,
) -> dict[str, Any]:
    from pyspark.sql import SparkSession

    base = normalize_hdfs_base_uri(hdfs_base_uri)
    snapshot = feature_cutoff.date()
    run_id = uuid.uuid4().hex
    created_at = datetime.now(UTC).replace(tzinfo=None)
    staging = f"{base}/tmp/user-item-features/{run_id}"
    backup = f"{base}/tmp/user-item-features-backup/{run_id}"
    spark = (
        SparkSession.builder.appName(f"user-item-features-{run_id}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    hdfs = filesystem(spark, staging)
    try:
        users = spark.read.parquet(gold_data_uri(base, "user_features", snapshot))
        items = spark.read.parquet(gold_data_uri(base, "item_features", snapshot))
        candidates = spark.read.parquet(gold_data_uri(base, "recsys_candidates", snapshot))
        for entity, frame in [
            ("user_features", users),
            ("item_features", items),
            ("recsys_candidates", candidates),
        ]:
            require_gold_contract(
                frame,
                feature_cutoff=feature_cutoff,
                lookback_days=lookback_days,
                dimensions_snapshot_date=dimensions_snapshot_date,
                entity=entity,
            )
        item_run_ids = {
            row["feature_run_id"] for row in items.select("feature_run_id").distinct().collect()
        }
        candidate_item_run_ids = {
            row["source_item_feature_run_id"]
            for row in candidates.select("source_item_feature_run_id").distinct().collect()
        }
        if candidate_item_run_ids != item_run_ids:
            raise ValueError(
                "recsys_candidates item lineage mismatch: "
                f"expected {sorted(item_run_ids)}, found {sorted(candidate_item_run_ids)}"
            )
        purchases, months, _ = read_eligible_purchases(
            spark, base, feature_cutoff, lookback_days, dimensions_snapshot_date
        )
        features, metrics = _build_features(
            candidates,
            users,
            items,
            purchases,
            feature_cutoff=feature_cutoff,
            lookback_days=lookback_days,
            dimensions_snapshot_date=dimensions_snapshot_date,
            run_id=run_id,
            feature_timestamp=created_at,
        )
        data_stage, metadata_stage = f"{staging}/data", f"{staging}/metadata"
        stage_parquet(features, data_stage)
        source_runs = {
            entity: sorted(
                row["feature_run_id"] for row in frame.select("feature_run_id").distinct().collect()
            )
            for entity, frame in [
                ("user_features", users),
                ("item_features", items),
                ("recsys_candidates", candidates),
            ]
        }
        metadata = {
            "job": "user_item_features",
            "run_id": run_id,
            "created_at": created_at.isoformat(),
            "snapshot_date": snapshot.isoformat(),
            "feature_cutoff": feature_cutoff.isoformat(),
            "lookback_days": lookback_days,
            "dimensions_snapshot_date": dimensions_snapshot_date.isoformat(),
            "source_purchase_months": months,
            "source_gold_run_ids": source_runs,
            "metrics": metrics,
        }
        write_json_metadata(spark, metadata_stage, metadata)
        replace_hdfs_paths(
            spark,
            [
                (data_stage, gold_data_uri(base, "user_item_features", snapshot)),
                (metadata_stage, gold_metadata_uri(base, "user_item_features", snapshot)),
            ],
            backup,
        )
        return metadata
    finally:
        hdfs.delete(hadoop_path(spark, staging), True)
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
    result = build_user_item_features(
        hdfs_base_uri=args.hdfs_base_uri,
        dimensions_snapshot_date=args.dimensions_snapshot_date,
        feature_cutoff=args.feature_cutoff,
        lookback_days=args.lookback_days,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
