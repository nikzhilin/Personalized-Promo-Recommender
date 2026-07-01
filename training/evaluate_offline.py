"""Build a compact operational evaluation snapshot for a completed recommendation run."""

from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from spark_jobs.bronze_common import filesystem, hadoop_path, normalize_hdfs_base_uri
from spark_jobs.build_user_features import parse_feature_cutoff
from spark_jobs.gold_common import gold_data_uri, gold_metadata_uri
from spark_jobs.silver_common import write_json_metadata
from spark_jobs.time_compat import UTC

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def safe_ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def evaluate_frames(
    recommendations: DataFrame, offers: DataFrame, users: DataFrame
) -> dict[str, Any]:
    from pyspark.sql import functions as functions

    user_count = users.select("client_id").distinct().count()
    recommended_users = recommendations.select("client_id").distinct().count()
    recommendation_count = recommendations.count()
    diversity = recommendations.groupBy("client_id").agg(
        functions.countDistinct("ranking_level_2").alias("categories"),
        functions.count("product_id").alias("items"),
    )
    diversity_row = diversity.agg(
        functions.avg("categories").alias("avg_categories"),
        functions.avg("items").alias("avg_list_size"),
    ).first()
    totals = offers.agg(
        functions.coalesce(functions.sum("expected_profit"), functions.lit(0.0)).alias(
            "expected_profit"
        ),
        functions.coalesce(functions.sum("incremental_profit"), functions.lit(0.0)).alias(
            "incremental_profit"
        ),
        functions.coalesce(
            functions.sum("expected_discount_cost"), functions.lit(0.0)
        ).alias("discount_spend"),
        functions.avg(functions.when(functions.col("discount") > 0, 1.0).otherwise(0.0)).alias(
            "discount_share"
        ),
    ).first()
    spend = float(totals["discount_spend"])
    incremental = float(totals["incremental_profit"])
    return {
        "recsys": {
            "users": user_count,
            "recommended_users": recommended_users,
            "coverage": safe_ratio(recommended_users, user_count),
            "recommendations": recommendation_count,
            "average_list_size": float(diversity_row["avg_list_size"] or 0.0),
            "average_category_diversity": float(diversity_row["avg_categories"] or 0.0),
        },
        "business": {
            "expected_profit_total": float(totals["expected_profit"]),
            "incremental_profit_total": incremental,
            "discount_spend": spend,
            "promo_roi": safe_ratio(incremental, spend),
            "selected_discount_share": float(totals["discount_share"] or 0.0),
        },
        "baselines": {
            "no_discount": {"discount_share": 0.0},
            "flat_10": {"policy": "all locally eligible offers until budget"},
            "top_propensity": {"policy": "descending p_base_purchase"},
            "top_uplift": {"policy": "descending uplift_score"},
            "profit_aware": {"policy": "published optimizer output"},
        },
    }


def evaluate_offline(*, hdfs_base_uri: str, feature_cutoff: datetime) -> dict[str, Any]:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("evaluate-offline").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    base = normalize_hdfs_base_uri(hdfs_base_uri)
    snapshot = feature_cutoff.date()
    run_id = uuid.uuid4().hex
    target = gold_metadata_uri(base, "offline_evaluation", snapshot)
    staging = f"{base}/_staging/offline_evaluation/{run_id}"
    backup = f"{base}/_backup/offline_evaluation/{run_id}"
    hdfs = filesystem(spark, staging)
    try:
        metrics = evaluate_frames(
            spark.read.parquet(gold_data_uri(base, "final_recommendations", snapshot)),
            spark.read.parquet(gold_data_uri(base, "optimized_offers", snapshot)),
            spark.read.parquet(gold_data_uri(base, "user_features", snapshot)),
        )
        result = {
            "job": "evaluate_offline",
            "run_id": run_id,
            "created_at": datetime.now(UTC).isoformat(),
            "feature_cutoff": feature_cutoff.isoformat(),
            "snapshot_date": snapshot.isoformat(),
            "metrics": metrics,
        }
        write_json_metadata(spark, staging, result)
        from spark_jobs.bronze_common import replace_hdfs_paths

        replace_hdfs_paths(spark, [(staging, target)], backup)
        return result
    finally:
        hdfs.delete(hadoop_path(spark, staging), True)
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    parser.add_argument("--feature-cutoff", required=True, type=parse_feature_cutoff)
    args = parser.parse_args(argv)
    print(
        json.dumps(
            evaluate_offline(
                hdfs_base_uri=args.hdfs_base_uri, feature_cutoff=args.feature_cutoff
            ),
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
