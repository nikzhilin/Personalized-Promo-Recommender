"""Generate deterministic top-50 RecSys candidates from Gold and Silver history."""

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
from spark_jobs.silver_common import (
    parse_iso_date,
    silver_data_uri,
    stage_parquet,
    write_json_metadata,
)
from spark_jobs.time_compat import UTC

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _normalize_source(frame: DataFrame, source: str) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as functions

    window = Window.partitionBy("client_id").orderBy(functions.asc("raw_score"))
    counts = Window.partitionBy("client_id")
    return frame.withColumn(
        "normalized_score",
        functions.when(functions.count("*").over(counts) == 1, 1.0).otherwise(
            functions.percent_rank().over(window)
        ),
    ).withColumn("candidate_source", functions.lit(source))


def _limit_source(frame: DataFrame, limit: int) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as functions

    rank = Window.partitionBy("client_id").orderBy(
        functions.desc("raw_score"), functions.asc("product_id")
    )
    return (
        frame.withColumn("_rank", functions.row_number().over(rank))
        .where(functions.col("_rank") <= limit)
        .drop("_rank")
    )


def _build_candidates(
    clients: DataFrame,
    item_features: DataFrame,
    purchases: DataFrame,
    *,
    feature_cutoff: datetime,
    lookback_days: int,
    dimensions_snapshot_date: date,
    run_id: str,
    feature_timestamp: datetime,
    source_item_feature_run_id: str,
) -> tuple[DataFrame, dict[str, Any]]:
    from pyspark.sql import Window
    from pyspark.sql import functions as functions

    interactions = (
        purchases.groupBy("client_id", "product_id")
        .agg(
            functions.countDistinct("transaction_id").alias("times_bought"),
            functions.max("transaction_datetime").alias("last_purchase_at"),
        )
        .withColumn(
            "days_since_last",
            functions.datediff(
                functions.to_date(functions.lit(feature_cutoff)),
                functions.to_date("last_purchase_at"),
            ),
        )
    )
    repeat = _limit_source(
        interactions.select(
            "client_id",
            "product_id",
            (
                functions.log1p("times_bought")
                * functions.exp(-functions.col("days_since_last") / 45.0)
            ).alias("raw_score"),
        ),
        20,
    )

    product_categories = item_features.select("product_id", "level_2", "unique_buyers")
    category_totals = (
        purchases.join(product_categories.select("product_id", "level_2"), "product_id")
        .where(functions.col("level_2").isNotNull())
        .groupBy("client_id", "level_2")
        .agg(functions.sum("product_quantity").alias("category_qty"))
    )
    user_totals = category_totals.groupBy("client_id").agg(
        functions.sum("category_qty").alias("user_qty")
    )
    affinities = category_totals.join(user_totals, "client_id").withColumn(
        "affinity", functions.col("category_qty") / functions.col("user_qty")
    )
    category_rank = Window.partitionBy("client_id").orderBy(
        functions.desc("affinity"), functions.asc("level_2")
    )
    top_categories = affinities.withColumn(
        "_rank", functions.row_number().over(category_rank)
    ).where(functions.col("_rank") <= 2)
    category = _limit_source(
        top_categories.join(product_categories, "level_2").select(
            "client_id",
            "product_id",
            (functions.col("affinity") * functions.log1p("unique_buyers")).alias("raw_score"),
        ),
        25,
    )

    receipt_items = purchases.select("client_id", "transaction_id", "product_id").distinct()
    item_receipts = receipt_items.groupBy("product_id").agg(
        functions.countDistinct("client_id", "transaction_id").alias("receipt_count")
    )
    left = receipt_items.select(
        "client_id", "transaction_id", functions.col("product_id").alias("seed_product_id")
    )
    right = receipt_items.select("client_id", "transaction_id", "product_id")
    pairs = (
        left.join(right, ["client_id", "transaction_id"])
        .where(functions.col("seed_product_id") != functions.col("product_id"))
        .groupBy("seed_product_id", "product_id")
        .agg(functions.countDistinct("client_id", "transaction_id").alias("co_receipts"))
        .join(
            item_receipts.select(
                functions.col("product_id").alias("seed_product_id"),
                functions.col("receipt_count").alias("seed_receipts"),
            ),
            "seed_product_id",
        )
        .join(
            item_receipts.select(
                "product_id", functions.col("receipt_count").alias("candidate_receipts")
            ),
            "product_id",
        )
        .withColumn(
            "similarity",
            functions.col("co_receipts")
            / functions.sqrt(functions.col("seed_receipts") * functions.col("candidate_receipts")),
        )
    )
    neighbour_rank = Window.partitionBy("seed_product_id").orderBy(
        functions.desc("similarity"), functions.asc("product_id")
    )
    neighbours = pairs.withColumn("_rank", functions.row_number().over(neighbour_rank)).where(
        functions.col("_rank") <= 20
    )
    i2i = _limit_source(
        interactions.select(
            "client_id", functions.col("product_id").alias("seed_product_id"), "days_since_last"
        )
        .join(neighbours, "seed_product_id")
        .groupBy("client_id", "product_id")
        .agg(
            functions.sum(
                functions.col("similarity")
                * functions.exp(-functions.col("days_since_last") / 45.0)
            ).alias("raw_score")
        ),
        25,
    )

    global_rank = Window.orderBy(
        functions.desc("unique_buyers"),
        functions.desc("total_sales_qty"),
        functions.asc("product_id"),
    )
    global_items = (
        item_features.where(functions.col("is_alcohol") == 0)
        .withColumn("_rank", functions.row_number().over(global_rank))
        .where(functions.col("_rank") <= 20)
        .select("product_id", functions.log1p("unique_buyers").alias("raw_score"))
    )
    global_candidates = clients.select("client_id").crossJoin(functions.broadcast(global_items))

    sources = [
        _normalize_source(repeat, "repeat_purchase"),
        _normalize_source(category, "category_popular"),
        _normalize_source(i2i, "item_to_item"),
        _normalize_source(global_candidates, "global_popular"),
    ]
    combined = sources[0]
    for source in sources[1:]:
        combined = combined.unionByName(source)
    merged = (
        combined.groupBy("client_id", "product_id")
        .agg(
            functions.sort_array(functions.collect_set("candidate_source")).alias(
                "candidate_sources"
            ),
            functions.max("normalized_score").alias("best_source_score"),
            functions.countDistinct("candidate_source").alias("source_count"),
        )
        .withColumn(
            "recsys_score",
            functions.col("best_source_score") + 0.05 * (functions.col("source_count") - 1),
        )
    )
    final_rank = Window.partitionBy("client_id").orderBy(
        functions.desc("recsys_score"), functions.asc("product_id")
    )
    candidates = (
        merged.withColumn("candidate_rank", functions.row_number().over(final_rank))
        .where(functions.col("candidate_rank") <= 50)
        .withColumns(
            {
                "feature_cutoff": functions.lit(feature_cutoff).cast("timestamp"),
                "lookback_days": functions.lit(lookback_days),
                "source_dimensions_snapshot_date": functions.lit(
                    dimensions_snapshot_date.isoformat()
                ),
                "feature_run_id": functions.lit(run_id),
                "source_item_feature_run_id": functions.lit(source_item_feature_run_id),
                "feature_ts": functions.lit(feature_timestamp).cast("timestamp"),
            }
        )
    )
    candidates = candidates.cache()
    output_candidates = candidates.count()
    counts = (
        candidates.groupBy("client_id")
        .count()
        .agg(
            functions.count("*").alias("output_clients"),
            functions.min("count").alias("min_candidates"),
            functions.max("count").alias("max_candidates"),
        )
        .first()
    )
    expected_clients = clients.select("client_id").distinct().count()
    if counts["output_clients"] != expected_clients:
        raise ValueError(
            "candidate client coverage mismatch: "
            f"expected {expected_clients}, produced {counts['output_clients']}"
        )
    metrics = {
        "output_candidates": output_candidates,
        "output_clients": counts["output_clients"],
        "min_candidates_per_client": counts["min_candidates"],
        "max_candidates_per_client": counts["max_candidates"],
    }
    return candidates, metrics


def generate_candidates(
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
    staging, backup = f"{base}/tmp/candidates/{run_id}", f"{base}/tmp/candidates-backup/{run_id}"
    spark = (
        SparkSession.builder.appName(f"candidates-{run_id}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    hdfs = filesystem(spark, staging)
    try:
        clients = spark.read.parquet(
            silver_data_uri(
                base, "clients", f"snapshot_date={dimensions_snapshot_date.isoformat()}"
            )
        )
        items = spark.read.parquet(gold_data_uri(base, "item_features", snapshot))
        require_gold_contract(
            items,
            feature_cutoff=feature_cutoff,
            lookback_days=lookback_days,
            dimensions_snapshot_date=dimensions_snapshot_date,
            entity="item_features",
        )
        item_run_ids = sorted(
            row["feature_run_id"] for row in items.select("feature_run_id").distinct().collect()
        )
        if len(item_run_ids) != 1:
            raise ValueError(f"item_features must contain one feature_run_id: {item_run_ids}")
        purchases, months, _ = read_eligible_purchases(
            spark, base, feature_cutoff, lookback_days, dimensions_snapshot_date
        )
        candidates, metrics = _build_candidates(
            clients,
            items,
            purchases,
            feature_cutoff=feature_cutoff,
            lookback_days=lookback_days,
            dimensions_snapshot_date=dimensions_snapshot_date,
            run_id=run_id,
            feature_timestamp=created_at,
            source_item_feature_run_id=item_run_ids[0],
        )
        data_stage, metadata_stage = f"{staging}/data", f"{staging}/metadata"
        stage_parquet(candidates, data_stage)
        metadata = {
            "job": "recsys_candidates",
            "run_id": run_id,
            "created_at": created_at.isoformat(),
            "snapshot_date": snapshot.isoformat(),
            "feature_cutoff": feature_cutoff.isoformat(),
            "lookback_days": lookback_days,
            "dimensions_snapshot_date": dimensions_snapshot_date.isoformat(),
            "source_purchase_months": months,
            "source_item_feature_run_ids": item_run_ids,
            "metrics": metrics,
        }
        write_json_metadata(spark, metadata_stage, metadata)
        replace_hdfs_paths(
            spark,
            [
                (data_stage, gold_data_uri(base, "recsys_candidates", snapshot)),
                (metadata_stage, gold_metadata_uri(base, "recsys_candidates", snapshot)),
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
    result = generate_candidates(
        hdfs_base_uri=args.hdfs_base_uri,
        dimensions_snapshot_date=args.dimensions_snapshot_date,
        feature_cutoff=args.feature_cutoff,
        lookback_days=args.lookback_days,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
