"""Build cutoff-safe Gold item features from validated Silver datasets."""

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
    read_margin_seed,
    source_run_ids,
)
from spark_jobs.silver_common import (
    parse_iso_date,
    silver_data_uri,
    stage_parquet,
    write_json_metadata,
)
from spark_jobs.simulation_config import PriceConfig, load_simulation_config
from spark_jobs.time_compat import UTC

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _build_features(
    products: DataFrame,
    purchases: DataFrame,
    *,
    default_margin: float,
    margin_overrides: dict[str, float],
    price_config: PriceConfig,
    feature_cutoff: datetime,
    lookback_days: int,
    dimensions_snapshot_date: date,
    run_id: str,
    feature_timestamp: datetime,
) -> tuple[DataFrame, dict[str, Any]]:
    from pyspark.sql import Window
    from pyspark.sql import functions as functions

    transactions = purchases.select("product_id", "client_id", "transaction_id").distinct()
    sales = purchases.groupBy("product_id").agg(
        functions.sum("product_quantity").alias("total_sales_qty"),
        functions.countDistinct("client_id").alias("unique_buyers"),
        functions.countDistinct("transaction_id").alias("transaction_count"),
    )
    buyer_counts = transactions.groupBy("product_id", "client_id").agg(
        functions.countDistinct("transaction_id").alias("buyer_transactions")
    )
    repeats = buyer_counts.groupBy("product_id").agg(
        functions.avg(
            functions.when(functions.col("buyer_transactions") > 1, 1.0).otherwise(0.0)
        ).alias("repeat_rate")
    )
    price_observations = (
        purchases.where(functions.col("is_valid_for_price"))
        .withColumn(
            "unit_price", functions.col("trn_sum_from_iss") / functions.col("product_quantity")
        )
        .where(functions.col("unit_price") > 0)
        .join(products.select("product_id", "level_2", "level_3"), "product_id")
    ).cache()
    category_bounds = price_observations.groupBy("level_2").agg(
        functions.percentile_approx(
            "unit_price",
            [price_config.lower_quantile, price_config.upper_quantile],
            price_config.median_accuracy,
        ).alias("price_bounds")
    )
    filtered_prices = (
        price_observations.join(category_bounds, "level_2")
        .where(
            (functions.col("unit_price") >= functions.col("price_bounds")[0])
            & (functions.col("unit_price") <= functions.col("price_bounds")[1])
        )
        .drop("price_bounds")
    ).cache()
    item_prices = filtered_prices.groupBy("product_id").agg(
        functions.percentile_approx(
            "unit_price", 0.5, price_config.median_accuracy
        ).alias("median_unit_price")
    )
    level_3_prices = filtered_prices.where(functions.col("level_3").isNotNull()).groupBy(
        "level_2", "level_3"
    ).agg(
        functions.percentile_approx(
            "unit_price", 0.5, price_config.median_accuracy
        ).alias("level_3_median_unit_price")
    )
    level_2_prices = filtered_prices.where(functions.col("level_2").isNotNull()).groupBy(
        "level_2"
    ).agg(
        functions.percentile_approx(
            "unit_price", 0.5, price_config.median_accuracy
        ).alias("level_2_median_unit_price")
    )
    prices = item_prices
    history = sales.join(repeats, "product_id", "left").join(prices, "product_id", "left")
    rank_window = Window.partitionBy("level_2").orderBy(
        functions.desc_nulls_last("total_sales_qty"),
        functions.desc_nulls_last("unique_buyers"),
        functions.asc("product_id"),
    )
    mapping_items: list[Any] = []
    for category, rate in sorted(margin_overrides.items()):
        mapping_items.extend([functions.lit(category), functions.lit(rate)])
    margin = functions.lit(default_margin)
    if mapping_items:
        margin = functions.coalesce(
            functions.element_at(functions.create_map(*mapping_items), functions.col("level_2")),
            margin,
        )
    features = (
        products.select(
            "product_id",
            "level_1",
            "level_2",
            "level_3",
            "level_4",
            "brand_id",
            "vendor_id",
            "segment_id",
            "netto",
            "is_own_trademark",
            "is_alcohol",
        )
        .join(history, "product_id", "left")
        .join(level_3_prices, ["level_2", "level_3"], "left")
        .join(level_2_prices, "level_2", "left")
        .withColumns(
            {
                "total_sales_qty": functions.coalesce("total_sales_qty", functions.lit(0.0)),
                "unique_buyers": functions.coalesce("unique_buyers", functions.lit(0)),
                "transaction_count": functions.coalesce("transaction_count", functions.lit(0)),
                "repeat_rate": functions.coalesce("repeat_rate", functions.lit(0.0)),
            }
        )
        .withColumn("popularity_rank_l2", functions.row_number().over(rank_window))
        .withColumns(
            {
                "margin_rate": margin,
                "unit_price": functions.coalesce(
                    "median_unit_price",
                    "level_3_median_unit_price",
                    "level_2_median_unit_price",
                ),
                "price_source": functions.when(
                    functions.col("median_unit_price").isNotNull(), "item"
                )
                .when(functions.col("level_3_median_unit_price").isNotNull(), "level_3")
                .when(functions.col("level_2_median_unit_price").isNotNull(), "level_2")
                .otherwise("missing"),
                "is_price_available": functions.coalesce(
                    functions.col("median_unit_price").isNotNull()
                    | functions.col("level_3_median_unit_price").isNotNull()
                    | functions.col("level_2_median_unit_price").isNotNull(),
                    functions.lit(False),
                ),
                "feature_cutoff": functions.lit(feature_cutoff).cast("timestamp"),
                "lookback_days": functions.lit(lookback_days),
                "source_dimensions_snapshot_date": functions.lit(
                    dimensions_snapshot_date.isoformat()
                ),
                "feature_run_id": functions.lit(run_id),
                "feature_ts": functions.lit(feature_timestamp).cast("timestamp"),
            }
        )
        .drop("level_3_median_unit_price", "level_2_median_unit_price")
    )
    features = features.cache()
    profile = features.agg(
        functions.count("*").alias("output_items"),
        functions.sum(
            functions.when(functions.col("transaction_count") == 0, 1).otherwise(0)
        ).alias("items_without_history"),
        functions.sum(
            functions.when(~functions.col("is_price_available"), 1).otherwise(0)
        ).alias("items_without_price"),
    ).first()
    metrics = {
        "eligible_purchase_rows": purchases.count(),
        "output_items": profile["output_items"],
        "items_without_history": profile["items_without_history"],
        "items_without_price": profile["items_without_price"],
        "margin_override_count": len(margin_overrides),
        "median_accuracy": price_config.median_accuracy,
        "price_observations": price_observations.count(),
        "price_observations_after_outlier_filter": filtered_prices.count(),
        "price_outliers_removed": price_observations.count() - filtered_prices.count(),
    }
    return features, metrics


def build_item_features(
    *,
    hdfs_base_uri: str,
    dimensions_snapshot_date: date,
    feature_cutoff: datetime,
    lookback_days: int = 180,
    margin_config: str = "/workspace/configs/margin_seed.csv",
    simulation_config: str = "/workspace/configs/simulation.yaml",
) -> dict[str, Any]:
    from pyspark.sql import SparkSession

    base = normalize_hdfs_base_uri(hdfs_base_uri)
    snapshot_date = feature_cutoff.date()
    run_id = uuid.uuid4().hex
    created_at = datetime.now(UTC).replace(tzinfo=None)
    staging = f"{base}/tmp/item-features/{run_id}"
    backup = f"{base}/tmp/item-features-backup/{run_id}"
    spark = (
        SparkSession.builder.appName(f"item-features-{run_id}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    hdfs = filesystem(spark, staging)
    try:
        partition = f"snapshot_date={dimensions_snapshot_date.isoformat()}"
        products = spark.read.parquet(silver_data_uri(base, "products", partition))
        purchases, months, source_rows = read_eligible_purchases(
            spark, base, feature_cutoff, lookback_days, dimensions_snapshot_date
        )
        default_margin, overrides = read_margin_seed(margin_config)
        simulation = load_simulation_config(simulation_config)
        features, metrics = _build_features(
            products,
            purchases,
            default_margin=default_margin,
            margin_overrides=overrides,
            price_config=simulation.price,
            feature_cutoff=feature_cutoff,
            lookback_days=lookback_days,
            dimensions_snapshot_date=dimensions_snapshot_date,
            run_id=run_id,
            feature_timestamp=created_at,
        )
        metrics["source_purchase_rows"] = source_rows
        staged_data, staged_metadata = f"{staging}/data", f"{staging}/metadata"
        stage_parquet(features, staged_data)
        metadata = {
            "job": "item_features",
            "run_id": run_id,
            "created_at": created_at.isoformat(),
            "snapshot_date": snapshot_date.isoformat(),
            "feature_cutoff": feature_cutoff.isoformat(),
            "lookback_days": lookback_days,
            "dimensions_snapshot_date": dimensions_snapshot_date.isoformat(),
            "source_purchase_months": months,
            "source_silver_run_ids": {
                "products": source_run_ids(products),
                "purchases": source_run_ids(purchases),
            },
            "margin_config": {"default": default_margin, "overrides": overrides},
            "simulation_version": simulation.version,
            "price_config": simulation.price.model_dump(),
            "metrics": metrics,
        }
        write_json_metadata(spark, staged_metadata, metadata)
        replace_hdfs_paths(
            spark,
            [
                (staged_data, gold_data_uri(base, "item_features", snapshot_date)),
                (staged_metadata, gold_metadata_uri(base, "item_features", snapshot_date)),
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
    parser.add_argument("--margin-config", default="/workspace/configs/margin_seed.csv")
    parser.add_argument("--simulation-config", default="/workspace/configs/simulation.yaml")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    print(
        json.dumps(
            build_item_features(
                hdfs_base_uri=args.hdfs_base_uri,
                dimensions_snapshot_date=args.dimensions_snapshot_date,
                feature_cutoff=args.feature_cutoff,
                lookback_days=args.lookback_days,
                margin_config=args.margin_config,
                simulation_config=args.simulation_config,
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
