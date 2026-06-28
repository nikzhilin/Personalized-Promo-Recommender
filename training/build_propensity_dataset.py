"""Build a temporally split propensity dataset from published Gold snapshots."""

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
from spark_jobs.build_user_features import parse_feature_cutoff, positive_int
from spark_jobs.gold_common import gold_data_uri, gold_metadata_uri, require_gold_contract
from spark_jobs.silver_common import parse_iso_date, stage_parquet, write_json_metadata
from spark_jobs.time_compat import UTC

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def validate_cutoffs(cutoffs: list[datetime]) -> list[datetime]:
    ordered = sorted(set(cutoffs))
    if len(ordered) < 2:
        raise ValueError("at least two distinct feature cutoffs are required for temporal split")
    if ordered != cutoffs:
        raise ValueError("feature cutoffs must be unique and strictly increasing")
    if len({cutoff.date() for cutoff in ordered}) != len(ordered):
        raise ValueError("feature cutoffs must use distinct snapshot dates")
    return ordered


def label_purchase_months(
    feature_cutoff: datetime, label_window_days: int, available_months: list[str]
) -> list[str]:
    label_end = feature_cutoff + timedelta(days=label_window_days)
    selected: list[str] = []
    for month in sorted(available_months):
        month_start = datetime.strptime(f"{month}-01", "%Y-%m-%d")
        month_end = month_start.replace(
            day=monthrange(month_start.year, month_start.month)[1]
        ) + timedelta(days=1)
        if month_start < label_end and month_end > feature_cutoff:
            selected.append(month)
    return selected


def _read_label_purchases(
    spark: SparkSession,
    base_uri: str,
    feature_cutoff: datetime,
    label_window_days: int,
    dimensions_snapshot_date: date,
) -> tuple[DataFrame, list[str]]:
    from pyspark.sql import functions as functions

    root = f"{normalize_hdfs_base_uri(base_uri)}/silver/purchases"
    hdfs = filesystem(spark, root)
    statuses = hdfs.globStatus(hadoop_path(spark, f"{root}/purchase_month=*")) or []
    available = [status.getPath().getName().split("=", maxsplit=1)[1] for status in statuses]
    selected = label_purchase_months(feature_cutoff, label_window_days, available)
    if not selected:
        raise ValueError(f"no Silver purchases overlap label window for {feature_cutoff}")
    purchases = spark.read.option("basePath", root).parquet(
        *[f"{root}/purchase_month={month}" for month in selected]
    )
    snapshots = {
        row["source_dimensions_snapshot_date"]
        for row in purchases.select("source_dimensions_snapshot_date").distinct().collect()
    }
    expected = dimensions_snapshot_date.isoformat()
    if snapshots != {expected}:
        raise ValueError(
            "Silver purchases dimensions snapshot mismatch in label window: "
            f"expected {expected}, found {sorted(str(value) for value in snapshots)}"
        )
    label_end = feature_cutoff + timedelta(days=label_window_days)
    return (
        purchases.where(
            (functions.col("transaction_datetime") >= functions.lit(feature_cutoff))
            & (functions.col("transaction_datetime") < functions.lit(label_end))
        ),
        selected,
    )


def _feature_columns(
    frame: DataFrame, keys: set[str], excluded: set[str] | None = None
) -> list[str]:
    lineage = {
        "feature_cutoff",
        "lookback_days",
        "source_dimensions_snapshot_date",
        "feature_run_id",
        "feature_ts",
        "source_item_feature_run_id",
    }
    return [
        column
        for column in frame.columns
        if column not in keys | lineage | (excluded or set())
    ]


def _build_observation(
    users: DataFrame,
    items: DataFrame,
    pairs: DataFrame,
    label_purchases: DataFrame,
    *,
    feature_cutoff: datetime,
    split: str,
) -> tuple[DataFrame, int]:
    from pyspark.sql import functions as functions

    positives = label_purchases.select("client_id", "product_id").distinct().withColumn(
        "label", functions.lit(1)
    )
    user_columns = _feature_columns(users, {"client_id"})
    item_columns = _feature_columns(items, {"product_id"})
    pair_columns = _feature_columns(
        pairs,
        {"client_id", "product_id"},
        {"level_2", "median_unit_price"},
    )
    frame = (
        pairs.select("client_id", "product_id", *pair_columns)
        .join(users.select("client_id", *user_columns), "client_id")
        .join(items.select("product_id", *item_columns), "product_id")
        .join(positives, ["client_id", "product_id"], "left")
        .withColumn("label", functions.coalesce("label", functions.lit(0)))
        .withColumn("observation_cutoff", functions.lit(feature_cutoff).cast("timestamp"))
        .withColumn("dataset_split", functions.lit(split))
    )
    if "candidate_sources" in frame.columns:
        frame = frame.withColumn(
            "candidate_sources", functions.concat_ws("|", "candidate_sources")
        )
    future_positive_pairs = positives.count()
    matched_positive_pairs = frame.where(functions.col("label") == 1).count()
    return frame, future_positive_pairs - matched_positive_pairs


def _sample_negatives(frame: DataFrame, negative_ratio: int, random_seed: int) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as functions

    positives = frame.where(functions.col("label") == 1)
    negatives = frame.where(functions.col("label") == 0)
    positive_count = positives.count()
    if positive_count == 0:
        raise ValueError("propensity dataset has no positive candidate pairs")
    limits = positives.groupBy("dataset_split").agg(
        (functions.count("*") * functions.lit(negative_ratio)).alias("_negative_limit")
    )
    order = Window.partitionBy("dataset_split").orderBy(
        functions.xxhash64(
            "client_id", "product_id", "observation_cutoff", functions.lit(random_seed)
        ),
        functions.asc("client_id"),
        functions.asc("product_id"),
        functions.asc("observation_cutoff"),
    )
    sampled = (
        negatives.join(limits, "dataset_split")
        .withColumn("_sample_rank", functions.row_number().over(order))
        .where(functions.col("_sample_rank") <= functions.col("_negative_limit"))
        .drop("_sample_rank", "_negative_limit")
    )
    return positives.unionByName(sampled)


def build_propensity_dataset(
    *,
    hdfs_base_uri: str,
    dimensions_snapshot_date: date,
    feature_cutoffs: list[datetime],
    lookback_days: int = 180,
    label_window_days: int = 30,
    negative_ratio: int = 3,
    random_seed: int = 42,
) -> dict[str, Any]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as functions

    cutoffs = validate_cutoffs(feature_cutoffs)
    base = normalize_hdfs_base_uri(hdfs_base_uri)
    dataset_snapshot = cutoffs[-1].date()
    run_id = uuid.uuid4().hex
    created_at = datetime.now(UTC).replace(tzinfo=None)
    staging = f"{base}/tmp/propensity-dataset/{run_id}"
    backup = f"{base}/tmp/propensity-dataset-backup/{run_id}"
    spark = (
        SparkSession.builder.appName(f"propensity-dataset-{run_id}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    hdfs = filesystem(spark, staging)
    try:
        observations: list[DataFrame] = []
        source_months: dict[str, list[str]] = {}
        source_runs: dict[str, dict[str, list[str]]] = {}
        unmatched_positives = 0
        for cutoff in cutoffs:
            snapshot = cutoff.date()
            frames = {
                entity: spark.read.parquet(gold_data_uri(base, entity, snapshot))
                for entity in ("user_features", "item_features", "user_item_features")
            }
            for entity, frame in frames.items():
                require_gold_contract(
                    frame,
                    feature_cutoff=cutoff,
                    lookback_days=lookback_days,
                    dimensions_snapshot_date=dimensions_snapshot_date,
                    entity=entity,
                )
            labels, months = _read_label_purchases(
                spark, base, cutoff, label_window_days, dimensions_snapshot_date
            )
            split = "validation" if cutoff == cutoffs[-1] else "train"
            observation, unmatched = _build_observation(
                frames["user_features"],
                frames["item_features"],
                frames["user_item_features"],
                labels,
                feature_cutoff=cutoff,
                split=split,
            )
            observations.append(observation)
            unmatched_positives += unmatched
            key = cutoff.isoformat()
            source_months[key] = months
            source_runs[key] = {
                entity: sorted(
                    str(row["feature_run_id"])
                    for row in frame.select("feature_run_id").distinct().collect()
                )
                for entity, frame in frames.items()
            }
        dataset = observations[0]
        for observation in observations[1:]:
            dataset = dataset.unionByName(observation)
        dataset = _sample_negatives(dataset, negative_ratio, random_seed).cache()
        profile = dataset.groupBy("dataset_split").agg(
            functions.count("*").alias("rows"),
            functions.sum("label").alias("positives"),
        )
        metrics = {row["dataset_split"]: row.asDict() for row in profile.collect()}
        if set(metrics) != {"train", "validation"}:
            raise ValueError(f"both temporal splits must be non-empty, found {sorted(metrics)}")
        duplicate_keys = (
            dataset.groupBy("client_id", "product_id", "observation_cutoff")
            .count()
            .where(functions.col("count") != 1)
            .count()
        )
        if duplicate_keys:
            raise ValueError(f"propensity dataset contains {duplicate_keys} duplicate keys")
        staged_data, staged_metadata = f"{staging}/data", f"{staging}/metadata"
        stage_parquet(dataset, staged_data)
        metadata = {
            "job": "propensity_dataset",
            "run_id": run_id,
            "created_at": created_at.isoformat(),
            "snapshot_date": dataset_snapshot.isoformat(),
            "feature_cutoffs": [cutoff.isoformat() for cutoff in cutoffs],
            "validation_cutoff": cutoffs[-1].isoformat(),
            "lookback_days": lookback_days,
            "label_window_days": label_window_days,
            "negative_ratio": negative_ratio,
            "random_seed": random_seed,
            "dimensions_snapshot_date": dimensions_snapshot_date.isoformat(),
            "source_purchase_months": source_months,
            "source_gold_run_ids": source_runs,
            "metrics": {
                "splits": metrics,
                "output_rows": dataset.count(),
                "future_positive_pairs_outside_candidates": unmatched_positives,
            },
        }
        write_json_metadata(spark, staged_metadata, metadata)
        replace_hdfs_paths(
            spark,
            [
                (staged_data, gold_data_uri(base, "propensity_dataset", dataset_snapshot)),
                (
                    staged_metadata,
                    gold_metadata_uri(base, "propensity_dataset", dataset_snapshot),
                ),
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
    parser.add_argument(
        "--feature-cutoff", required=True, action="append", type=parse_feature_cutoff
    )
    parser.add_argument("--lookback-days", default=180, type=positive_int)
    parser.add_argument("--label-window-days", default=30, type=positive_int)
    parser.add_argument("--negative-ratio", default=3, type=positive_int)
    parser.add_argument("--random-seed", default=42, type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_propensity_dataset(
        hdfs_base_uri=args.hdfs_base_uri,
        dimensions_snapshot_date=args.dimensions_snapshot_date,
        feature_cutoffs=args.feature_cutoff,
        lookback_days=args.lookback_days,
        label_window_days=args.label_window_days,
        negative_ratio=args.negative_ratio,
        random_seed=args.random_seed,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
