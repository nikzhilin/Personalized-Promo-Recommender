"""Seed isolated balanced Silver/Gold inputs for the uplift dataset smoke test."""

from __future__ import annotations

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as functions


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    parser.add_argument("--source-dimensions-snapshot-date", required=True)
    parser.add_argument("--source-feature-snapshot-date", required=True)
    parser.add_argument("--target-dimensions-snapshot-date", required=True)
    parser.add_argument("--target-feature-cutoff", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("seed-uplift-dataset-inputs").getOrCreate()
    try:
        root = args.hdfs_base_uri
        source_users = spark.read.parquet(
            f"{root}/gold/user_features/"
            f"snapshot_date={args.source_feature_snapshot_date}"
        )
        source_uplift = spark.read.parquet(
            f"{root}/silver/uplift/train/"
            f"snapshot_date={args.source_dimensions_snapshot_date}"
        )
        labels = spark.createDataFrame(
            [
                (f"uplift-{treatment}-{target}-{copy}", treatment, target)
                for treatment in (0, 1)
                for target in (0, 1)
                for copy in (1, 2)
            ],
            ["client_id", "treatment_flg", "target"],
        )
        user_template = source_users.orderBy("client_id").limit(1).drop("client_id")
        cutoff = datetime.fromisoformat(args.target_feature_cutoff)
        users = labels.select("client_id").crossJoin(user_template).withColumns(
            {
                "age": (
                    functions.lit(20.0)
                    + functions.pmod(functions.xxhash64("client_id"), functions.lit(40))
                ).cast("double"),
                "total_spent": (
                    functions.pmod(functions.xxhash64("client_id"), functions.lit(1000))
                    / 10.0
                ).cast("double"),
                "feature_cutoff": functions.lit(cutoff).cast("timestamp"),
                "source_dimensions_snapshot_date": functions.lit(
                    args.target_dimensions_snapshot_date
                ),
                "feature_run_id": functions.lit("uplift-fixture-user-run"),
            }
        )
        uplift_template = source_uplift.orderBy("client_id").limit(1).drop(
            "client_id", "treatment_flg", "target"
        )
        uplift = labels.crossJoin(uplift_template).withColumn(
            "silver_run_id", functions.lit("uplift-fixture-silver-run")
        )
        users.write.mode("overwrite").parquet(
            f"{root}/gold/user_features/"
            f"snapshot_date={cutoff.date().isoformat()}"
        )
        uplift.write.mode("overwrite").parquet(
            f"{root}/silver/uplift/train/"
            f"snapshot_date={args.target_dimensions_snapshot_date}"
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
