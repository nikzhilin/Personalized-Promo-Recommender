"""Verify Silver fixture data, rejects, lineage, and monthly policies."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as functions


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    parser.add_argument("--snapshot-date", required=True)
    args = parser.parse_args()
    snapshot = f"snapshot_date={args.snapshot_date}"
    spark = SparkSession.builder.appName("verify-silver-fixture").getOrCreate()
    try:
        clients = spark.read.parquet(f"{args.hdfs_base_uri}/silver/clients/{snapshot}")
        products = spark.read.parquet(f"{args.hdfs_base_uri}/silver/products/{snapshot}")
        uplift_train = spark.read.parquet(
            f"{args.hdfs_base_uri}/silver/uplift/train/{snapshot}"
        )
        uplift_test = spark.read.parquet(
            f"{args.hdfs_base_uri}/silver/uplift/test/{snapshot}"
        )
        assert clients.count() == 3
        assert products.count() == 2
        assert uplift_train.count() == 2
        assert uplift_test.count() == 2
        assert clients.where(functions.col("age") == -5).count() == 1
        assert clients.where(functions.col("silver_run_id").isNull()).count() == 0

        client_rejects = spark.read.parquet(
            f"{args.hdfs_base_uri}/silver/rejects/clients/{snapshot}"
        )
        product_rejects = spark.read.parquet(
            f"{args.hdfs_base_uri}/silver/rejects/products/{snapshot}"
        )
        assert client_rejects.count() == 0
        assert product_rejects.count() == 2
        assert product_rejects.where(
            functions.array_contains("reason_codes", "CONFLICTING_PRIMARY_KEY")
        ).count() == 2

        purchases = spark.read.parquet(f"{args.hdfs_base_uri}/silver/purchases")
        assert purchases.count() == 11
        assert purchases.where(~functions.col("is_valid_for_price")).count() == 1
        assert purchases.where(
            (functions.col("transaction_id") == "transaction-1")
            & (functions.col("product_id") == "product-1")
        ).count() == 2

        purchase_rejects = spark.read.parquet(
            f"{args.hdfs_base_uri}/silver/rejects/purchases/{snapshot}"
        )
        assert purchase_rejects.count() == 4
        expected_reasons = {
            "NONPOSITIVE_QUANTITY",
            "UNKNOWN_CLIENT",
            "UNKNOWN_PRODUCT",
            "FUTURE_TRANSACTION",
        }
        actual_reasons = {
            row["reason"]
            for row in purchase_rejects.select(
                functions.explode("reason_codes").alias("reason")
            ).collect()
        }
        assert actual_reasons == expected_reasons
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
