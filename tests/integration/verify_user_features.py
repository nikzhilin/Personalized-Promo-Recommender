"""Verify cutoff-safe Gold user features and publication metadata."""

from __future__ import annotations

import argparse
import json
import math

from pyspark.sql import SparkSession


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    parser.add_argument("--snapshot-date", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("verify-user-features").getOrCreate()
    try:
        root = (
            f"{args.hdfs_base_uri}/gold/user_features/"
            f"snapshot_date={args.snapshot_date}"
        )
        features = spark.read.parquet(root)
        assert features.count() == 3
        rows = {row["client_id"]: row.asDict() for row in features.collect()}

        client_1 = rows["client-1"]
        assert client_1["total_transactions"] == 5
        assert client_1["total_items"] == 10.0
        assert client_1["total_spent"] == 280.0
        assert client_1["avg_check"] == 56.0
        assert client_1["days_since_last_purchase"] == 7
        assert client_1["purchases_7d"] == 1
        assert client_1["purchases_30d"] == 2
        assert client_1["purchases_90d"] == 5
        assert client_1["favorite_category_l2"] == "l2-a"
        assert client_1["category_diversity"] == 2
        assert math.isclose(client_1["avg_item_price"], 355.0 / 9.0)
        assert client_1["purchase_frequency_30d"] == 1.0
        assert client_1["promo_sensitivity_proxy"] == 0.0

        client_2 = rows["client-2"]
        assert client_2["total_transactions"] == 1
        assert client_2["promo_sensitivity_proxy"] == 1.0

        cold_start = rows["client-3"]
        assert cold_start["total_transactions"] == 0
        assert cold_start["total_items"] == 0.0
        assert cold_start["total_spent"] == 0.0
        assert cold_start["purchases_7d"] == 0
        assert cold_start["avg_check"] is None
        assert cold_start["days_since_last_purchase"] is None
        assert cold_start["favorite_category_l2"] is None
        assert cold_start["avg_item_price"] is None
        assert cold_start["purchase_frequency_30d"] is None
        assert cold_start["promo_sensitivity_proxy"] is None
        assert cold_start["age"] == -5.0

        assert all(
            row["feature_cutoff"].isoformat() == "2019-03-01T00:00:00"
            for row in rows.values()
        )
        assert all(row["lookback_days"] == 180 for row in rows.values())
        assert len({row["feature_run_id"] for row in rows.values()}) == 1

        metadata_path = (
            f"{args.hdfs_base_uri}/gold/metadata/user_features/"
            f"snapshot_date={args.snapshot_date}/_metadata.json"
        )
        metadata = json.loads(spark.read.text(metadata_path).first()["value"])
        assert metadata["source_purchase_months"] == [
            "2018-12",
            "2019-01",
            "2019-02",
        ]
        metrics = metadata["metrics"]
        assert metrics["output_users"] == 3
        assert metrics["cold_start_users"] == 1
        assert metrics["inconsistent_receipts"] == 1
        assert metrics["ineligible_price_rows"] == 1
        assert metrics["published_rows_on_or_after_cutoff"] == 0
        assert metrics["age_winsorization"]["lower_bound"] == -5.0
        assert metrics["age_winsorization"]["upper_bound"] == 42.0
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
