"""Verify the synthetic Bronze snapshot from inside the Spark cluster."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

EXPECTED = {
    "clients": (
        "bronze/clients",
        4,
        {
            "client_id": "string",
            "first_issue_date": "timestamp",
            "first_redeem_date": "timestamp",
            "age": "double",
            "gender": "string",
        },
    ),
    "products": (
        "bronze/products",
        5,
        {
            "product_id": "string",
            "segment_id": "double",
            "netto": "double",
            "is_own_trademark": "integer",
            "is_alcohol": "integer",
        },
    ),
    "uplift_train": (
        "bronze/uplift/train",
        3,
        {"client_id": "string", "treatment_flg": "integer", "target": "integer"},
    ),
    "uplift_test": ("bronze/uplift/test", 3, {"client_id": "string"}),
}

PURCHASE_TYPES = {
    "client_id": "string",
    "transaction_id": "string",
    "transaction_datetime": "timestamp",
    "purchase_sum": "double",
    "product_id": "string",
    "product_quantity": "double",
    "trn_sum_from_iss": "double",
    "trn_sum_from_red": "double",
    "purchase_month": "string",
}
PURCHASE_MONTH_COUNTS = {
    "2018-12": 4,
    "2019-01": 8,
    "2019-02": 1,
    "2019-03": 1,
    "2026-07": 1,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    parser.add_argument("--ingest-date", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("verify-bronze-fixture").getOrCreate()
    try:
        for dataset, (relative_path, expected_rows, expected_types) in EXPECTED.items():
            uri = f"{args.hdfs_base_uri}/{relative_path}/ingest_date={args.ingest_date}"
            frame = spark.read.parquet(uri)
            assert frame.count() == expected_rows, (
                f"{dataset}: expected {expected_rows} rows"
            )
            actual_types = dict(frame.dtypes)
            for field_name, field_type in expected_types.items():
                assert actual_types[field_name] == field_type, (
                    f"{dataset}.{field_name}: expected {field_type}, "
                    f"received {actual_types[field_name]}"
                )
            assert actual_types["ingest_ts"] == "timestamp"
            assert actual_types["source_file"] == "string"
            assert frame.where(frame.ingest_ts.isNull()).count() == 0
            assert frame.select("source_file").distinct().count() == 1

        purchases = spark.read.parquet(f"{args.hdfs_base_uri}/bronze/purchases")
        assert purchases.count() == sum(PURCHASE_MONTH_COUNTS.values())
        purchase_types = dict(purchases.dtypes)
        for field_name, field_type in PURCHASE_TYPES.items():
            assert purchase_types[field_name] == field_type, (
                f"purchases.{field_name}: expected {field_type}, "
                f"received {purchase_types[field_name]}"
            )
        actual_month_counts = {
            row["purchase_month"]: row["count"]
            for row in purchases.groupBy("purchase_month").count().collect()
        }
        assert actual_month_counts == PURCHASE_MONTH_COUNTS
        assert purchases.where(purchases.ingest_ts.isNull()).count() == 0
        assert purchases.select("source_file").distinct().first()["source_file"] == (
            "purchases.csv"
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
