"""Verify the synthetic Bronze snapshot from inside the Spark cluster."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

EXPECTED = {
    "clients": (
        "bronze/clients",
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
        {"client_id": "string", "treatment_flg": "integer", "target": "integer"},
    ),
    "uplift_test": ("bronze/uplift/test", {"client_id": "string"}),
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    parser.add_argument("--ingest-date", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("verify-bronze-fixture").getOrCreate()
    try:
        for dataset, (relative_path, expected_types) in EXPECTED.items():
            uri = f"{args.hdfs_base_uri}/{relative_path}/ingest_date={args.ingest_date}"
            frame = spark.read.parquet(uri)
            assert frame.count() == 2, f"{dataset}: expected exactly two rows"
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
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
