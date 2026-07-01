"""Seed deterministic exported feedback events for the HDFS feature smoke test."""

from __future__ import annotations

import argparse
from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("client_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("shown_discount", DecimalType(5, 4), False),
        StructField("purchase_value", DecimalType(12, 2), True),
        StructField("created_at", TimestampType(), False),
        StructField("received_at", TimestampType(), False),
        StructField("verification_status", StringType(), False),
        StructField("event_fingerprint", StringType(), False),
    ]
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("seed-feedback-events").config(
        "spark.sql.session.timeZone", "UTC"
    ).getOrCreate()
    try:
        rows = [
            (  # click inside 30 days
                "event-1", "client-1", "click", 0.10, None,
                "2019-02-28T10:00:00", "2019-02-28T10:01:00", "VERIFIED",
            ),
            (
                "event-2", "client-1", "cart", 0.05, None,
                "2019-02-20T10:00:00", "2019-02-20T10:01:00", "VERIFIED",
            ),
            (
                "event-3", "client-1", "purchase", 0.10, 80.0,
                "2019-02-15T10:00:00", "2019-02-15T10:01:00", "VERIFIED",
            ),
            (
                "event-4", "client-1", "purchase", 0.00, 40.0,
                "2019-02-10T10:00:00", "2019-02-10T10:01:00", "VERIFIED",
            ),
            (
                "event-5", "client-2", "click", 0.00, None,
                "2019-01-15T10:00:00", "2019-01-15T10:01:00", "VERIFIED",
            ),
            (
                "event-6", "client-2", "purchase", 0.15, 50.0,
                "2019-02-25T10:00:00", "2019-02-25T10:01:00",
                "UNVERIFIED_MISSING_REQUEST",
            ),
            (
                "event-7", "client-2", "click", 0.00, None,
                "2019-02-27T10:00:00", "2019-03-01T00:00:00", "VERIFIED",
            ),
            (
                "event-8", "unknown-client", "click", 0.00, None,
                "2019-02-26T10:00:00", "2019-02-26T10:01:00", "VERIFIED",
            ),
        ]
        normalized = [
            (
                event_id,
                client_id,
                event_type,
                Decimal(str(shown_discount)),
                None if purchase_value is None else Decimal(str(purchase_value)),
                datetime.fromisoformat(created_at),
                datetime.fromisoformat(received_at),
                verification_status,
                event_id.ljust(64, "0"),
            )
            for (
                event_id,
                client_id,
                event_type,
                shown_discount,
                purchase_value,
                created_at,
                received_at,
                verification_status,
            ) in rows
        ]
        from pyspark.sql import functions as functions

        frame = spark.createDataFrame(normalized, schema=SCHEMA).withColumn(
            "event_date", functions.to_date("created_at")
        )
        frame.write.mode("overwrite").partitionBy("event_date").parquet(
            f"{args.hdfs_base_uri}/feedback/events"
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
