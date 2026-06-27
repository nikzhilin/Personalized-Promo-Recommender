"""Verify item, candidate, and candidate-only user-item Gold datasets."""

from __future__ import annotations

import argparse
import json
import math

from pyspark.sql import SparkSession


def read_hdfs_json(spark: SparkSession, uri: str) -> dict[str, object]:
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)  # noqa: SLF001
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())  # noqa: SLF001
    stream = filesystem.open(path)
    reader = spark._jvm.java.io.BufferedReader(spark._jvm.java.io.InputStreamReader(stream))  # noqa: SLF001
    try:
        return json.loads(reader.readLine())
    finally:
        reader.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    parser.add_argument("--snapshot-date", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("verify-gold-features").getOrCreate()
    try:
        root = f"{args.hdfs_base_uri}/gold"
        suffix = f"snapshot_date={args.snapshot_date}"
        items = spark.read.parquet(f"{root}/item_features/{suffix}")
        item_rows = {row["product_id"]: row.asDict() for row in items.collect()}
        assert set(item_rows) == {"product-1", "product-2"}
        assert item_rows["product-1"]["median_unit_price"] == 50.0
        assert item_rows["product-1"]["unit_price"] == 50.0
        assert item_rows["product-1"]["price_source"] == "item"
        assert item_rows["product-1"]["is_price_available"] is True
        assert item_rows["product-1"]["transaction_count"] == 5
        assert item_rows["product-1"]["unique_buyers"] == 2
        assert math.isclose(item_rows["product-1"]["repeat_rate"], 0.5)
        assert all(row["margin_rate"] == 0.25 for row in item_rows.values())

        candidates = spark.read.parquet(f"{root}/recsys_candidates/{suffix}")
        assert candidates.count() == 6
        assert candidates.select("client_id").distinct().count() == 3
        assert candidates.groupBy("client_id").count().where("count > 50").count() == 0
        cold = candidates.where("client_id = 'client-3'").collect()
        assert len(cold) == 2
        assert all(row["candidate_sources"] == ["global_popular"] for row in cold)

        pairs = spark.read.parquet(f"{root}/user_item_features/{suffix}")
        assert pairs.count() == candidates.count()
        assert pairs.select("client_id", "product_id").distinct().count() == pairs.count()
        cold_pairs = pairs.where("client_id = 'client-3'").collect()
        assert all(row["bought_before"] == 0 for row in cold_pairs)
        assert all(row["user_category_affinity"] == 0.0 for row in cold_pairs)
        assert all(row["price_vs_user_avg"] is None for row in cold_pairs)

        for entity in ["item_features", "recsys_candidates", "user_item_features"]:
            metadata_path = f"{root}/metadata/{entity}/{suffix}/_metadata.json"
            metadata = read_hdfs_json(spark, metadata_path)
            assert metadata["feature_cutoff"] == "2019-03-01T00:00:00"
            assert metadata["lookback_days"] == 180
            if entity == "item_features":
                assert metadata["simulation_version"] == "1.0"
                assert metadata["price_config"]["lower_quantile"] == 0.01
                assert metadata["price_config"]["upper_quantile"] == 0.99
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
