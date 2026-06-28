"""Verify the isolated uplift dataset and its metadata."""

from __future__ import annotations

import argparse
import json

from pyspark.sql import SparkSession


def read_hdfs_json(spark: SparkSession, uri: str) -> dict[str, object]:
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)  # noqa: SLF001
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())  # noqa: SLF001
    stream = filesystem.open(path)
    reader = spark._jvm.java.io.BufferedReader(  # noqa: SLF001
        spark._jvm.java.io.InputStreamReader(stream)  # noqa: SLF001
    )
    try:
        return json.loads(reader.readLine())
    finally:
        reader.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    parser.add_argument("--snapshot-date", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("verify-uplift-dataset").getOrCreate()
    try:
        suffix = f"snapshot_date={args.snapshot_date}"
        root = f"{args.hdfs_base_uri}/gold"
        dataset = spark.read.parquet(f"{root}/uplift_dataset/{suffix}")
        assert dataset.count() == 8
        assert dataset.select("client_id").distinct().count() == 8
        split_counts = {row["dataset_split"]: row["count"] for row in dataset.groupBy(
            "dataset_split"
        ).count().collect()}
        assert split_counts == {"train": 4, "validation": 4}
        assert dataset.groupBy("dataset_split", "treatment_flg", "target").count().count() == 8
        assert dataset.where("source_uplift_run_id != 'uplift-fixture-silver-run'").count() == 0
        metadata = read_hdfs_json(
            spark, f"{root}/metadata/uplift_dataset/{suffix}/_metadata.json"
        )
        assert metadata["validation_ratio"] == 0.2
        assert metadata["metrics"]["output_rows"] == 8
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
