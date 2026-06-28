"""Verify the current-only HDFS uplift model artifact contract."""

from __future__ import annotations

import argparse
import json

from pyspark.sql import SparkSession


def read_json(spark: SparkSession, uri: str) -> dict[str, object]:
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)  # noqa: SLF001
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())  # noqa: SLF001
    stream = filesystem.open(path)
    reader = spark._jvm.java.io.BufferedReader(  # noqa: SLF001
        spark._jvm.java.io.InputStreamReader(stream)  # noqa: SLF001
    )
    try:
        lines: list[str] = []
        while (line := reader.readLine()) is not None:
            lines.append(line)
        return json.loads("\n".join(lines))
    finally:
        reader.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    parser.add_argument("--dataset-snapshot-date", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("verify-uplift-model").getOrCreate()
    try:
        root = f"{args.hdfs_base_uri}/models/uplift"
        path = spark._jvm.org.apache.hadoop.fs.Path(root)  # noqa: SLF001
        filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())  # noqa: SLF001
        statuses = filesystem.globStatus(  # noqa: SLF001
            spark._jvm.org.apache.hadoop.fs.Path(f"{root}/run_id=*")  # noqa: SLF001
        )
        assert statuses is not None and len(statuses) == 1
        run_uri = statuses[0].getPath().toString()
        names = {
            status.getPath().getName()
            for status in filesystem.listStatus(statuses[0].getPath())
        }
        assert names == {
            "feature_manifest.json",
            "metrics.json",
            "model_control.cbm",
            "model_treatment.cbm",
            "run_metadata.json",
        }
        metadata = read_json(spark, f"{run_uri}/run_metadata.json")
        metrics = read_json(spark, f"{run_uri}/metrics.json")
        assert metadata["dataset_snapshot_date"] == args.dataset_snapshot_date
        assert set(metrics["branches"]) == {"control", "treatment"}
        assert "qini" in metrics["uplift"]
        assert "roc_auc" in metrics["treatment_overlap"]
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
