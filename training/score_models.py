"""Score propensity candidates and client uplift into one atomic Gold snapshot."""

from __future__ import annotations

import argparse
import json
import math
import re
import uuid
from collections.abc import Iterator, Sequence
from datetime import date, datetime
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
from training.build_propensity_dataset import _feature_columns

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

MODEL_RUN_PATTERN = re.compile(r"[0-9a-f]{32}")


def model_run_id(value: str) -> str:
    if MODEL_RUN_PATTERN.fullmatch(value) is None:
        raise argparse.ArgumentTypeError("model run id must be 32 lowercase hexadecimal characters")
    return value


def validate_feature_manifest(
    payload: dict[str, Any], model_name: str
) -> tuple[list[str], list[str]]:
    features = payload.get("features")
    categorical = payload.get("categorical_features")
    feature_types = payload.get("feature_types")
    if not isinstance(features, list) or not features or not all(
        isinstance(value, str) and value for value in features
    ):
        raise ValueError(f"{model_name} manifest contains no valid features")
    if len(features) != len(set(features)):
        raise ValueError(f"{model_name} manifest contains duplicate features")
    if not isinstance(categorical, list) or not all(value in features for value in categorical):
        raise ValueError(f"{model_name} manifest has invalid categorical features")
    if not isinstance(feature_types, dict) or set(feature_types) != set(features):
        raise ValueError(f"{model_name} manifest feature_types do not match features")
    return features, categorical


def require_finite_probabilities(values: Sequence[float], name: str) -> None:
    if any(not math.isfinite(value) or not 0 <= value <= 1 for value in values):
        raise ValueError(f"{name} probabilities must be finite and in [0, 1]")


def _read_hdfs_json(spark: SparkSession, uri: str) -> dict[str, Any]:
    hdfs = filesystem(spark, uri)
    path = hadoop_path(spark, uri)
    if not hdfs.exists(path):
        raise FileNotFoundError(f"required HDFS artifact does not exist: {uri}")
    stream = hdfs.open(path)
    reader = spark._jvm.java.io.BufferedReader(  # noqa: SLF001
        spark._jvm.java.io.InputStreamReader(stream)  # noqa: SLF001
    )
    try:
        return json.loads(reader.readLine())
    finally:
        reader.close()


def _require_artifacts(spark: SparkSession, model_root: str, names: Sequence[str]) -> None:
    hdfs = filesystem(spark, model_root)
    missing = [
        name
        for name in names
        if not hdfs.exists(hadoop_path(spark, f"{model_root}/{name}"))
    ]
    if missing:
        raise FileNotFoundError(f"model run is missing artifacts: {', '.join(missing)}")


def _require_schema(frame: DataFrame, manifest: dict[str, Any], model_name: str) -> None:
    features, _ = validate_feature_manifest(manifest, model_name)
    missing = sorted(set(features) - set(frame.columns))
    if missing:
        raise ValueError(f"{model_name} scoring input is missing features: {missing}")
    actual = {field.name: field.dataType.simpleString() for field in frame.schema.fields}
    mismatched = {
        feature: {"expected": manifest["feature_types"][feature], "actual": actual[feature]}
        for feature in features
        if actual[feature] != manifest["feature_types"][feature]
    }
    if mismatched:
        raise ValueError(f"{model_name} scoring feature types mismatch: {mismatched}")


def _prepare_features(frame: Any, features: list[str], categorical: list[str]) -> Any:
    selected = frame.loc[:, features].copy()
    for column in categorical:
        selected[column] = selected[column].astype("string").fillna("__MISSING__")
    return selected


def score_propensity_frame(
    model: Any, frame: Any, features: list[str], categorical: list[str]
) -> Any:
    probabilities = model.predict_proba(_prepare_features(frame, features, categorical))[:, 1]
    values = probabilities.tolist()
    require_finite_probabilities(values, "propensity")
    result = frame.loc[:, ["client_id", "product_id"]].copy()
    result["p_base_purchase"] = values
    return result


def score_uplift_frame(
    control: Any,
    treatment: Any,
    frame: Any,
    features: list[str],
    categorical: list[str],
) -> Any:
    prepared = _prepare_features(frame, features, categorical)
    p_control = control.predict_proba(prepared)[:, 1].tolist()
    p_treatment = treatment.predict_proba(prepared)[:, 1].tolist()
    require_finite_probabilities(p_control, "control")
    require_finite_probabilities(p_treatment, "treatment")
    result = frame.loc[:, ["client_id"]].copy()
    result["p_control"] = p_control
    result["p_treatment"] = p_treatment
    result["uplift_score"] = [
        treatment_value - control_value
        for control_value, treatment_value in zip(p_control, p_treatment, strict=True)
    ]
    return result


def _score_propensity_batches(
    batches: Iterator[Any], features: list[str], categorical: list[str]
) -> Iterator[Any]:
    from catboost import CatBoostClassifier
    from pyspark import SparkFiles

    model = CatBoostClassifier()
    model.load_model(SparkFiles.get("model.cbm"))
    for batch in batches:
        yield score_propensity_frame(model, batch, features, categorical)


def _score_uplift_batches(
    batches: Iterator[Any], features: list[str], categorical: list[str]
) -> Iterator[Any]:
    from catboost import CatBoostClassifier
    from pyspark import SparkFiles

    control = CatBoostClassifier()
    treatment = CatBoostClassifier()
    control.load_model(SparkFiles.get("model_control.cbm"))
    treatment.load_model(SparkFiles.get("model_treatment.cbm"))
    for batch in batches:
        yield score_uplift_frame(control, treatment, batch, features, categorical)


def _assemble_propensity_input(users: DataFrame, items: DataFrame, pairs: DataFrame) -> DataFrame:
    from pyspark.sql import functions as functions

    user_columns = _feature_columns(users, {"client_id"})
    item_columns = _feature_columns(items, {"product_id"})
    pair_columns = _feature_columns(
        pairs, {"client_id", "product_id"}, {"level_2", "median_unit_price"}
    )
    frame = (
        pairs.select("client_id", "product_id", *pair_columns)
        .join(users.select("client_id", *user_columns), "client_id")
        .join(items.select("product_id", *item_columns), "product_id")
    )
    if "candidate_sources" in frame.columns:
        frame = frame.withColumn("candidate_sources", functions.concat_ws("|", "candidate_sources"))
    return frame


def _source_run_ids(frames: dict[str, DataFrame]) -> dict[str, list[str]]:
    return {
        name: sorted(
            str(row["feature_run_id"])
            for row in frame.select("feature_run_id").distinct().collect()
        )
        for name, frame in frames.items()
    }


def score_models(
    *,
    hdfs_base_uri: str,
    dimensions_snapshot_date: date,
    feature_cutoff: datetime,
    propensity_model_run_id: str,
    uplift_model_run_id: str,
    lookback_days: int = 180,
) -> dict[str, Any]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as functions
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    base = normalize_hdfs_base_uri(hdfs_base_uri)
    snapshot = feature_cutoff.date()
    run_id = uuid.uuid4().hex
    created_at = datetime.now(UTC).replace(tzinfo=None)
    staging = f"{base}/tmp/model-scores/{run_id}"
    backup = f"{base}/tmp/model-scores-backup/{run_id}"
    propensity_root = f"{base}/models/propensity/run_id={propensity_model_run_id}"
    uplift_root = f"{base}/models/uplift/run_id={uplift_model_run_id}"
    spark = (
        SparkSession.builder.appName(f"score-models-{run_id}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    hdfs = filesystem(spark, staging)
    try:
        _require_artifacts(
            spark, propensity_root, ("model.cbm", "feature_manifest.json", "run_metadata.json")
        )
        _require_artifacts(
            spark,
            uplift_root,
            (
                "model_control.cbm",
                "model_treatment.cbm",
                "feature_manifest.json",
                "run_metadata.json",
            ),
        )
        propensity_manifest = _read_hdfs_json(spark, f"{propensity_root}/feature_manifest.json")
        uplift_manifest = _read_hdfs_json(spark, f"{uplift_root}/feature_manifest.json")
        propensity_metadata = _read_hdfs_json(spark, f"{propensity_root}/run_metadata.json")
        uplift_metadata = _read_hdfs_json(spark, f"{uplift_root}/run_metadata.json")
        if propensity_metadata.get("run_id") != propensity_model_run_id:
            raise ValueError("propensity model metadata run_id mismatch")
        if uplift_metadata.get("run_id") != uplift_model_run_id:
            raise ValueError("uplift model metadata run_id mismatch")

        frames = {
            entity: spark.read.parquet(gold_data_uri(base, entity, snapshot))
            for entity in ("user_features", "item_features", "user_item_features")
        }
        for entity, frame in frames.items():
            require_gold_contract(
                frame,
                feature_cutoff=feature_cutoff,
                lookback_days=lookback_days,
                dimensions_snapshot_date=dimensions_snapshot_date,
                entity=entity,
            )
        propensity_input = _assemble_propensity_input(
            frames["user_features"], frames["item_features"], frames["user_item_features"]
        ).cache()
        client_ids = frames["user_item_features"].select("client_id").distinct()
        uplift_input = client_ids.join(frames["user_features"], "client_id").cache()
        _require_schema(propensity_input, propensity_manifest, "propensity")
        _require_schema(uplift_input, uplift_manifest, "uplift")

        propensity_features, propensity_categorical = validate_feature_manifest(
            propensity_manifest, "propensity"
        )
        uplift_features, uplift_categorical = validate_feature_manifest(uplift_manifest, "uplift")
        spark.sparkContext.addFile(f"{propensity_root}/model.cbm")
        spark.sparkContext.addFile(f"{uplift_root}/model_control.cbm")
        spark.sparkContext.addFile(f"{uplift_root}/model_treatment.cbm")
        propensity_schema = StructType(
            [
                StructField("client_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("p_base_purchase", DoubleType(), False),
            ]
        )
        uplift_schema = StructType(
            [
                StructField("client_id", StringType(), False),
                StructField("p_control", DoubleType(), False),
                StructField("p_treatment", DoubleType(), False),
                StructField("uplift_score", DoubleType(), False),
            ]
        )
        propensity_scores = propensity_input.select(
            "client_id", "product_id", *propensity_features
        ).mapInPandas(
            lambda batches: _score_propensity_batches(
                batches, propensity_features, propensity_categorical
            ),
            propensity_schema,
        )
        uplift_scores = uplift_input.select("client_id", *uplift_features).mapInPandas(
            lambda batches: _score_uplift_batches(batches, uplift_features, uplift_categorical),
            uplift_schema,
        )
        lineage = {
            "scoring_run_id": functions.lit(run_id),
            "scored_at": functions.lit(created_at).cast("timestamp"),
            "feature_cutoff": functions.lit(feature_cutoff).cast("timestamp"),
        }
        propensity_scores = propensity_scores.withColumns(lineage).cache()
        uplift_scores = uplift_scores.withColumns(lineage).cache()
        candidate_count = frames["user_item_features"].count()
        assembled_candidate_count = propensity_input.count()
        if assembled_candidate_count != candidate_count:
            raise ValueError(
                "propensity feature coverage mismatch: "
                f"expected {candidate_count}, assembled {assembled_candidate_count}"
            )
        client_count = client_ids.count()
        propensity_count = propensity_scores.count()
        uplift_count = uplift_scores.count()
        if propensity_count != candidate_count:
            raise ValueError(
                "propensity coverage mismatch: "
                f"expected {candidate_count}, produced {propensity_count}"
            )
        if uplift_count != client_count:
            raise ValueError(
                f"uplift coverage mismatch: expected {client_count}, produced {uplift_count}"
            )
        propensity_duplicates = (
            propensity_scores.groupBy("client_id", "product_id")
            .count()
            .where("count != 1")
            .count()
        )
        if propensity_duplicates:
            raise ValueError("propensity scores contain duplicate candidate keys")
        if uplift_scores.groupBy("client_id").count().where("count != 1").count():
            raise ValueError("uplift scores contain duplicate client keys")

        propensity_stage = f"{staging}/propensity/data"
        uplift_stage = f"{staging}/uplift/data"
        metadata_stage = f"{staging}/metadata"
        stage_parquet(propensity_scores, propensity_stage)
        stage_parquet(uplift_scores, uplift_stage)
        summary = uplift_scores.agg(
            functions.avg("uplift_score").alias("mean_uplift"),
            functions.avg(
                functions.when(functions.col("uplift_score") < 0, 1.0).otherwise(0.0)
            ).alias("negative_uplift_share"),
        ).first()
        metadata = {
            "job": "score_models",
            "run_id": run_id,
            "created_at": created_at.isoformat(),
            "snapshot_date": snapshot.isoformat(),
            "feature_cutoff": feature_cutoff.isoformat(),
            "lookback_days": lookback_days,
            "dimensions_snapshot_date": dimensions_snapshot_date.isoformat(),
            "model_run_ids": {
                "propensity": propensity_model_run_id,
                "uplift": uplift_model_run_id,
            },
            "source_model_dataset_run_ids": {
                "propensity": propensity_metadata.get("source_dataset_run_id"),
                "uplift": uplift_metadata.get("source_dataset_run_id"),
            },
            "source_gold_run_ids": _source_run_ids(frames),
            "metrics": {
                "candidate_rows": propensity_count,
                "client_rows": uplift_count,
                "mean_propensity": propensity_scores.agg(
                    functions.avg("p_base_purchase")
                ).first()[0],
                "mean_uplift": summary["mean_uplift"],
                "negative_uplift_share": summary["negative_uplift_share"],
            },
        }
        write_json_metadata(spark, metadata_stage, metadata)
        replace_hdfs_paths(
            spark,
            [
                (propensity_stage, gold_data_uri(base, "propensity_scores", snapshot)),
                (uplift_stage, gold_data_uri(base, "uplift_scores", snapshot)),
                (metadata_stage, gold_metadata_uri(base, "model_scores", snapshot)),
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
    parser.add_argument("--feature-cutoff", required=True, type=parse_feature_cutoff)
    parser.add_argument("--lookback-days", default=180, type=positive_int)
    parser.add_argument("--propensity-model-run-id", required=True, type=model_run_id)
    parser.add_argument("--uplift-model-run-id", required=True, type=model_run_id)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = score_models(
        hdfs_base_uri=args.hdfs_base_uri,
        dimensions_snapshot_date=args.dimensions_snapshot_date,
        feature_cutoff=args.feature_cutoff,
        lookback_days=args.lookback_days,
        propensity_model_run_id=args.propensity_model_run_id,
        uplift_model_run_id=args.uplift_model_run_id,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
