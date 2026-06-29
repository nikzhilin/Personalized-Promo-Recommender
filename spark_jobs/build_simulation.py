"""Build discount-response and economics scenarios from one scored Gold snapshot."""

from __future__ import annotations

import argparse
import json
import uuid
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
from spark_jobs.simulation_config import SimulationConfig, load_simulation_config
from spark_jobs.time_compat import UTC

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def purchase_burst_raw(purchases_7d: float, purchases_30d: float) -> float:
    return max(purchases_7d - (7.0 / 30.0) * purchases_30d, 0.0)


def discount_probability(
    p_base_purchase: float,
    uplift_score: float,
    uplift_multiplier: float,
    recsys_score: float,
    relevance_floor: float,
    relevance_weight: float,
) -> float:
    value = p_base_purchase + max(uplift_score, 0.0) * uplift_multiplier * (
        relevance_floor + relevance_weight * recsys_score
    )
    return min(1.0, max(0.0, value))


def promo_abuse_value(
    promo_sensitivity: float,
    purchase_burst_score: float,
    redeemed_points_share: float,
    uplift_outlier_score: float,
    config: SimulationConfig,
) -> float:
    weights = config.promo_abuse
    value = (
        weights.promo_sensitivity_weight * promo_sensitivity
        + weights.purchase_burst_weight * purchase_burst_score
        + weights.redeemed_points_weight * redeemed_points_share
        + weights.uplift_outlier_weight * uplift_outlier_score
    )
    return min(1.0, max(0.0, value))


def economic_values(
    *,
    probability: float,
    baseline_probability: float,
    unit_price: float,
    margin_rate: float,
    discount: float,
    roi_epsilon: float,
) -> dict[str, float]:
    expected_profit = probability * unit_price * (margin_rate - discount)
    baseline_profit = baseline_probability * unit_price * margin_rate
    expected_discount_cost = probability * unit_price * discount
    incremental_profit = expected_profit - baseline_profit
    return {
        "gross_margin": unit_price * margin_rate,
        "expected_profit": expected_profit,
        "expected_discount_cost": expected_discount_cost,
        "incremental_profit": incremental_profit,
        "roi": incremental_profit / max(expected_discount_cost, roi_epsilon),
    }


def _read_hdfs_json(spark: SparkSession, uri: str) -> dict[str, Any]:
    hdfs = filesystem(spark, uri)
    path = hadoop_path(spark, uri)
    if not hdfs.exists(path):
        raise FileNotFoundError(f"required HDFS metadata does not exist: {uri}")
    stream = hdfs.open(path)
    reader = spark._jvm.java.io.BufferedReader(  # noqa: SLF001
        spark._jvm.java.io.InputStreamReader(stream)  # noqa: SLF001
    )
    try:
        return json.loads(reader.readLine())
    finally:
        reader.close()


def _require_scoring_contract(
    metadata: dict[str, Any],
    *,
    snapshot: date,
    feature_cutoff: datetime,
    lookback_days: int,
    dimensions_snapshot_date: date,
) -> str:
    expected = {
        "snapshot_date": snapshot.isoformat(),
        "feature_cutoff": feature_cutoff.isoformat(),
        "lookback_days": lookback_days,
        "dimensions_snapshot_date": dimensions_snapshot_date.isoformat(),
    }
    mismatched = {
        name: {"expected": value, "actual": metadata.get(name)}
        for name, value in expected.items()
        if metadata.get(name) != value
    }
    if mismatched:
        raise ValueError(f"model scoring metadata mismatch: {mismatched}")
    run_id = metadata.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise ValueError("model scoring metadata is missing run_id")
    return run_id


def _require_single_value(frame: DataFrame, column: str, expected: Any, entity: str) -> None:
    actual = {row[column] for row in frame.select(column).distinct().collect()}
    if actual != {expected}:
        raise ValueError(f"{entity} {column} mismatch: expected {expected}, found {actual}")


def _gold_run_ids(frames: dict[str, DataFrame]) -> dict[str, list[str]]:
    return {
        entity: sorted(
            str(row["feature_run_id"])
            for row in frame.select("feature_run_id").distinct().collect()
        )
        for entity, frame in frames.items()
    }


def _build_client_risk(
    users: DataFrame, uplift_scores: DataFrame, config: SimulationConfig
) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as functions

    clients = uplift_scores.select("client_id", "uplift_score").join(
        users.select(
            "client_id",
            "promo_sensitivity_proxy",
            "redeemed_points_share",
            "purchases_7d",
            "purchases_30d",
        ),
        "client_id",
    )
    clients = clients.withColumns(
        {
            "purchase_burst_raw": functions.greatest(
                functions.col("purchases_7d")
                - functions.lit(7.0 / 30.0) * functions.col("purchases_30d"),
                functions.lit(0.0),
            ),
            "uplift_outlier_raw": functions.greatest(
                functions.col("uplift_score"), functions.lit(0.0)
            ),
            "promo_sensitivity_proxy": functions.coalesce(
                "promo_sensitivity_proxy", functions.lit(0.0)
            ),
            "redeemed_points_share": functions.coalesce(
                "redeemed_points_share", functions.lit(0.0)
            ),
        }
    )
    clients = clients.withColumns(
        {
            "purchase_burst_score": functions.percent_rank().over(
                Window.orderBy("purchase_burst_raw")
            ),
            "uplift_outlier_score": functions.percent_rank().over(
                Window.orderBy("uplift_outlier_raw")
            ),
        }
    )
    weights = config.promo_abuse
    return clients.withColumn(
        "promo_abuse_score",
        functions.least(
            functions.lit(1.0),
            functions.greatest(
                functions.lit(0.0),
                functions.lit(weights.promo_sensitivity_weight)
                * functions.col("promo_sensitivity_proxy")
                + functions.lit(weights.purchase_burst_weight)
                * functions.col("purchase_burst_score")
                + functions.lit(weights.redeemed_points_weight)
                * functions.col("redeemed_points_share")
                + functions.lit(weights.uplift_outlier_weight)
                * functions.col("uplift_outlier_score"),
            ),
        ),
    ).drop("purchases_7d", "purchases_30d", "uplift_score")


def _expand_scenarios(candidates: DataFrame, config: SimulationConfig) -> DataFrame:
    from pyspark.sql import functions as functions

    spark = candidates.sparkSession
    discounts = spark.createDataFrame(
        list(
            zip(
                config.discount_response.grid,
                config.discount_response.uplift_multipliers,
                strict=True,
            )
        ),
        "discount double, uplift_multiplier double",
    )
    expanded = candidates.crossJoin(functions.broadcast(discounts)).where(
        functions.col("is_price_available") | (functions.col("discount") == 0)
    )
    response = config.discount_response
    p_discount = functions.least(
        functions.lit(1.0),
        functions.greatest(
            functions.lit(0.0),
            functions.col("p_base_purchase")
            + functions.greatest(functions.col("uplift_score"), functions.lit(0.0))
            * functions.col("uplift_multiplier")
            * (
                functions.lit(response.relevance_floor)
                + functions.lit(response.relevance_weight) * functions.col("recsys_score")
            ),
        ),
    )
    expanded = expanded.withColumns(
        {
            "p_discount": p_discount,
            "is_discount_eligible": functions.col("is_price_available"),
            "gross_margin": functions.when(
                functions.col("is_price_available"),
                functions.col("unit_price") * functions.col("margin_rate"),
            ),
            "expected_profit": functions.when(
                functions.col("is_price_available"),
                p_discount
                * functions.col("unit_price")
                * (functions.col("margin_rate") - functions.col("discount")),
            ),
            "expected_discount_cost": functions.when(
                functions.col("is_price_available"),
                p_discount * functions.col("unit_price") * functions.col("discount"),
            ),
            "baseline_expected_profit": functions.when(
                functions.col("is_price_available"),
                functions.col("p_base_purchase")
                * functions.col("unit_price")
                * functions.col("margin_rate"),
            ),
        }
    )
    expanded = expanded.withColumn(
        "incremental_profit",
        functions.col("expected_profit") - functions.col("baseline_expected_profit"),
    ).withColumn(
        "roi",
        functions.when(
            functions.col("is_price_available"),
            functions.col("incremental_profit")
            / functions.greatest(
                functions.col("expected_discount_cost"),
                functions.lit(config.economics.roi_epsilon),
            ),
        ),
    )
    return expanded.drop("uplift_multiplier", "is_price_available")


def build_simulation(
    *,
    hdfs_base_uri: str,
    dimensions_snapshot_date: date,
    feature_cutoff: datetime,
    lookback_days: int = 180,
    simulation_config: str = "/workspace/configs/simulation.yaml",
) -> dict[str, Any]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as functions

    config = load_simulation_config(simulation_config)
    base = normalize_hdfs_base_uri(hdfs_base_uri)
    snapshot = feature_cutoff.date()
    run_id = uuid.uuid4().hex
    created_at = datetime.now(UTC).replace(tzinfo=None)
    staging = f"{base}/tmp/simulation/{run_id}"
    backup = f"{base}/tmp/simulation-backup/{run_id}"
    spark = (
        SparkSession.builder.appName(f"build-simulation-{run_id}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    hdfs = filesystem(spark, staging)
    try:
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
        propensity = spark.read.parquet(gold_data_uri(base, "propensity_scores", snapshot))
        uplift = spark.read.parquet(gold_data_uri(base, "uplift_scores", snapshot))
        scoring_metadata = _read_hdfs_json(
            spark, f"{gold_metadata_uri(base, 'model_scores', snapshot)}/_metadata.json"
        )
        scoring_run_id = _require_scoring_contract(
            scoring_metadata,
            snapshot=snapshot,
            feature_cutoff=feature_cutoff,
            lookback_days=lookback_days,
            dimensions_snapshot_date=dimensions_snapshot_date,
        )
        source_gold_run_ids = _gold_run_ids(frames)
        if scoring_metadata.get("source_gold_run_ids") != source_gold_run_ids:
            raise ValueError(
                "model scoring Gold lineage mismatch: "
                f"expected {source_gold_run_ids}, "
                f"found {scoring_metadata.get('source_gold_run_ids')}"
            )
        _require_single_value(propensity, "scoring_run_id", scoring_run_id, "propensity_scores")
        _require_single_value(uplift, "scoring_run_id", scoring_run_id, "uplift_scores")
        _require_single_value(propensity, "feature_cutoff", feature_cutoff, "propensity_scores")
        _require_single_value(uplift, "feature_cutoff", feature_cutoff, "uplift_scores")

        pairs = frames["user_item_features"]
        candidate_count = pairs.count()
        if propensity.count() != candidate_count:
            raise ValueError("propensity scores do not cover all candidate pairs")
        if propensity.groupBy("client_id", "product_id").count().where("count != 1").count():
            raise ValueError("propensity scores contain duplicate candidate keys")
        client_count = pairs.select("client_id").distinct().count()
        if uplift.count() != client_count:
            raise ValueError("uplift scores do not cover all candidate clients")
        if uplift.groupBy("client_id").count().where("count != 1").count():
            raise ValueError("uplift scores contain duplicate client keys")

        client_risk = _build_client_risk(frames["user_features"], uplift, config)
        candidates = (
            pairs.select("client_id", "product_id", "recsys_score", "candidate_sources")
            .join(propensity.select("client_id", "product_id", "p_base_purchase"),
                  ["client_id", "product_id"])
            .join(uplift.select("client_id", "p_control", "p_treatment", "uplift_score"),
                  "client_id")
            .join(
                frames["item_features"].select(
                    "product_id",
                    "level_2",
                    "unit_price",
                    "price_source",
                    "is_price_available",
                    "margin_rate",
                ),
                "product_id",
            )
            .join(client_risk, "client_id")
        ).cache()
        if candidates.count() != candidate_count:
            raise ValueError("simulation joins do not cover all candidate pairs")
        scenarios = _expand_scenarios(candidates, config).withColumns(
            {
                "simulation_run_id": functions.lit(run_id),
                "simulation_version": functions.lit(config.version),
                "is_synthetic": functions.lit(True),
                "simulated_at": functions.lit(created_at).cast("timestamp"),
                "feature_cutoff": functions.lit(feature_cutoff).cast("timestamp"),
            }
        ).cache()
        price_available_count = candidates.where("is_price_available").count()
        missing_price_count = candidate_count - price_available_count
        expected_rows = (
            price_available_count * len(config.discount_response.grid) + missing_price_count
        )
        output_rows = scenarios.count()
        if output_rows != expected_rows:
            raise ValueError(
                f"simulation row count mismatch: expected {expected_rows}, produced {output_rows}"
            )
        if scenarios.groupBy("client_id", "product_id", "discount").count().where(
            "count != 1"
        ).count():
            raise ValueError("simulation contains duplicate scenario keys")

        staged_data = f"{staging}/data"
        staged_metadata = f"{staging}/metadata"
        stage_parquet(scenarios, staged_data)
        metadata = {
            "job": "build_simulation",
            "run_id": run_id,
            "created_at": created_at.isoformat(),
            "snapshot_date": snapshot.isoformat(),
            "feature_cutoff": feature_cutoff.isoformat(),
            "lookback_days": lookback_days,
            "dimensions_snapshot_date": dimensions_snapshot_date.isoformat(),
            "simulation_config": config.model_dump(mode="json"),
            "source_scoring_run_id": scoring_run_id,
            "source_gold_run_ids": source_gold_run_ids,
            "metrics": {
                "candidate_pairs": candidate_count,
                "candidates_with_price": price_available_count,
                "candidates_without_price": missing_price_count,
                "output_rows": output_rows,
                "discount_grid_size": len(config.discount_response.grid),
                "mean_promo_abuse_score": scenarios.select(
                    "client_id", "promo_abuse_score"
                ).distinct().agg(functions.avg("promo_abuse_score")).first()[0],
            },
        }
        write_json_metadata(spark, staged_metadata, metadata)
        replace_hdfs_paths(
            spark,
            [
                (staged_data, gold_data_uri(base, "simulation_candidates", snapshot)),
                (staged_metadata, gold_metadata_uri(base, "simulation_candidates", snapshot)),
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
    parser.add_argument("--simulation-config", default="/workspace/configs/simulation.yaml")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_simulation(
        hdfs_base_uri=args.hdfs_base_uri,
        dimensions_snapshot_date=args.dimensions_snapshot_date,
        feature_cutoff=args.feature_cutoff,
        lookback_days=args.lookback_days,
        simulation_config=args.simulation_config,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
