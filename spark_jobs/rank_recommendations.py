"""Rank optimized offers into a diverse per-client HDFS recommendation snapshot."""

from __future__ import annotations

import argparse
import json
import uuid
from collections.abc import Collection
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from spark_jobs.bronze_common import (
    filesystem,
    hadoop_path,
    normalize_hdfs_base_uri,
    replace_hdfs_paths,
)
from spark_jobs.build_user_features import parse_feature_cutoff
from spark_jobs.gold_common import gold_data_uri, gold_metadata_uri
from spark_jobs.ranking_config import RankingConfig, load_ranking_config
from spark_jobs.silver_common import stage_parquet, write_json_metadata
from spark_jobs.time_compat import UTC

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

REASON_CODES = frozenset(
    {
        "HIGH_INCREMENTAL_PROFIT",
        "ORGANIC_PURCHASE_NO_DISCOUNT",
        "CATEGORY_RELEVANCE",
        "REPEAT_PURCHASE",
        "COLD_START_POPULAR",
    }
)


def final_score_value(
    profit_norm: float,
    relevance_norm: float,
    promo_abuse_score: float,
    config: RankingConfig,
) -> float:
    weights = config.weights
    return (
        weights.profit * profit_norm
        + weights.relevance * relevance_norm
        - weights.promo_abuse_penalty * promo_abuse_score
    )


def assign_reason_code(
    *, cold_start: bool, discount: float, candidate_sources: Collection[str]
) -> str:
    sources = set(candidate_sources)
    if cold_start and "global_popular" in sources:
        return "COLD_START_POPULAR"
    if discount > 0:
        return "HIGH_INCREMENTAL_PROFIT"
    if "repeat_purchase" in sources:
        return "REPEAT_PURCHASE"
    if "category_popular" in sources:
        return "CATEGORY_RELEVANCE"
    return "ORGANIC_PURCHASE_NO_DISCOUNT"


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


def _require_optimizer_contract(
    metadata: dict[str, Any], *, snapshot: date, feature_cutoff: datetime
) -> tuple[str, str, str]:
    expected = {
        "snapshot_date": snapshot.isoformat(),
        "feature_cutoff": feature_cutoff.isoformat(),
    }
    mismatched = {
        name: {"expected": value, "actual": metadata.get(name)}
        for name, value in expected.items()
        if metadata.get(name) != value
    }
    if mismatched:
        raise ValueError(f"optimizer metadata mismatch: {mismatched}")
    run_id = metadata.get("run_id")
    simulation_run_id = metadata.get("source_simulation_run_id")
    optimizer_policy = metadata.get("optimizer_policy")
    policy_version = (
        optimizer_policy.get("version") if isinstance(optimizer_policy, dict) else None
    )
    if not isinstance(run_id, str) or not run_id:
        raise ValueError("optimizer metadata is missing run_id")
    if not isinstance(simulation_run_id, str) or not simulation_run_id:
        raise ValueError("optimizer metadata is missing source simulation run_id")
    if not isinstance(policy_version, str) or not policy_version:
        raise ValueError("optimizer metadata is missing policy version")
    return run_id, simulation_run_id, policy_version


def _require_single_value(frame: DataFrame, column: str, expected: Any, entity: str) -> None:
    actual = {row[column] for row in frame.select(column).distinct().collect()}
    if actual != {expected}:
        raise ValueError(f"{entity} {column} mismatch: expected {expected}, found {actual}")


def _require_user_lineage(
    spark: SparkSession,
    base: str,
    snapshot: date,
    simulation_run_id: str,
) -> DataFrame:
    simulation_metadata = _read_hdfs_json(
        spark, f"{gold_metadata_uri(base, 'simulation_candidates', snapshot)}/_metadata.json"
    )
    if simulation_metadata.get("run_id") != simulation_run_id:
        raise ValueError(
            "optimizer references a stale simulation run: "
            f"expected {simulation_run_id}, found {simulation_metadata.get('run_id')}"
        )
    source_runs = simulation_metadata.get("source_gold_run_ids")
    expected_user_runs = source_runs.get("user_features") if isinstance(source_runs, dict) else None
    if not isinstance(expected_user_runs, list) or not expected_user_runs:
        raise ValueError("simulation metadata is missing user feature lineage")
    users = spark.read.parquet(gold_data_uri(base, "user_features", snapshot))
    actual_user_runs = sorted(
        str(row["feature_run_id"])
        for row in users.select("feature_run_id").distinct().collect()
    )
    if actual_user_runs != expected_user_runs:
        raise ValueError(
            "user feature lineage mismatch: "
            f"expected {expected_user_runs}, found {actual_user_runs}"
        )
    return users


def _rank_offers(
    offers: DataFrame,
    users: DataFrame,
    config: RankingConfig,
    *,
    run_id: str,
    created_at: datetime,
) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as functions

    eligible = offers.where(functions.col("expected_profit").isNotNull()).join(
        users.select("client_id", "total_transactions"), "client_id"
    )
    client_window = Window.partitionBy("client_id")
    ranked = eligible.withColumns(
        {
            "profit_norm": functions.percent_rank().over(
                client_window.orderBy(functions.asc("expected_profit"))
            ),
            "relevance_norm": functions.percent_rank().over(
                client_window.orderBy(functions.asc("recsys_score"))
            ),
            "ranking_level_2": functions.coalesce(
                "level_2", functions.lit(config.unknown_level_2)
            ),
        }
    )
    weights = config.weights
    ranked = ranked.withColumn(
        "final_score",
        functions.lit(weights.profit) * functions.col("profit_norm")
        + functions.lit(weights.relevance) * functions.col("relevance_norm")
        - functions.lit(weights.promo_abuse_penalty)
        * functions.col("promo_abuse_score"),
    ).withColumn(
        "reason_code",
        functions.when(
            (functions.col("total_transactions") == 0)
            & functions.array_contains("candidate_sources", "global_popular"),
            "COLD_START_POPULAR",
        )
        .when(functions.col("discount") > 0, "HIGH_INCREMENTAL_PROFIT")
        .when(
            functions.array_contains("candidate_sources", "repeat_purchase"),
            "REPEAT_PURCHASE",
        )
        .when(
            functions.array_contains("candidate_sources", "category_popular"),
            "CATEGORY_RELEVANCE",
        )
        .otherwise("ORGANIC_PURCHASE_NO_DISCOUNT"),
    )
    ordering = [
        functions.desc("final_score"),
        functions.desc("expected_profit"),
        functions.desc("recsys_score"),
        functions.asc("product_id"),
    ]
    category_window = Window.partitionBy("client_id", "ranking_level_2").orderBy(*ordering)
    diverse = ranked.withColumn(
        "_category_rank", functions.row_number().over(category_window)
    ).where(functions.col("_category_rank") <= config.max_items_per_level_2)
    final_window = Window.partitionBy("client_id").orderBy(*ordering)
    return (
        diverse.withColumn("final_rank", functions.row_number().over(final_window))
        .where(functions.col("final_rank") <= config.top_n)
        .withColumns(
            {
                "ranking_run_id": functions.lit(run_id),
                "ranking_policy_version": functions.lit(config.version),
                "ranked_at": functions.lit(created_at).cast("timestamp"),
            }
        )
        .drop("_category_rank", "total_transactions")
    )


def rank_recommendations(
    *,
    hdfs_base_uri: str,
    feature_cutoff: datetime,
    ranking_config: str = "/workspace/configs/ranking.yaml",
) -> dict[str, Any]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as functions

    config = load_ranking_config(ranking_config)
    base = normalize_hdfs_base_uri(hdfs_base_uri)
    snapshot = feature_cutoff.date()
    run_id = uuid.uuid4().hex
    created_at = datetime.now(UTC).replace(tzinfo=None)
    staging = f"{base}/tmp/final-ranking/{run_id}"
    backup = f"{base}/tmp/final-ranking-backup/{run_id}"
    spark = (
        SparkSession.builder.appName(f"rank-recommendations-{run_id}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    hdfs = filesystem(spark, staging)
    try:
        optimizer_metadata = _read_hdfs_json(
            spark, f"{gold_metadata_uri(base, 'optimized_offers', snapshot)}/_metadata.json"
        )
        optimizer_run_id, simulation_run_id, optimizer_policy_version = (
            _require_optimizer_contract(
            optimizer_metadata, snapshot=snapshot, feature_cutoff=feature_cutoff
            )
        )
        offers = spark.read.parquet(gold_data_uri(base, "optimized_offers", snapshot)).cache()
        _require_single_value(offers, "optimizer_run_id", optimizer_run_id, "optimized_offers")
        _require_single_value(
            offers,
            "optimizer_policy_version",
            optimizer_policy_version,
            "optimized_offers",
        )
        _require_single_value(offers, "feature_cutoff", feature_cutoff, "optimized_offers")
        if offers.groupBy("client_id", "product_id").count().where("count != 1").count():
            raise ValueError("optimized offers contain duplicate candidate keys")
        users = _require_user_lineage(spark, base, snapshot, simulation_run_id)
        input_rows = offers.count()
        input_users = offers.select("client_id").distinct().count()
        excluded_missing_profit = offers.where("expected_profit is null").count()
        eligible_rows = input_rows - excluded_missing_profit
        covered_eligible_rows = (
            offers.where("expected_profit is not null")
            .select("client_id", "product_id")
            .join(users.select("client_id"), "client_id")
            .count()
        )
        if covered_eligible_rows != eligible_rows:
            raise ValueError(
                "user feature coverage mismatch: "
                f"expected {eligible_rows}, found {covered_eligible_rows}"
            )
        recommendations = _rank_offers(
            offers, users, config, run_id=run_id, created_at=created_at
        ).cache()
        output_rows = recommendations.count()
        output_users = recommendations.select("client_id").distinct().count()
        if recommendations.groupBy("client_id", "product_id").count().where(
            "count != 1"
        ).count():
            raise ValueError("final recommendations contain duplicate products per client")
        if recommendations.where(functions.col("final_rank") > config.top_n).count():
            raise ValueError("final recommendations exceed configured top_n")
        if recommendations.groupBy("client_id", "ranking_level_2").count().where(
            functions.col("count") > config.max_items_per_level_2
        ).count():
            raise ValueError("final recommendations violate category diversity cap")
        actual_reason_codes = {
            row["reason_code"]
            for row in recommendations.select("reason_code").distinct().collect()
        }
        if not actual_reason_codes <= REASON_CODES:
            raise ValueError(f"unknown final reason codes: {actual_reason_codes - REASON_CODES}")

        staged_data = f"{staging}/data"
        staged_metadata = f"{staging}/metadata"
        stage_parquet(recommendations, staged_data)
        result = {
            "job": "rank_recommendations",
            "run_id": run_id,
            "created_at": created_at.isoformat(),
            "snapshot_date": snapshot.isoformat(),
            "feature_cutoff": feature_cutoff.isoformat(),
            "source_optimizer_run_id": optimizer_run_id,
            "source_simulation_run_id": simulation_run_id,
            "ranking_policy": config.model_dump(mode="json"),
            "metrics": {
                "input_candidates": input_rows,
                "eligible_candidates": eligible_rows,
                "excluded_missing_profit": excluded_missing_profit,
                "output_recommendations": output_rows,
                "input_users": input_users,
                "output_users": output_users,
                "users_without_recommendations": input_users - output_users,
                "average_list_size": output_rows / output_users if output_users else 0.0,
                "reason_code_counts": {
                    row["reason_code"]: int(row["count"])
                    for row in recommendations.groupBy("reason_code").count().collect()
                },
            },
        }
        write_json_metadata(spark, staged_metadata, result)
        replace_hdfs_paths(
            spark,
            [
                (staged_data, gold_data_uri(base, "final_recommendations", snapshot)),
                (
                    staged_metadata,
                    gold_metadata_uri(base, "final_recommendations", snapshot),
                ),
            ],
            backup,
        )
        return result
    finally:
        hdfs.delete(hadoop_path(spark, staging), True)
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    parser.add_argument("--feature-cutoff", required=True, type=parse_feature_cutoff)
    parser.add_argument("--ranking-config", default="/workspace/configs/ranking.yaml")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = rank_recommendations(
        hdfs_base_uri=args.hdfs_base_uri,
        feature_cutoff=args.feature_cutoff,
        ranking_config=args.ranking_config,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
