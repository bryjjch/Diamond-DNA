#!/usr/bin/env python3
"""Gold player-year KNN similarity in PCA space from the archetype clustering bundle."""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.neighbors import NearestNeighbors

from ...common.runtime_helpers import current_utc_year, event_or_env_int, event_or_env_str
from ...common.lake_keys import ARCHETYPES, gold_feature_key, model_key, prediction_key
from ...common.s3_helpers import get_s3_client, read_parquet_from_s3, write_parquet_to_s3
from ...common.settings import PipelineSettings
from ..archetypes.archetype_clustering import (
    ArchetypeClusteringConfig,
    ArchetypeClusteringConfigsByRole,
)

logger = logging.getLogger(__name__)

# archetype_clustering.py is run directly to train models, so its dataclasses get
# pickled under module "__main__" instead of their real dotted path. Alias them onto
# __main__ here so joblib.load can resolve those references from any entry point.
sys.modules["__main__"].ArchetypeClusteringConfig = ArchetypeClusteringConfig
sys.modules["__main__"].ArchetypeClusteringConfigsByRole = ArchetypeClusteringConfigsByRole


def _write_json_to_s3(bucket: str, key: str, payload: Mapping[str, Any]) -> None:
    """Write JSON to S3."""
    client = get_s3_client()
    body = json.dumps(payload, indent=2, sort_keys=True, default=str).encode()
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def load_archetype_clustering_bundle_from_s3(bucket: str, key: str) -> Optional[Dict[str, Any]]:
    """Load ``archetype_clustering.joblib`` from S3, or None if missing."""
    client = get_s3_client()
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
        return joblib.load(io.BytesIO(obj["Body"].read()))
    except client.exceptions.NoSuchKey:
        return None
    except Exception:
        logger.exception("Failed to read joblib from s3://%s/%s", bucket, key)
        raise

def features_pca_from_bundle(df: pd.DataFrame, bundle: Mapping[str, Any]) -> np.ndarray:
    """
    Apply the saved StandardScaler and PCA to gold rows using ``bundle['feature_columns']``.
    """
    feature_cols: List[str] = list(bundle["feature_columns"])
    missing = [c for c in feature_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"Gold frame missing {len(missing)} clustering feature column(s), e.g. {missing[:8]!r}."
        )
    X = df[feature_cols].to_numpy(dtype=np.float64, copy=True)
    if np.isnan(X).any():
        raise ValueError("NaN in feature matrix; expected gold-preprocessed inputs.")
    scaler = bundle["scaler"]
    pca = bundle["pca"]
    return pca.transform(scaler.transform(X))


def build_neighbor_long_table(
    df: pd.DataFrame,
    X_pca: np.ndarray,
    *,
    role: str,
    year: int,
    k_neighbors: int,
    metric: str,
    minkowski_p: int,
    algorithm: str,
) -> pd.DataFrame:
    """
    For each row, up to ``k_neighbors`` nearest others in PCA space (long format).

    When ``n_samples <= 1``, returns an empty frame with the expected columns.
    """
    if "player_name" in df.columns:
        empty_cols = [
            "player_id",
            "player_name",
            "year",
            "role",
            "neighbor_rank",
            "neighbor_player_id",
            "neighbor_player_name",
            "distance",
        ]
    else:
        empty_cols = [
            "player_id",
            "year",
            "role",
            "neighbor_rank",
            "neighbor_player_id",
            "distance",
        ]

    # Get the number of samples in the PCA space.
    n_samples = int(X_pca.shape[0])
    if n_samples < 2:
        return pd.DataFrame(columns=empty_cols)

    # Get the number of neighbors to fit.
    n_fit = min(k_neighbors + 1, n_samples)
    # Set the nearest neighbors parameters.
    nn_kw: Dict[str, Any] = {
        "n_neighbors": n_fit,
        "metric": metric,
        "algorithm": algorithm,
    }
    # Set the Minkowski p if the metric is Minkowski.
    if metric == "minkowski":
        nn_kw["p"] = minkowski_p

    # Fit the nearest neighbors model.
    nn = NearestNeighbors(**nn_kw)
    nn.fit(X_pca)
    # Get the distances and indices of the nearest neighbors.
    distances, indices = nn.kneighbors(X_pca)

    # Get the player IDs and names.
    pid = df["player_id"].to_numpy()
    pname = df["player_name"].to_numpy() if "player_name" in df.columns else None

    # Get the maximum number of other players.
    rows: List[Dict[str, Any]] = []
    max_other = max(0, n_samples - 1)
    # Get the effective number of neighbors.
    k_cap = min(k_neighbors, max_other)

    # Build the neighbor table.
    for i in range(n_samples):
        # Initialize the rank.
        rank = 0
        # Iterate over the nearest neighbors.
        for j, dist in zip(indices[i], distances[i]):
            if j == i:
                continue
            rank += 1
            if rank > k_cap:
                break
            if pname is not None:
                rec = {
                    "player_id": pid[i],
                    "player_name": pname[i],
                    "year": year,
                    "role": role,
                    "neighbor_rank": rank,
                    "neighbor_player_id": pid[j],
                    "neighbor_player_name": pname[j],
                    "distance": float(dist),
                }
            else:
                rec = {
                    "player_id": pid[i],
                    "year": year,
                    "role": role,
                    "neighbor_rank": rank,
                    "neighbor_player_id": pid[j],
                    "distance": float(dist),
                }
            rows.append(rec)

    return pd.DataFrame(rows)


def _similarity_metadata(
    *,
    role: str,
    year: int,
    k_neighbors: int,
    n_samples: int,
    neighbor_row_count: int,
    metric: str,
    minkowski_p: int,
    algorithm: str,
    source_model_key: str,
    neighbors_parquet_key: str,
) -> Dict[str, Any]:
    max_per_player = max(0, n_samples - 1)
    k_effective = min(k_neighbors, max_per_player)
    return {
        "role": role,
        "year": year,
        "similarity_method": "knn_pca_space",
        "k_neighbors_requested": k_neighbors,
        "k_neighbors_effective_cap": k_effective,
        "n_samples": n_samples,
        "neighbor_table_rows": neighbor_row_count,
        "metric": metric,
        "minkowski_p": minkowski_p if metric == "minkowski" else None,
        "algorithm": algorithm,
        "source_archetype_clustering_model_s3_key": source_model_key,
        "neighbor_table_s3_key": neighbors_parquet_key,
        "sklearn_version": sklearn.__version__,
        "generated_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "notes": (
            "Neighbors are other players in the same role-year cohort; "
            "distances are in the PCA space from archetype_clustering.joblib. "
            "When n_samples is small, fewer than k neighbors exist per player."
        ),
    }


@dataclass(frozen=True)
class PlayerSimilarityConfig:
    """Hyperparameters for one similarity run."""

    k_neighbors: int
    metric: str = "minkowski"
    minkowski_p: int = 2
    algorithm: str = "auto"


def build_gold_player_similarity(
    *,
    bucket: str,
    gold_prefix: str,
    predictions_prefix: str,
    models_prefix: str,
    start_year: int,
    end_year: int,
    role_filter: str = "all",
    config: PlayerSimilarityConfig,
) -> Dict[str, Any]:
    """
    Read gold preprocessed parquet + archetype clustering joblib per role/year, write KNN table + metadata.

    Requires ``models/archetypes/{role}/year=Y/model.joblib`` from the archetype clustering
    stage for each partition processed. The neighbor table goes to ``predictions_prefix``;
    run metadata goes to ``models_prefix``.
    """
    if config.k_neighbors < 1:
        return {
            "status": "error",
            "message": f"k_neighbors must be >= 1; got {config.k_neighbors}.",
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
        }

    valid_metrics = ("minkowski", "euclidean", "manhattan", "chebyshev")
    if config.metric not in valid_metrics:
        return {
            "status": "error",
            "message": f"metric must be one of {valid_metrics}; got {config.metric!r}.",
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
        }

    if start_year > end_year:
        return {
            "status": "error",
            "message": f"start_year ({start_year}) must be <= end_year ({end_year})",
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
        }

    valid_roles = ("batter", "pitcher", "catcher", "all")
    if role_filter not in valid_roles:
        return {
            "status": "error",
            "message": f"role_filter must be one of {valid_roles}, got '{role_filter}'",
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
        }

    roles: Sequence[str] = (
        ("batter", "pitcher", "catcher") if role_filter == "all" else (role_filter,)
    )

    # Initialize the counters.
    rows_written = 0
    years_written: set[int] = set()
    role_years_processed: List[Dict[str, Any]] = []
    errors: List[str] = []

    for role in roles:
        for year in range(start_year, end_year + 1):
            # Get the input key for the gold player-year output.
            in_key = gold_feature_key(
                gold_prefix, ARCHETYPES, role, year, "player_year_features_preprocessed.parquet"
            )
            # Get the model key for the archetype clustering model.
            bundle_key = model_key(models_prefix, "archetypes", role, year, "model.joblib")

            # Read the gold player-year output.
            df = read_parquet_from_s3(bucket, in_key, missing_key_log="none")
            # Check if the dataframe is empty.
            if df is None or df.empty:
                continue

            if "role" not in df.columns:
                df = df.copy()
                df["role"] = role
            
            # Load the archetype clustering bundle.
            bundle = load_archetype_clustering_bundle_from_s3(bucket, bundle_key)
            if bundle is None:
                msg = f"role={role} year={year}: missing archetype model at s3://{bucket}/{bundle_key}"
                logger.warning("Skipping player similarity: %s", msg)
                errors.append(msg)
                continue

            # Check if the bundle is missing any required keys.
            missing_bundle_keys = [k for k in ("scaler", "pca", "feature_columns") if k not in bundle]
            if missing_bundle_keys:
                msg = f"role={role} year={year}: bundle missing keys {missing_bundle_keys}"
                logger.warning("Skipping player similarity: %s", msg)
                errors.append(msg)
                continue

            try:
                # Apply the StandardScaler and PCA to the gold dataframe.
                X_pca = features_pca_from_bundle(df, bundle)
            except ValueError as exc:
                msg = f"role={role} year={year}: {exc}"
                logger.warning("Skipping player similarity: %s", msg)
                errors.append(msg)
                continue

            # Build the neighbor table.
            neighbors_df = build_neighbor_long_table(
                df,
                X_pca,
                role=role,
                year=year,
                k_neighbors=config.k_neighbors,
                metric=config.metric,
                minkowski_p=config.minkowski_p,
                algorithm=config.algorithm,
            )

            # Write the neighbor table to S3.
            out_parquet_key = prediction_key(
                predictions_prefix,
                "neighbors",
                role,
                year,
                "player_year_similar_neighbors.parquet",
            )
            write_parquet_to_s3(neighbors_df, bucket, out_parquet_key, log_write=False)

            # Write the metadata to S3.
            meta_key = model_key(models_prefix, "similarity", role, year, "metadata.json")
            meta = _similarity_metadata(
                role=role,
                year=year,
                k_neighbors=config.k_neighbors,
                n_samples=int(len(df)),
                neighbor_row_count=int(len(neighbors_df)),
                metric=config.metric,
                minkowski_p=config.minkowski_p,
                algorithm=config.algorithm,
                source_model_key=bundle_key,
                neighbors_parquet_key=out_parquet_key,
            )
            _write_json_to_s3(bucket, meta_key, meta)

            n = int(len(neighbors_df))
            rows_written += n
            years_written.add(year)
            role_years_processed.append(
                {
                    "role": role,
                    "year": year,
                    "neighbor_rows": n,
                    "n_players": int(len(df)),
                    "k_neighbors_requested": config.k_neighbors,
                }
            )
            logger.info(
                "Player similarity wrote %d neighbor rows for role=%s year=%d to s3://%s/%s",
                n,
                role,
                year,
                bucket,
                out_parquet_key,
            )

    if rows_written == 0 and not errors:
        return {
            "status": "no_data",
            "message": (
                f"No gold feature tables found for roles={list(roles)} years={start_year}..{end_year}"
            ),
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
            "errors": [],
        }

    if rows_written == 0 and errors:
        return {
            "status": "no_data",
            "message": "No similarity outputs written; all role-years skipped or missing inputs.",
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
            "errors": errors,
        }

    sorted_years = sorted(years_written)
    message = (
        f"Player similarity wrote {rows_written} neighbor rows across years {sorted_years} "
        f"for roles {list(roles)}"
    )
    return {
        "status": "ok",
        "message": message,
        "years_written": sorted_years,
        "rows_written": rows_written,
        "role_years_processed": role_years_processed,
        "errors": errors,
    }


def main() -> None:
    cfg = PipelineSettings.from_environ()
    cy = current_utc_year()
    parser = argparse.ArgumentParser(
        description=(
            "Build K-nearest neighbor similarity in PCA space using archetype_clustering.joblib "
            "per gold role/year and write long-form neighbor Parquet + metadata to S3."
        )
    )
    parser.add_argument("--start-year", type=int, default=cy - 1)
    parser.add_argument("--end-year", type=int, default=cy)
    parser.add_argument("--bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument("--gold-prefix", type=str, default=cfg.gold_prefix)
    parser.add_argument("--predictions-prefix", type=str, default=cfg.predictions_prefix)
    parser.add_argument("--models-prefix", type=str, default=cfg.models_prefix)
    parser.add_argument(
        "--role",
        choices=("all", "batter", "pitcher", "catcher"),
        default="all",
        help="Run similarity for all role cohorts or one specific role.",
    )
    parser.add_argument(
        "--k-neighbors",
        type=int,
        default=10,
        help="Number of nearest other players per row (default: 10).",
    )
    parser.add_argument(
        "--metric",
        choices=("minkowski", "euclidean", "manhattan", "chebyshev"),
        default="minkowski",
        help="sklearn NearestNeighbors metric (default: minkowski with p=2 = Euclidean).",
    )
    parser.add_argument(
        "--minkowski-p",
        type=int,
        default=2,
        help="Minkowski p when --metric=minkowski (default: 2).",
    )
    parser.add_argument(
        "--algorithm",
        type=str,
        default="auto",
        help="sklearn NearestNeighbors algorithm (default: auto).",
    )
    args = parser.parse_args()

    sim_cfg = PlayerSimilarityConfig(
        k_neighbors=args.k_neighbors,
        metric=args.metric,
        minkowski_p=args.minkowski_p,
        algorithm=args.algorithm,
    )

    result = build_gold_player_similarity(
        bucket=args.bucket,
        gold_prefix=args.gold_prefix,
        predictions_prefix=args.predictions_prefix,
        models_prefix=args.models_prefix,
        start_year=args.start_year,
        end_year=args.end_year,
        role_filter=args.role,
        config=sim_cfg,
    )

    if result["status"] == "error":
        logger.error(result["message"])
        raise SystemExit(1)
    if result["status"] == "no_data":
        logger.warning(result["message"])
        for err in result.get("errors", []):
            logger.warning(err)
    else:
        logger.info(result["message"])
        for err in result.get("errors", []):
            if err:
                logger.warning("Partial skip: %s", err)


def handler(event: dict, context) -> dict:
    cy = current_utc_year()
    cfg = PipelineSettings.from_environ()
    start_year = event_or_env_int(event, "start_year", "START_YEAR", cy - 1)
    end_year = event_or_env_int(event, "end_year", "END_YEAR", cy)
    bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    gold_prefix = event_or_env_str(event, "gold_prefix", "GOLD_PREFIX", cfg.gold_prefix)
    predictions_prefix = event_or_env_str(
        event, "predictions_prefix", "PREDICTIONS_PREFIX", cfg.predictions_prefix
    )
    models_prefix = event_or_env_str(event, "models_prefix", "MODELS_PREFIX", cfg.models_prefix)
    role = event_or_env_str(event, "role", "ROLE", "all")

    k_raw = str(event_or_env_str(event, "k_neighbors", "K_NEIGHBORS", "10")).strip()
    metric = str(
        event_or_env_str(event, "metric", "METRIC", "minkowski")
    ).strip().lower()
    p_raw = str(event_or_env_str(event, "minkowski_p", "MINKOWSKI_P", "2")).strip()
    algorithm = str(event_or_env_str(event, "algorithm", "ALGORITHM", "auto")).strip()

    valid_metrics = ("minkowski", "euclidean", "manhattan", "chebyshev")
    if metric not in valid_metrics:
        return {
            "statusCode": 400,
            "body": f"METRIC must be one of {list(valid_metrics)}; got {metric!r}.",
            "details": {},
        }

    try:
        k_neighbors = int(k_raw)
        minkowski_p = int(p_raw)
    except ValueError:
        return {
            "statusCode": 400,
            "body": "K_NEIGHBORS and MINKOWSKI_P must be integers when set.",
            "details": {},
        }

    sim_cfg = PlayerSimilarityConfig(
        k_neighbors=k_neighbors,
        metric=metric,
        minkowski_p=minkowski_p,
        algorithm=algorithm,
    )

    result = build_gold_player_similarity(
        bucket=bucket,
        gold_prefix=gold_prefix,
        predictions_prefix=predictions_prefix,
        models_prefix=models_prefix,
        start_year=start_year,
        end_year=end_year,
        role_filter=role,
        config=sim_cfg,
    )
    status_code = 200 if result.get("status") in ("ok", "no_data") else 400
    return {
        "statusCode": status_code,
        "body": result.get("message", ""),
        "details": result,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
