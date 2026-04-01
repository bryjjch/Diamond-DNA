#!/usr/bin/env python3
"""Gold player-year archetype clustering: StandardScaler → PCA → KMeans with K selection."""

from __future__ import annotations

import hashlib
import io
import json
import logging
import math
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import davies_bouldin_score, silhouette_score
from sklearn.preprocessing import StandardScaler

from ..gold.silver_to_gold_preprocessing import ID_COLUMNS
from ..pipeline.lake_paths import (
    gold_archetype_assignments_key,
    gold_archetype_clustering_metadata_key,
    gold_archetype_clustering_model_key,
    gold_player_year_output_key,
)
from ..pipeline.s3_parquet import get_s3_client, read_parquet_from_s3, write_parquet_to_s3

logger = logging.getLogger(__name__)

EXCLUDED_FROM_CLUSTERING = frozenset({"n_pitches_total"})
MIN_SAMPLES_FOR_CLUSTERING = 3


@dataclass(frozen=True)
class ArchetypeClusteringConfig:
    """Hyperparameters for one (role, year) archetype clustering run."""

    pca_variance_threshold: float = 0.90
    max_pca_components: int = 50
    k_min: int = 2
    k_max_cap: int = 25
    random_state: int = 42
    n_init: int = 10


def numeric_feature_columns(df: pd.DataFrame) -> List[str]:
    """Numeric columns suitable for clustering (excludes IDs, volume)."""
    id_set = set(ID_COLUMNS)
    numeric = df.select_dtypes(include=[np.number]).columns.tolist()
    out: List[str] = []
    for c in numeric:
        if c in id_set:
            continue
        if c in EXCLUDED_FROM_CLUSTERING:
            continue
        out.append(c)
    return sorted(out)


def _k_max_heuristic(n_samples: int, k_max_cap: int, k_min: int) -> int:
    """Return an upper bound on the number of clusters to consider for K-means clustering."""
    if n_samples <= k_min:
        return k_min
    h = int(math.floor(math.sqrt(n_samples / 2.0)))
    k_max = min(k_max_cap, max(k_min, h), n_samples - 1)
    return max(k_min, k_max)


def _fit_pca(
    X_scaled: np.ndarray,
    cfg: ArchetypeClusteringConfig,
) -> Tuple[PCA, int, List[float], float]:
    """Fit PCA on scaled features and return PCA object, number of components, explained variance ratio, and total explained variance."""
    n_samples, n_features = X_scaled.shape
    max_k = min(cfg.max_pca_components, n_features, max(1, n_samples - 1))
    if max_k < 1:
        raise ValueError("Cannot run PCA: insufficient samples or features.")
    probe = PCA(n_components=max_k, random_state=cfg.random_state)
    probe.fit(X_scaled)
    ratios = probe.explained_variance_ratio_
    cumsum = np.cumsum(ratios)
    idx = int(np.searchsorted(cumsum, cfg.pca_variance_threshold, side="left"))
    n_keep = min(max(1, idx + 1), max_k)
    pca = PCA(n_components=n_keep, random_state=cfg.random_state)
    pca.fit(X_scaled)
    evr = pca.explained_variance_ratio_.tolist()
    total_var = float(np.sum(pca.explained_variance_ratio_))
    return pca, n_keep, evr, total_var


def _k_sweep(
    X_pca: np.ndarray,
    cfg: ArchetypeClusteringConfig,
) -> Tuple[int, List[Dict[str, Any]], List[KMeans]]:
    """Return best K by maximum silhouette, metric curve, and fitted models per K."""
    n_samples = X_pca.shape[0]
    k_max = _k_max_heuristic(n_samples, cfg.k_max_cap, cfg.k_min)
    curve: List[Dict[str, Any]] = []
    models: List[KMeans] = []
    best_k = cfg.k_min
    best_sil = -1.0
    for k in range(cfg.k_min, k_max + 1):
        km = KMeans(
            n_clusters=k,
            random_state=cfg.random_state,
            n_init=cfg.n_init,
        )
        labels = km.fit_predict(X_pca)
        inertia = float(km.inertia_)
        sil = float("nan")
        if k >= 2 and n_samples > k:
            try:
                sil = float(silhouette_score(X_pca, labels))
            except ValueError:
                sil = float("nan")
        db = float("nan")
        try:
            db = float(davies_bouldin_score(X_pca, labels))
        except ValueError:
            pass
        curve.append(
            {
                "k": k,
                "inertia": inertia,
                "silhouette": sil,
                "davies_bouldin": db,
            }
        )
        models.append(km)
        if not math.isnan(sil) and sil > best_sil:
            best_sil = sil
            best_k = k
    return best_k, curve, models


def fit_archetype_clustering(
    df: pd.DataFrame,
    *,
    role: str,
    year: int,
    config: ArchetypeClusteringConfig,
) -> Tuple[pd.DataFrame, Dict[str, Any], Dict[str, Any]]:
    """
    Fit scaler → PCA → KMeans on one role-year gold frame.

    Returns (assignments_df with cluster_id, metadata dict for JSON, joblib bundle dict).
    """
    if df.empty:
        raise ValueError("Empty dataframe for archetype clustering.")
    feature_cols = numeric_feature_columns(df)
    if not feature_cols:
        raise ValueError("No numeric feature columns after exclusions.")

    X = df[feature_cols].to_numpy(dtype=np.float64, copy=True)
    if np.isnan(X).any():
        raise ValueError("NaN in feature matrix; expected gold-preprocessed inputs.")

    n_samples = X.shape[0]
    if n_samples < MIN_SAMPLES_FOR_CLUSTERING:
        raise ValueError(
            f"Need at least {MIN_SAMPLES_FOR_CLUSTERING} rows for clustering; got {n_samples}."
        )

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    pca, n_comp, evr_list, total_explained = _fit_pca(X_scaled, config)
    X_pca = pca.transform(X_scaled)

    best_k, curve, models = _k_sweep(X_pca, config)
    k_idx = best_k - config.k_min
    kmeans_final = models[k_idx]

    labels = kmeans_final.predict(X_pca)
    out = df.copy()
    out["cluster_id"] = labels.astype(np.int64)

    feature_hash = hashlib.sha256(",".join(feature_cols).encode()).hexdigest()[:16]

    metadata: Dict[str, Any] = {
        "role": role,
        "year": year,
        "n_samples": n_samples,
        "n_features_used": len(feature_cols),
        "feature_columns": feature_cols,
        "feature_columns_sha256_16": feature_hash,
        "excluded_from_clustering": sorted(EXCLUDED_FROM_CLUSTERING),
        "pca_n_components": n_comp,
        "pca_explained_variance_ratio": evr_list,
        "pca_total_explained_variance": total_explained,
        "pca_variance_threshold": config.pca_variance_threshold,
        "chosen_k": best_k,
        "k_selection_rule": "maximize_silhouette_score_on_pca_space",
        "k_sweep_metrics": curve,
        "random_state": config.random_state,
        "n_init": config.n_init,
        "sklearn_version": sklearn.__version__,
        "config": asdict(config),
        "generated_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }

    bundle: Dict[str, Any] = {
        "scaler": scaler,
        "pca": pca,
        "kmeans": kmeans_final,
        "feature_columns": feature_cols,
        "role": role,
        "year": year,
        "chosen_k": best_k,
        "config": config,
    }

    return out, metadata, bundle


def _write_json_to_s3(bucket: str, key: str, payload: Mapping[str, Any]) -> None:
    client = get_s3_client()
    body = json.dumps(payload, indent=2, sort_keys=True, default=str).encode()
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def _write_joblib_to_s3(bundle: Dict[str, Any], bucket: str, key: str) -> None:
    client = get_s3_client()
    buf = io.BytesIO()
    joblib.dump(bundle, buf)
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )


def build_gold_archetype_clustering(
    *,
    bucket: str,
    gold_prefix: str,
    start_year: int,
    end_year: int,
    role_filter: str = "all",
    config: Optional[ArchetypeClusteringConfig] = None,
) -> Dict[str, Any]:
    """
    Read gold preprocessed parquet per role/year, fit clustering, write assignments + model + metadata.
    """
    if start_year > end_year:
        return {
            "status": "error",
            "message": f"start_year ({start_year}) must be <= end_year ({end_year})",
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
        }

    valid_roles = ("batter", "pitcher", "all")
    if role_filter not in valid_roles:
        return {
            "status": "error",
            "message": f"role_filter must be one of {valid_roles}, got '{role_filter}'",
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
        }

    cfg = config or ArchetypeClusteringConfig()
    roles: Sequence[str] = ("batter", "pitcher") if role_filter == "all" else (role_filter,)

    rows_written = 0
    years_written: set[int] = set()
    role_years_processed: List[Dict[str, Any]] = []
    errors: List[str] = []

    for role in roles:
        for year in range(start_year, end_year + 1):
            in_key = gold_player_year_output_key(gold_prefix, role, year)
            df = read_parquet_from_s3(bucket, in_key, missing_key_log="none")
            if df is None or df.empty:
                continue

            if "role" not in df.columns:
                df = df.copy()
                df["role"] = role

            try:
                labeled, metadata, bundle = fit_archetype_clustering(
                    df, role=role, year=year, config=cfg
                )
            except ValueError as exc:
                msg = f"role={role} year={year}: {exc}"
                logger.warning("Skipping archetype clustering: %s", msg)
                errors.append(msg)
                continue

            out_parquet_key = gold_archetype_assignments_key(gold_prefix, role, year)
            write_parquet_to_s3(labeled, bucket, out_parquet_key, log_write=False)

            model_key = gold_archetype_clustering_model_key(gold_prefix, role, year)
            _write_joblib_to_s3(bundle, bucket, model_key)

            meta_key = gold_archetype_clustering_metadata_key(gold_prefix, role, year)
            _write_json_to_s3(bucket, meta_key, metadata)

            n = int(len(labeled))
            rows_written += n
            years_written.add(year)
            role_years_processed.append(
                {"role": role, "year": year, "rows": n, "chosen_k": metadata["chosen_k"]}
            )
            logger.info(
                "Archetype clustering wrote %d rows k=%s for role=%s year=%d to s3://%s/%s",
                n,
                metadata["chosen_k"],
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
            "message": "No clustering outputs written; all role-years skipped or missing gold data.",
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
            "errors": errors,
        }

    sorted_years = sorted(years_written)
    message = (
        f"Archetype clustering wrote {rows_written} rows across years {sorted_years} for roles {list(roles)}"
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
    from ..pipeline.cli import run_gold_archetype_clustering_main

    run_gold_archetype_clustering_main()


def handler(event: dict, context) -> dict:
    from ..pipeline.handlers import gold_archetype_clustering_handler

    return gold_archetype_clustering_handler(event, context)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
