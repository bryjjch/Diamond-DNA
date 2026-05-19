#!/usr/bin/env python3
"""Gold player-year archetype clustering: RobustScaler → PCA → Gaussian Mixture."""

from __future__ import annotations

import hashlib
import io
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.decomposition import PCA
from sklearn.mixture import GaussianMixture
from sklearn.metrics import davies_bouldin_score, silhouette_score
from sklearn.preprocessing import RobustScaler

from ..gold.silver_to_gold_preprocessing import ID_COLUMNS
from ..pipeline.s3_interaction import (
    get_s3_client,
    gold_archetype_assignments_key,
    gold_archetype_cluster_labels_key,
    gold_archetype_clustering_metadata_key,
    gold_archetype_clustering_model_key,
    gold_player_year_output_key,
    read_parquet_from_s3,
    write_parquet_to_s3,
)

logger = logging.getLogger(__name__)

EXCLUDED_FROM_CLUSTERING = frozenset({"n_pitches_total"})
MIN_SAMPLES_FOR_CLUSTERING = 3

ARCHETYPE_CLUSTER_INDEX: tuple[str, ...] = (
    "player_id",
    "player_name",
    "year",
    "role",
    "n_pitches_total",
)

VALID_ROLES: Tuple[str, ...] = ("batter", "pitcher", "catcher")

# Production defaults for per-role (pca_n_components, n_clusters), applied by the
# CLI and Lambda handler when the caller does not supply PCA dimensionality or
# cluster count. Tuned in notebooks/clustering_gmm_experimentation.ipynb.
DEFAULT_ROLE_HYPERPARAMS: Mapping[str, Mapping[str, int]] = {
    "pitcher": {"pca_n_components": 13, "n_clusters": 4},
    "batter": {"pca_n_components": 4, "n_clusters": 4},
    "catcher": {"pca_n_components": 4, "n_clusters": 3},
}


def prepare_dataframe_for_archetype_clustering(df: pd.DataFrame) -> pd.DataFrame:
    """
    Move identity / volume columns to the index so they are never used as model features.

    Uses ``ARCHETYPE_CLUSTER_INDEX`` order; any of those columns missing in ``df`` are skipped.

    Call this before ``numeric_feature_columns`` / scaling when fitting or exploring.
    """
    out = df.copy()
    to_index = [c for c in ARCHETYPE_CLUSTER_INDEX if c in out.columns]
    if not to_index:
        return out
    return out.set_index(to_index, drop=True)


GMM_COVARIANCE_TYPES: tuple[str, ...] = ("full", "tied", "diag", "spherical")
_VALID_GMM_COVARIANCE = frozenset(GMM_COVARIANCE_TYPES)


@dataclass(frozen=True)
class ArchetypeClusteringConfig:
    """
    Hyperparameters for one (role, year) archetype run (PCA + GaussianMixture).

    PCA dimensionality is set in one of two ways:
      - ``pca_n_components`` (int): use exactly this many components.
      - ``pca_variance_target`` (float in (0, 1]): keep the smallest number of
        components whose cumulative explained variance ratio reaches this target.
    Exactly one of these must be provided.

    Cluster count is set in one of two ways:
      - ``n_clusters`` (int): fit GMM with exactly this number of components.
      - ``bic_k_range`` (Tuple[int, int]): sweep ``[k_min, k_max]`` inclusive and pick
        the k that minimizes BIC on the PCA-transformed features.
    Exactly one of these must be provided.
    """

    pca_n_components: Optional[int] = None
    n_clusters: Optional[int] = None
    pca_variance_target: Optional[float] = None
    bic_k_range: Optional[Tuple[int, int]] = None
    random_state: int = 42
    n_init: int = 10
    covariance_type: str = "full"

    def __post_init__(self) -> None:
        # Validate exactly-one-of for PCA dimensionality.
        if (self.pca_n_components is None) == (self.pca_variance_target is None):
            raise ValueError(
                "ArchetypeClusteringConfig requires exactly one of pca_n_components or "
                "pca_variance_target."
            )
        if self.pca_variance_target is not None and not (0.0 < self.pca_variance_target <= 1.0):
            raise ValueError(
                f"pca_variance_target must be in (0, 1]; got {self.pca_variance_target!r}."
            )
        if self.pca_n_components is not None and self.pca_n_components < 1:
            raise ValueError(f"pca_n_components must be >= 1; got {self.pca_n_components!r}.")
        # Validate exactly-one-of for cluster count.
        if (self.n_clusters is None) == (self.bic_k_range is None):
            raise ValueError(
                "ArchetypeClusteringConfig requires exactly one of n_clusters or bic_k_range."
            )
        if self.n_clusters is not None and self.n_clusters < 2:
            raise ValueError(f"n_clusters must be >= 2; got {self.n_clusters!r}.")
        if self.bic_k_range is not None:
            k_lo, k_hi = self.bic_k_range
            if k_lo < 2 or k_hi < k_lo:
                raise ValueError(
                    f"bic_k_range must satisfy 2 <= k_min <= k_max; got {self.bic_k_range!r}."
                )


@dataclass(frozen=True)
class ArchetypeClusteringConfigsByRole:
    """
    Separate clustering hyperparameters per role.

    Used when ``role_filter`` is ``all``. ``catcher`` is optional so existing two-role
    callers (pitcher + batter only) continue to work; if catcher rows are present but
    no catcher config is provided, the batter config is reused as a sensible fallback.
    """

    pitcher: ArchetypeClusteringConfig
    batter: ArchetypeClusteringConfig
    catcher: Optional[ArchetypeClusteringConfig] = None


def _config_for_role(
    role: str,
    *,
    default: Optional[ArchetypeClusteringConfig],
    configs_by_role: Optional[ArchetypeClusteringConfigsByRole],
) -> ArchetypeClusteringConfig:
    """Get the clustering configuration for a role."""
    if role not in VALID_ROLES:
        raise ValueError(f"role must be one of {VALID_ROLES}; got {role!r}")
    if configs_by_role is not None:
        if role == "pitcher":
            return configs_by_role.pitcher
        if role == "batter":
            return configs_by_role.batter
        # catcher: prefer explicit config, fall back to batter.
        return configs_by_role.catcher or configs_by_role.batter
    if default is not None:
        return default
    raise ValueError("No clustering config: pass config= or configs_by_role=.")


def numeric_feature_columns(df: pd.DataFrame) -> List[str]:
    """
    Numeric columns used for PCA / mixture model.

    Identity / volume fields are excluded here: ``player_id``, ``player_name``,
    ``year``, ``role``, and ``n_pitches_total`` (moved to the index first via
    ``prepare_dataframe_for_archetype_clustering`` when those columns exist).
    """
    id_set = set(ID_COLUMNS) | set(EXCLUDED_FROM_CLUSTERING)
    numeric = df.select_dtypes(include=[np.number]).columns.tolist()
    return sorted(c for c in numeric if c not in id_set)


def _fit_pca(
    X_scaled: np.ndarray,
    cfg: ArchetypeClusteringConfig,
) -> tuple[PCA, int, List[float], float, str]:
    """
    Fit PCA either by fixed ``cfg.pca_n_components`` or by ``cfg.pca_variance_target``.

    Returns (pca, n_components_kept, explained_variance_ratios, total_explained, mode).
    ``mode`` is ``'fixed'`` or ``'variance_target'`` for the metadata.
    """
    n_samples, n_features = X_scaled.shape
    max_rank = min(n_features, max(1, n_samples - 1))

    if cfg.pca_variance_target is not None:
        # Fit at max rank, then truncate to the smallest n that hits the target.
        full = PCA(n_components=max_rank, random_state=cfg.random_state)
        full.fit(X_scaled)
        cum = np.cumsum(full.explained_variance_ratio_)
        n_keep = int(np.searchsorted(cum, cfg.pca_variance_target) + 1)
        n_keep = min(n_keep, max_rank)
        if n_keep < 1:
            raise ValueError(
                f"PCA produced no components for variance target {cfg.pca_variance_target!r}."
            )
        pca = PCA(n_components=n_keep, random_state=cfg.random_state)
        pca.fit(X_scaled)
        return (
            pca,
            n_keep,
            pca.explained_variance_ratio_.tolist(),
            float(np.sum(pca.explained_variance_ratio_)),
            "variance_target",
        )

    requested = int(cfg.pca_n_components)
    n_keep = min(requested, max_rank)
    if n_keep < 1:
        raise ValueError(
            f"Cannot run PCA: need pca_n_components >= 1 and rank >= 1; "
            f"got pca_n_components={requested!r}, max_rank={max_rank}."
        )
    if requested > max_rank:
        logger.warning(
            "pca_n_components=%s exceeds max_rank=%s; using %s components.",
            requested,
            max_rank,
            n_keep,
        )
    pca = PCA(n_components=n_keep, random_state=cfg.random_state)
    pca.fit(X_scaled)
    return (
        pca,
        n_keep,
        pca.explained_variance_ratio_.tolist(),
        float(np.sum(pca.explained_variance_ratio_)),
        "fixed",
    )


def _select_n_clusters_by_bic(
    X_pca: np.ndarray,
    cfg: ArchetypeClusteringConfig,
) -> Tuple[int, List[Dict[str, float]]]:
    """
    Sweep ``cfg.bic_k_range`` and return the k with the lowest BIC plus the sweep table.

    Caller-supplied ``k_max`` is clamped to ``n_samples - 1``. If clamping removes the
    entire range, raises ValueError.
    """
    assert cfg.bic_k_range is not None
    n_samples = X_pca.shape[0]
    k_lo, k_hi = cfg.bic_k_range
    k_hi = min(k_hi, max(2, n_samples - 1))
    if k_hi < k_lo:
        raise ValueError(
            f"bic_k_range {cfg.bic_k_range!r} has no valid k for n_samples={n_samples}."
        )
    sweep: List[Dict[str, float]] = []
    best_k = k_lo
    best_bic = float("inf")
    for k in range(k_lo, k_hi + 1):
        gmm = GaussianMixture(
            n_components=k,
            covariance_type=cfg.covariance_type,
            random_state=cfg.random_state,
            n_init=cfg.n_init,
        )
        gmm.fit(X_pca)
        bic = float(gmm.bic(X_pca))
        aic = float(gmm.aic(X_pca))
        sweep.append({"k": int(k), "bic": bic, "aic": aic})
        if bic < best_bic:
            best_bic = bic
            best_k = k
    return best_k, sweep


def fit_archetype_clustering(
    df: pd.DataFrame,
    *,
    role: str,
    year: int,
    config: ArchetypeClusteringConfig,
) -> tuple[pd.DataFrame, Dict[str, Any], Dict[str, Any]]:
    """
    Fit RobustScaler -> PCA -> GaussianMixture on one role-year gold frame.

    PCA dimensionality is either fixed (``config.pca_n_components``) or chosen to hit a
    cumulative-variance target (``config.pca_variance_target``). Cluster count is either
    fixed (``config.n_clusters``) or chosen by minimum BIC over ``config.bic_k_range``.

    Returns (assignments_df with cluster_id, metadata dict for JSON, joblib bundle dict).
    """
    if df.empty:
        raise ValueError("Empty dataframe for archetype clustering.")
    if config.covariance_type not in _VALID_GMM_COVARIANCE:
        raise ValueError(
            f"covariance_type must be one of {sorted(_VALID_GMM_COVARIANCE)}; got {config.covariance_type!r}."
        )

    df_work = prepare_dataframe_for_archetype_clustering(df)
    index_cols = [c for c in ARCHETYPE_CLUSTER_INDEX if c in df.columns]
    feature_cols = numeric_feature_columns(df_work)
    if not feature_cols:
        raise ValueError("No numeric feature columns after exclusions.")

    X = df_work[feature_cols].to_numpy(dtype=np.float64, copy=True)
    if np.isnan(X).any():
        raise ValueError("NaN in feature matrix; expected gold-preprocessed inputs.")

    n_samples = X.shape[0]
    if n_samples < MIN_SAMPLES_FOR_CLUSTERING:
        raise ValueError(
            f"Need at least {MIN_SAMPLES_FOR_CLUSTERING} rows for clustering; got {n_samples}."
        )
    if config.n_clusters is not None and config.n_clusters > n_samples:
        raise ValueError(
            f"n_clusters ({config.n_clusters}) cannot exceed n_samples ({n_samples})."
        )

    # RobustScaler uses median / IQR, which is far less sensitive than StandardScaler to
    # the small-sample outlier seasons (rookie call-ups, extreme platoon roles) that
    # were warping clusters before.
    scaler = RobustScaler()
    X_scaled = scaler.fit_transform(X)

    pca, n_comp, evr_list, total_explained, pca_mode = _fit_pca(X_scaled, config)
    X_pca = pca.transform(X_scaled)

    # Pick n_clusters: explicit fixed value, or BIC sweep over the configured range.
    bic_sweep: Optional[List[Dict[str, float]]] = None
    if config.n_clusters is not None:
        n_clusters = int(config.n_clusters)
        n_clusters_mode = "fixed"
    else:
        n_clusters, bic_sweep = _select_n_clusters_by_bic(X_pca, config)
        n_clusters_mode = "bic_selected"

    gmm = GaussianMixture(
        n_components=n_clusters,
        covariance_type=config.covariance_type,
        random_state=config.random_state,
        n_init=config.n_init,
    )
    gmm.fit(X_pca)
    labels = gmm.predict(X_pca)
    probs = gmm.predict_proba(X_pca)
    top2_idx = np.argsort(-probs, axis=1)[:, :2]
    row_idx = np.arange(probs.shape[0])
    prob_primary = probs[row_idx, top2_idx[:, 0]]
    prob_secondary = probs[row_idx, top2_idx[:, 1]]

    sil = float("nan")
    if n_clusters >= 2 and n_samples > n_clusters:
        try:
            sil = float(silhouette_score(X_pca, labels))
        except ValueError:
            pass
    db = float("nan")
    try:
        db = float(davies_bouldin_score(X_pca, labels))
    except ValueError:
        pass
    gmm_aic = float(gmm.aic(X_pca))
    gmm_bic = float(gmm.bic(X_pca))
    gmm_lower_bound = float(gmm.lower_bound_)

    out = df_work.reset_index()
    out["cluster_id"] = labels.astype(np.int64)
    out["cluster_id_secondary"] = top2_idx[:, 1].astype(np.int64)
    out["prob_primary"] = prob_primary.astype(np.float64)
    out["prob_secondary"] = prob_secondary.astype(np.float64)
    for k in range(n_clusters):
        out[f"prob_{k}"] = probs[:, k].astype(np.float64)

    feature_hash = hashlib.sha256(",".join(feature_cols).encode()).hexdigest()[:16]

    metadata: Dict[str, Any] = {
        "role": role,
        "year": year,
        "n_samples": n_samples,
        "n_features_used": len(feature_cols),
        "feature_columns": feature_cols,
        "feature_columns_sha256_16": feature_hash,
        "clustering_index_columns": index_cols,
        "feature_exclusion_rules": [
            "player_id, player_name, year, role, n_pitches_total → not used as PCA/GMM features (index via prepare_dataframe_for_archetype_clustering when present as columns)",
            "All other column selection for clustering is done in silver_to_gold_preprocessing (archetype-training drop pass and role-irrelevant drop pass)",
        ],
        "scaler": "RobustScaler",
        "pca_mode": pca_mode,
        "pca_n_components": n_comp,
        "pca_variance_target": config.pca_variance_target,
        "pca_explained_variance_ratio": evr_list,
        "pca_total_explained_variance": total_explained,
        "clustering_method": "gaussian_mixture",
        "n_clusters_mode": n_clusters_mode,
        "n_clusters": n_clusters,
        "bic_sweep": bic_sweep,
        "gmm_covariance_type": config.covariance_type,
        "gmm_aic": gmm_aic,
        "gmm_bic": gmm_bic,
        "gmm_lower_bound": gmm_lower_bound,
        "silhouette_score": sil,
        "davies_bouldin_score": db,
        "soft_assignment_schema_version": 1,
        "prob_columns": [f"prob_{k}" for k in range(n_clusters)],
        "mean_max_prob": float(prob_primary.mean()),
        "frac_confident_p_ge_0_7": float((prob_primary >= 0.7).mean()),
        "random_state": config.random_state,
        "n_init": config.n_init,
        "sklearn_version": sklearn.__version__,
        "config": asdict(config),
        "generated_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }

    bundle: Dict[str, Any] = {
        "scaler": scaler,
        "pca": pca,
        "gmm": gmm,
        "feature_columns": feature_cols,
        "role": role,
        "year": year,
        "n_clusters": n_clusters,
        "config": config,
    }

    return out, metadata, bundle


def _write_json_to_s3(bucket: str, key: str, payload: Mapping[str, Any]) -> None:
    """Write JSON to S3."""
    client = get_s3_client()
    body = json.dumps(payload, indent=2, sort_keys=True, default=str).encode()
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def _write_joblib_to_s3(bundle: Dict[str, Any], bucket: str, key: str) -> None:
    """Write joblib to S3."""
    client = get_s3_client()
    buf = io.BytesIO()
    joblib.dump(bundle, buf)
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )


def _build_cluster_labels_for_role_year(
    *,
    labeled: pd.DataFrame,
    role: str,
    year: int,
    feature_cols: Sequence[str],
    bucket: str,
    gold_prefix: str,
) -> Dict[str, Any]:
    """Defer the import so the labeling module (and Gemini SDK) is only loaded when needed."""
    from .archetype_labeling import build_cluster_labels

    labels_key = gold_archetype_cluster_labels_key(gold_prefix, role, year)
    return build_cluster_labels(
        labeled,
        role=role,
        year=year,
        feature_cols=feature_cols,
        bucket=bucket,
        key=labels_key,
    )


def build_gold_archetype_clustering(
    *,
    bucket: str,
    gold_prefix: str,
    start_year: int,
    end_year: int,
    role_filter: str = "all",
    config: Optional[ArchetypeClusteringConfig] = None,
    configs_by_role: Optional[ArchetypeClusteringConfigsByRole] = None,
) -> Dict[str, Any]:
    """
    Read gold preprocessed parquet per role/year, fit clustering, write assignments + model + metadata.

    Pass either ``config`` (same hyperparameters for every role processed) or ``configs_by_role``
    (pitcher vs batter). When ``role_filter`` is ``pitcher`` or ``batter``, only that role's
    entry from ``configs_by_role`` is used; the other is ignored.
    """
    # Raise an error if no clustering configuration is provided.
    if config is None and configs_by_role is None:
        return {
            "status": "error",
            "message": (
                "ArchetypeClusteringConfig is required: pass config= (one setting for all roles) "
                "or configs_by_role= (pitcher vs batter), e.g. from "
                "notebooks/clustering_gmm_experimentation.ipynb."
            ),
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
        }
    if config is not None and configs_by_role is not None:
        return {
            "status": "error",
            "message": "Pass only one of config= or configs_by_role=, not both.",
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

    try:
        for r in roles:
            # Get the clustering configuration for the role.
            _config_for_role(r, default=config, configs_by_role=configs_by_role)
    except ValueError as exc:
        return {
            "status": "error",
            "message": str(exc),
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
        }

    rows_written = 0
    years_written: set[int] = set()
    role_years_processed: List[Dict[str, Any]] = []
    errors: List[str] = []

    for role in roles:
        for year in range(start_year, end_year + 1):
            # Read the gold player-year output.
            in_key = gold_player_year_output_key(gold_prefix, role, year)
            df = read_parquet_from_s3(bucket, in_key, missing_key_log="none")
            if df is None or df.empty:
                continue

            # Add role column if not present.
            if "role" not in df.columns:
                df = df.copy()
                df["role"] = role

            try:
                # Get the clustering configuration for the role.
                role_cfg = _config_for_role(
                    role, default=config, configs_by_role=configs_by_role
                )
                # Fit the archetype clustering.
                labeled, metadata, bundle = fit_archetype_clustering(
                    df, role=role, year=year, config=role_cfg
                )
            except ValueError as exc:
                msg = f"role={role} year={year}: {exc}"
                logger.warning("Skipping archetype clustering: %s", msg)
                errors.append(msg)
                continue

            # Write the archetype assignments to S3.
            out_parquet_key = gold_archetype_assignments_key(gold_prefix, role, year)
            write_parquet_to_s3(labeled, bucket, out_parquet_key, log_write=False)

            # Write the archetype clustering model to S3.
            model_key = gold_archetype_clustering_model_key(gold_prefix, role, year)
            _write_joblib_to_s3(bundle, bucket, model_key)

            # Write the archetype clustering metadata to S3.
            meta_key = gold_archetype_clustering_metadata_key(gold_prefix, role, year)
            _write_json_to_s3(bucket, meta_key, metadata)

            # Generate + write the cluster_labels.json sidecar (Gemini). A failure here
            # must not break the parquet/joblib/metadata writes already on disk; the
            # API loader falls back to "Cluster N" when the sidecar is absent.
            try:
                _build_cluster_labels_for_role_year(
                    labeled=labeled,
                    role=role,
                    year=year,
                    feature_cols=metadata["feature_columns"],
                    bucket=bucket,
                    gold_prefix=gold_prefix,
                )
            except Exception as exc:
                msg = f"role={role} year={year}: label generation failed: {exc}"
                logger.warning("Cluster labelling skipped: %s", msg)
                errors.append(msg)

            # Update counters.
            n = int(len(labeled))
            rows_written += n
            years_written.add(year)
            role_years_processed.append(
                {
                    "role": role,
                    "year": year,
                    "rows": n,
                    "n_clusters": metadata["n_clusters"],
                    "pca_n_components": metadata["pca_n_components"],
                }
            )
            logger.info(
                "Archetype clustering wrote %d rows (GMM n_components=%s, PCA dims=%s) for role=%s year=%d to s3://%s/%s",
                n,
                metadata["n_clusters"],
                metadata["pca_n_components"],
                role,
                year,
                bucket,
                out_parquet_key,
            )

    # Raise an error if no data was written and no errors occurred.
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

    # Raise an error if no data was written and errors occurred.
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
