#!/usr/bin/env python3
"""Gold player-year archetype clustering: RobustScaler → PCA → Gaussian Mixture."""

from __future__ import annotations

import argparse
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

from ...data_pipeline.gold.gold_preprocessing import ID_COLUMNS
from ...common.runtime_helpers import current_utc_year, event_or_env_int, event_or_env_str
from ...common.s3_helpers import get_s3_client, read_parquet_from_s3, write_parquet_to_s3
from ...common.settings import PipelineSettings

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
    "batter": {"pca_n_components": 4, "n_clusters": 5},
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
            "All other column selection for clustering is done in data_pipeline.gold.gold_preprocessing (archetype-training drop pass and role-irrelevant drop pass)",
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

    labels_key = f"{gold_prefix.strip('/')}/{role}/year={year}/cluster_labels.json"
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
            in_key = (
                f"{gold_prefix.strip('/')}/{role}/year={year}"
                "/player_year_features_preprocessed.parquet"
            )
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
            out_parquet_key = (
                f"{gold_prefix.strip('/')}/{role}/year={year}/player_year_archetypes.parquet"
            )
            write_parquet_to_s3(labeled, bucket, out_parquet_key, log_write=False)

            # Write the archetype clustering model to S3.
            model_key = f"{gold_prefix.strip('/')}/{role}/year={year}/archetype_clustering.joblib"
            _write_joblib_to_s3(bundle, bucket, model_key)

            # Write the archetype clustering metadata to S3.
            meta_key = (
                f"{gold_prefix.strip('/')}/{role}/year={year}/archetype_clustering_metadata.json"
            )
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
    def _parse_bic_k_range(raw: Optional[str]) -> Optional[Tuple[int, int]]:
        """Parse ``"k_min:k_max"`` into a tuple, or return None when empty."""
        if raw is None:
            return None
        s = str(raw).strip()
        if not s:
            return None
        if ":" not in s:
            raise argparse.ArgumentTypeError(
                f"bic-k-range must be 'k_min:k_max'; got {raw!r}."
            )
        k_lo_s, k_hi_s = s.split(":", 1)
        try:
            return int(k_lo_s), int(k_hi_s)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(str(exc)) from exc

    cfg = PipelineSettings.from_environ()
    cy = current_utc_year()
    parser = argparse.ArgumentParser(
        description=(
            "Fit archetype clustering (RobustScaler, PCA, GaussianMixture) on gold "
            "preprocessed player-year tables and write assignments + model artifacts to S3. "
            "PCA dimensionality may be fixed (--pca-n-components) or variance-target "
            "(--pca-variance-target). Cluster count may be fixed (--n-clusters) or BIC-selected "
            "(--bic-k-range k_min:k_max)."
        )
    )
    parser.add_argument("--start-year", type=int, default=cy - 1)
    parser.add_argument("--end-year", type=int, default=cy)
    parser.add_argument("--bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument("--gold-prefix", type=str, default=cfg.gold_prefix)
    parser.add_argument(
        "--role",
        choices=("all", "batter", "pitcher", "catcher"),
        default="all",
        help="Run clustering for all cohorts or one specific role.",
    )
    # Defaults applied to every role unless overridden.
    parser.add_argument("--pca-n-components", type=int, default=None)
    parser.add_argument(
        "--pca-variance-target",
        type=float,
        default=None,
        help="Smallest n_components such that cumulative variance >= target (in (0, 1]).",
    )
    parser.add_argument("--n-clusters", type=int, default=None)
    parser.add_argument(
        "--bic-k-range",
        type=str,
        default=None,
        help="Sweep range 'k_min:k_max' (inclusive); pick k minimizing GMM BIC.",
    )
    parser.add_argument(
        "--gmm-covariance-type",
        choices=GMM_COVARIANCE_TYPES,
        default="full",
        help="GaussianMixture covariance_type (default: full).",
    )
    for role in ("pitcher", "batter", "catcher"):
        parser.add_argument(f"--{role}-pca-n-components", type=int, default=None)
        parser.add_argument(f"--{role}-pca-variance-target", type=float, default=None)
        parser.add_argument(f"--{role}-n-clusters", type=int, default=None)
        parser.add_argument(f"--{role}-bic-k-range", type=str, default=None)
        parser.add_argument(
            f"--{role}-gmm-covariance-type",
            choices=GMM_COVARIANCE_TYPES,
            default=None,
        )
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--n-init", type=int, default=10)
    args = parser.parse_args()

    base_cov = args.gmm_covariance_type
    base_pca = args.pca_n_components
    base_var = args.pca_variance_target
    base_k = args.n_clusters
    base_bic = _parse_bic_k_range(args.bic_k_range)

    def _resolve(role: str) -> ArchetypeClusteringConfig:
        ns_pca = getattr(args, f"{role}_pca_n_components")
        ns_var = getattr(args, f"{role}_pca_variance_target")
        ns_k = getattr(args, f"{role}_n_clusters")
        ns_bic_raw = getattr(args, f"{role}_bic_k_range")
        ns_cov = getattr(args, f"{role}_gmm_covariance_type")
        # If the role provides EITHER PCA flag, use only those; else fall back to defaults.
        if ns_pca is not None or ns_var is not None:
            pca_n, pca_var = ns_pca, ns_var
        else:
            pca_n, pca_var = base_pca, base_var
        if ns_k is not None or ns_bic_raw is not None:
            k_n, bic_k = ns_k, _parse_bic_k_range(ns_bic_raw)
        else:
            k_n, bic_k = base_k, base_bic
        role_defaults = DEFAULT_ROLE_HYPERPARAMS[role]
        if pca_n is None and pca_var is None:
            pca_n = role_defaults["pca_n_components"]
        if k_n is None and bic_k is None:
            k_n = role_defaults["n_clusters"]
        if (pca_n is None) == (pca_var is None):
            raise SystemExit(
                f"{role}: provide exactly one of pca_n_components or pca_variance_target."
            )
        if (k_n is None) == (bic_k is None):
            raise SystemExit(
                f"{role}: provide exactly one of n_clusters or bic_k_range."
            )
        return ArchetypeClusteringConfig(
            pca_n_components=pca_n,
            pca_variance_target=pca_var,
            n_clusters=k_n,
            bic_k_range=bic_k,
            random_state=args.random_state,
            n_init=args.n_init,
            covariance_type=ns_cov or base_cov,
        )

    if args.role == "pitcher":
        build_kw: dict = {"config": _resolve("pitcher")}
    elif args.role == "batter":
        build_kw = {"config": _resolve("batter")}
    elif args.role == "catcher":
        build_kw = {"config": _resolve("catcher")}
    else:
        pitcher_cfg = _resolve("pitcher")
        batter_cfg = _resolve("batter")
        catcher_cfg = _resolve("catcher")
        if pitcher_cfg == batter_cfg == catcher_cfg:
            build_kw = {"config": pitcher_cfg}
        else:
            build_kw = {
                "configs_by_role": ArchetypeClusteringConfigsByRole(
                    pitcher=pitcher_cfg,
                    batter=batter_cfg,
                    catcher=catcher_cfg,
                )
            }

    result = build_gold_archetype_clustering(
        bucket=args.bucket,
        gold_prefix=args.gold_prefix,
        start_year=args.start_year,
        end_year=args.end_year,
        role_filter=args.role,
        **build_kw,
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
    def _parse_optional_int(raw: str) -> Optional[int]:
        s = str(raw).strip() if raw is not None else ""
        return int(s) if s else None

    def _parse_optional_float(raw: str) -> Optional[float]:
        s = str(raw).strip() if raw is not None else ""
        return float(s) if s else None

    def _parse_optional_bic_range(raw: str) -> Optional[Tuple[int, int]]:
        s = str(raw).strip() if raw is not None else ""
        if not s:
            return None
        if ":" not in s:
            raise ValueError(f"BIC_K_RANGE must be 'k_min:k_max'; got {raw!r}.")
        lo_s, hi_s = s.split(":", 1)
        try:
            return int(lo_s), int(hi_s)
        except ValueError as exc:
            raise ValueError(f"BIC_K_RANGE values must be integers; got {raw!r}.") from exc

    def _validate_cov(label: str, value: str) -> Optional[Dict[str, Any]]:
        if value not in GMM_COVARIANCE_TYPES:
            return {
                "statusCode": 400,
                "body": (
                    f"{label} must be one of {list(GMM_COVARIANCE_TYPES)}; got {value!r}."
                ),
                "details": {},
            }
        return None

    cy = current_utc_year()
    cfg = PipelineSettings.from_environ()
    start_year = event_or_env_int(event, "start_year", "START_YEAR", cy - 1)
    end_year = event_or_env_int(event, "end_year", "END_YEAR", cy)
    bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    gold_prefix = event_or_env_str(event, "gold_prefix", "GOLD_PREFIX", cfg.gold_prefix)
    role = event_or_env_str(event, "role", "ROLE", "all")

    rs_raw = event_or_env_str(event, "random_state", "RANDOM_STATE", "42")
    n_init_raw = event_or_env_str(event, "n_init", "N_INIT", "10")
    cov_raw = str(
        event_or_env_str(event, "gmm_covariance_type", "GMM_COVARIANCE_TYPE", "full")
    ).strip()
    err = _validate_cov("GMM_COVARIANCE_TYPE", cov_raw)
    if err:
        return err

    try:
        base_pca = _parse_optional_int(
            event_or_env_str(event, "pca_n_components", "PCA_N_COMPONENTS", "")
        )
        base_var = _parse_optional_float(
            event_or_env_str(event, "pca_variance_target", "PCA_VARIANCE_TARGET", "")
        )
        base_k = _parse_optional_int(
            event_or_env_str(event, "n_clusters", "N_CLUSTERS", "")
        )
        base_bic = _parse_optional_bic_range(
            event_or_env_str(event, "bic_k_range", "BIC_K_RANGE", "")
        )
    except ValueError as exc:
        return {"statusCode": 400, "body": str(exc), "details": {}}

    role_configs: Dict[str, ArchetypeClusteringConfig] = {}
    for r in ("pitcher", "batter", "catcher"):
        r_upper = r.upper()
        try:
            r_pca = _parse_optional_int(
                event_or_env_str(event, f"{r}_pca_n_components", f"{r_upper}_PCA_N_COMPONENTS", "")
            )
            r_var = _parse_optional_float(
                event_or_env_str(event, f"{r}_pca_variance_target", f"{r_upper}_PCA_VARIANCE_TARGET", "")
            )
            r_k = _parse_optional_int(
                event_or_env_str(event, f"{r}_n_clusters", f"{r_upper}_N_CLUSTERS", "")
            )
            r_bic = _parse_optional_bic_range(
                event_or_env_str(event, f"{r}_bic_k_range", f"{r_upper}_BIC_K_RANGE", "")
            )
        except ValueError as exc:
            return {"statusCode": 400, "body": f"{r}: {exc}", "details": {}}
        r_cov = str(
            event_or_env_str(
                event,
                f"{r}_gmm_covariance_type",
                f"{r_upper}_GMM_COVARIANCE_TYPE",
                "",
            )
        ).strip() or cov_raw
        err = _validate_cov(f"{r_upper}_GMM_COVARIANCE_TYPE", r_cov)
        if err:
            return err

        pca_n = r_pca if (r_pca is not None or r_var is not None) else base_pca
        pca_var = r_var if (r_pca is not None or r_var is not None) else base_var
        k_n = r_k if (r_k is not None or r_bic is not None) else base_k
        k_bic = r_bic if (r_k is not None or r_bic is not None) else base_bic

        role_defaults = DEFAULT_ROLE_HYPERPARAMS[r]
        if pca_n is None and pca_var is None:
            pca_n = role_defaults["pca_n_components"]
        if k_n is None and k_bic is None:
            k_n = role_defaults["n_clusters"]

        if role in (r, "all"):
            if (pca_n is None) == (pca_var is None):
                return {
                    "statusCode": 400,
                    "body": (
                        f"{r}: provide exactly one of PCA_N_COMPONENTS/PCA_VARIANCE_TARGET "
                        f"(or the {r_upper}_ override)."
                    ),
                    "details": {},
                }
            if (k_n is None) == (k_bic is None):
                return {
                    "statusCode": 400,
                    "body": (
                        f"{r}: provide exactly one of N_CLUSTERS/BIC_K_RANGE "
                        f"(or the {r_upper}_ override)."
                    ),
                    "details": {},
                }
            try:
                role_configs[r] = ArchetypeClusteringConfig(
                    pca_n_components=pca_n,
                    pca_variance_target=pca_var,
                    n_clusters=k_n,
                    bic_k_range=k_bic,
                    random_state=int(rs_raw),
                    n_init=int(n_init_raw),
                    covariance_type=r_cov,
                )
            except ValueError as exc:
                return {"statusCode": 400, "body": f"{r}: {exc}", "details": {}}

    if role in ("pitcher", "batter", "catcher"):
        build_kw: Dict[str, Any] = {"config": role_configs[role]}
    else:
        # role == "all"
        pitcher_cfg = role_configs["pitcher"]
        batter_cfg = role_configs["batter"]
        catcher_cfg = role_configs["catcher"]
        if pitcher_cfg == batter_cfg == catcher_cfg:
            build_kw = {"config": pitcher_cfg}
        else:
            build_kw = {
                "configs_by_role": ArchetypeClusteringConfigsByRole(
                    pitcher=pitcher_cfg,
                    batter=batter_cfg,
                    catcher=catcher_cfg,
                )
            }

    result = build_gold_archetype_clustering(
        bucket=bucket,
        gold_prefix=gold_prefix,
        start_year=start_year,
        end_year=end_year,
        role_filter=role,
        **build_kw,
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
