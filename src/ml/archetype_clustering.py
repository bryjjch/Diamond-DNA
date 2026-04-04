#!/usr/bin/env python3
"""Gold player-year archetype clustering: StandardScaler → PCA → Gaussian Mixture (fixed PCA dims and n_components)."""

from __future__ import annotations

import hashlib
import io
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.decomposition import PCA
from sklearn.mixture import GaussianMixture
from sklearn.metrics import davies_bouldin_score, silhouette_score
from sklearn.preprocessing import StandardScaler

from ..gold.silver_to_gold_preprocessing import ID_COLUMNS
from ..pipeline.s3_interaction import (
    get_s3_client,
    gold_archetype_assignments_key,
    gold_archetype_clustering_metadata_key,
    gold_archetype_clustering_model_key,
    gold_player_year_output_key,
    read_parquet_from_s3,
    write_parquet_to_s3,
)

logger = logging.getLogger(__name__)

EXCLUDED_FROM_CLUSTERING = frozenset({"n_pitches_total"})
MIN_SAMPLES_FOR_CLUSTERING = 3

ARCHETYPE_CLUSTER_INDEX: tuple[str, ...] = ("player_id", "year", "role", "n_pitches_total")

# Display names for cluster_id 0..5 in the canonical six-component GMM archetypes.
ARCHETYPE_CLUSTER_LABELS_BATTER: Dict[int, str] = {
    0: "The Power Slugger",
    1: "The Backstop",
    2: "The Soft-Tossing Utility",
    3: "The Contact Hitter",
    4: "The Elite Glove/Speedster",
    5: "The Cannon Arm/Free-Swinger",
}
ARCHETYPE_CLUSTER_LABELS_PITCHER: Dict[int, str] = {
    0: "The Finesse Artist",
    1: "The Power Starter",
    2: "The Flyballer",
    3: "The Deception Specialist",
    4: "The Groundball Specialist",
    5: "The High-Leverage Power Reliever",
}
ARCHETYPE_CLUSTER_LABELS_BY_ROLE: Dict[str, Dict[int, str]] = {
    "batter": ARCHETYPE_CLUSTER_LABELS_BATTER,
    "pitcher": ARCHETYPE_CLUSTER_LABELS_PITCHER,
}


def archetype_cluster_label(role: str, cluster_id: int) -> str:
    """
    Map ``cluster_id`` to a human-readable archetype name for the standard six-cluster run.

    If ``role`` is not ``batter``/``pitcher``, or ``cluster_id`` is outside the defined table,
    returns a generic label of the form ``Cluster <id>`` (after normalizing ``role`` to lowercase).
    """
    r = role.strip().lower()
    table = ARCHETYPE_CLUSTER_LABELS_BY_ROLE.get(r)
    if table is None:
        return f"Cluster {int(cluster_id)}"
    return table.get(int(cluster_id), f"Cluster {int(cluster_id)}")


# Excluded ``pitch_type_<PT>_share`` columns (arsenal summarized by ``pitch_type_entropy`` only).
PITCH_TYPE_SHARE_CODES_EXCLUDED: frozenset[str] = frozenset(
    {
        "UN",
        "NONE",
        "PO",
        "EP",
        "FA",
        "CS",
        "SC",
        "FO",
        "KN",
        "CH",
        "CU",
        "FC",
        "FF",
        "FS",
        "KC",
        "SI",
        "SL",
        "ST",
        "SV",
    }
)


def _is_column_excluded_from_archetype_features(col: str) -> bool:
    if col.endswith("_was_missing"):
        return True
    if col.startswith("pt_"):
        return True
    if col in ("xwoba_allowed_lhb_mean", "xwoba_allowed_rhb_mean"):
        return True
    for pt in PITCH_TYPE_SHARE_CODES_EXCLUDED:
        if col == f"pitch_type_{pt}_share":
            return True
    return False


def prepare_dataframe_for_archetype_clustering(df: pd.DataFrame) -> pd.DataFrame:
    """
    Move identity / volume columns to the index so they are never used as model features.

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
    """Hyperparameters for one (role, year) archetype run (PCA + GaussianMixture)."""

    pca_n_components: int
    n_clusters: int
    random_state: int = 42
    n_init: int = 10
    covariance_type: str = "full"


@dataclass(frozen=True)
class ArchetypeClusteringConfigsByRole:
    """Separate clustering hyperparameters for pitchers vs batters (used when ``role_filter`` is ``all``)."""

    pitcher: ArchetypeClusteringConfig
    batter: ArchetypeClusteringConfig


def _config_for_role(
    role: str,
    *,
    default: Optional[ArchetypeClusteringConfig],
    configs_by_role: Optional[ArchetypeClusteringConfigsByRole],
) -> ArchetypeClusteringConfig:
    if role not in ("pitcher", "batter"):
        raise ValueError(f"role must be 'pitcher' or 'batter'; got {role!r}")
    if configs_by_role is not None:
        return configs_by_role.pitcher if role == "pitcher" else configs_by_role.batter
    if default is not None:
        return default
    raise ValueError("No clustering config: pass config= or configs_by_role=.")


def numeric_feature_columns(df: pd.DataFrame) -> List[str]:
    """
    Numeric columns used for PCA / mixture model after gold preprocessing.

    Excludes: ``player_id`` / ``year`` / ``role`` / ``n_pitches_total`` (use index via
    ``prepare_dataframe_for_archetype_clustering``), imputation flags ``*_was_missing``,
    all ``pt_*`` pitch-type physics columns, listed ``pitch_type_*_share`` columns (junk +
    core types; entropy retained), and redundant platoon xwoba means.
    """
    id_set = set(ID_COLUMNS) | set(EXCLUDED_FROM_CLUSTERING)
    numeric = df.select_dtypes(include=[np.number]).columns.tolist()
    out: List[str] = []
    for c in numeric:
        if c in id_set:
            continue
        if _is_column_excluded_from_archetype_features(c):
            continue
        out.append(c)
    return sorted(out)


def _fit_pca_fixed(
    X_scaled: np.ndarray,
    cfg: ArchetypeClusteringConfig,
) -> tuple[PCA, int, List[float], float]:
    """Fit PCA with ``cfg.pca_n_components`` (clamped to valid rank)."""
    n_samples, n_features = X_scaled.shape
    max_rank = min(n_features, max(1, n_samples - 1))
    n_keep = min(int(cfg.pca_n_components), max_rank)
    if n_keep < 1:
        raise ValueError(
            f"Cannot run PCA: need pca_n_components >= 1 and rank >= 1; got pca_n_components={cfg.pca_n_components!r}, max_rank={max_rank}."
        )
    if cfg.pca_n_components > max_rank:
        logger.warning(
            "pca_n_components=%s exceeds max_rank=%s; using %s components.",
            cfg.pca_n_components,
            max_rank,
            n_keep,
        )
    pca = PCA(n_components=n_keep, random_state=cfg.random_state)
    pca.fit(X_scaled)
    evr = pca.explained_variance_ratio_.tolist()
    total_var = float(np.sum(pca.explained_variance_ratio_))
    return pca, n_keep, evr, total_var


def fit_archetype_clustering(
    df: pd.DataFrame,
    *,
    role: str,
    year: int,
    config: ArchetypeClusteringConfig,
) -> tuple[pd.DataFrame, Dict[str, Any], Dict[str, Any]]:
    """
    Fit scaler → PCA → GaussianMixture on one role-year gold frame.

    Returns (assignments_df with cluster_id, metadata dict for JSON, joblib bundle dict).
    """
    if df.empty:
        raise ValueError("Empty dataframe for archetype clustering.")
    if config.n_clusters < 2:
        raise ValueError(f"n_clusters must be >= 2; got {config.n_clusters}.")
    if config.pca_n_components < 1:
        raise ValueError(f"pca_n_components must be >= 1; got {config.pca_n_components}.")
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
    if config.n_clusters > n_samples:
        raise ValueError(
            f"n_clusters ({config.n_clusters}) cannot exceed n_samples ({n_samples})."
        )

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    pca, n_comp, evr_list, total_explained = _fit_pca_fixed(X_scaled, config)
    X_pca = pca.transform(X_scaled)

    gmm = GaussianMixture(
        n_components=config.n_clusters,
        covariance_type=config.covariance_type,
        random_state=config.random_state,
        n_init=config.n_init,
    )
    gmm.fit(X_pca)
    labels = gmm.predict(X_pca)

    sil = float("nan")
    if config.n_clusters >= 2 and n_samples > config.n_clusters:
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
            "player_id, year, role, n_pitches_total → index (when present as columns)",
            "columns ending with _was_missing",
            "columns starting with pt_",
            "pitch_type_<PT>_share for PT in PITCH_TYPE_SHARE_CODES_EXCLUDED (incl. core CH/CU/FC/FF/FS/KC/SI/SL/ST/SV + junk types); pitch_type_entropy kept",
            "xwoba_allowed_lhb_mean, xwoba_allowed_rhb_mean",
        ],
        "pca_n_components": n_comp,
        "pca_explained_variance_ratio": evr_list,
        "pca_total_explained_variance": total_explained,
        "clustering_method": "gaussian_mixture",
        "n_clusters": config.n_clusters,
        "gmm_covariance_type": config.covariance_type,
        "gmm_aic": gmm_aic,
        "gmm_bic": gmm_bic,
        "gmm_lower_bound": gmm_lower_bound,
        "silhouette_score": sil,
        "davies_bouldin_score": db,
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
        "n_clusters": config.n_clusters,
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
    configs_by_role: Optional[ArchetypeClusteringConfigsByRole] = None,
) -> Dict[str, Any]:
    """
    Read gold preprocessed parquet per role/year, fit clustering, write assignments + model + metadata.

    Pass either ``config`` (same hyperparameters for every role processed) or ``configs_by_role``
    (pitcher vs batter). When ``role_filter`` is ``pitcher`` or ``batter``, only that role's
    entry from ``configs_by_role`` is used; the other is ignored.
    """
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

    valid_roles = ("batter", "pitcher", "all")
    if role_filter not in valid_roles:
        return {
            "status": "error",
            "message": f"role_filter must be one of {valid_roles}, got '{role_filter}'",
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
        }

    roles: Sequence[str] = ("batter", "pitcher") if role_filter == "all" else (role_filter,)

    try:
        for r in roles:
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
            in_key = gold_player_year_output_key(gold_prefix, role, year)
            df = read_parquet_from_s3(bucket, in_key, missing_key_log="none")
            if df is None or df.empty:
                continue

            if "role" not in df.columns:
                df = df.copy()
                df["role"] = role

            try:
                role_cfg = _config_for_role(
                    role, default=config, configs_by_role=configs_by_role
                )
                labeled, metadata, bundle = fit_archetype_clustering(
                    df, role=role, year=year, config=role_cfg
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
