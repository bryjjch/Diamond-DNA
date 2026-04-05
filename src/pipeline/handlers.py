"""Lambda handlers (thin wrappers around stage logic)."""

from __future__ import annotations

from typing import Any, Dict, Optional

from .runtime import (
    current_utc_year,
    event_or_env_int,
    event_or_env_str,
)
from .settings import PipelineSettings


def silver_to_gold_preprocessing_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ..gold.silver_to_gold_preprocessing import build_silver_to_gold_preprocessing

    cy = current_utc_year()
    cfg = PipelineSettings.from_environ()
    start_year = event_or_env_int(event, "start_year", "START_YEAR", cy - 1)
    end_year = event_or_env_int(event, "end_year", "END_YEAR", cy)
    bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    silver_prefix = event_or_env_str(event, "silver_prefix", "FEATURE_PREFIX", cfg.feature_prefix)
    gold_prefix = event_or_env_str(event, "gold_prefix", "GOLD_PREFIX", cfg.gold_prefix)
    role = event_or_env_str(event, "role", "ROLE", "all")
    corr_raw = event_or_env_str(event, "correlation_threshold", "CORRELATION_THRESHOLD", "0.95")
    nzv_raw = event_or_env_str(
        event,
        "near_zero_variance_unique_ratio",
        "NEAR_ZERO_VARIANCE_UNIQUE_RATIO",
        "0.005",
    )
    correlation_threshold = float(corr_raw)
    near_zero_variance_unique_ratio = float(nzv_raw)

    result = build_silver_to_gold_preprocessing(
        bucket=bucket,
        silver_prefix=silver_prefix,
        gold_prefix=gold_prefix,
        start_year=start_year,
        end_year=end_year,
        role_filter=role,
        correlation_threshold=correlation_threshold,
        near_zero_variance_unique_ratio=near_zero_variance_unique_ratio,
    )
    status_code = 200 if result.get("status") in ("ok", "no_data") else 400
    return {
        "statusCode": status_code,
        "body": result.get("message", ""),
        "details": result,
    }


def gold_archetype_clustering_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ..ml.archetype_clustering import (
        ArchetypeClusteringConfig,
        ArchetypeClusteringConfigsByRole,
        GMM_COVARIANCE_TYPES,
        build_gold_archetype_clustering,
    )

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

    pca_n_raw = str(event_or_env_str(event, "pca_n_components", "PCA_N_COMPONENTS", "")).strip()
    n_clusters_raw = str(
        event_or_env_str(event, "n_clusters", "N_CLUSTERS", "")
    ).strip()
    try:
        base_pca = int(pca_n_raw) if pca_n_raw else None
        base_k = int(n_clusters_raw) if n_clusters_raw else None
    except ValueError:
        return {
            "statusCode": 400,
            "body": "PCA_N_COMPONENTS and N_CLUSTERS must be integers when set.",
            "details": {},
        }
    rs_raw = event_or_env_str(event, "random_state", "RANDOM_STATE", "42")
    n_init_raw = event_or_env_str(event, "n_init", "N_INIT", "10")
    cov_raw = str(
        event_or_env_str(event, "gmm_covariance_type", "GMM_COVARIANCE_TYPE", "full")
    ).strip()
    err = _validate_cov("GMM_COVARIANCE_TYPE", cov_raw)
    if err:
        return err

    p_pca_s = str(
        event_or_env_str(event, "pitcher_pca_n_components", "PITCHER_PCA_N_COMPONENTS", "")
    ).strip()
    p_k_s = str(
        event_or_env_str(event, "pitcher_n_clusters", "PITCHER_N_CLUSTERS", "")
    ).strip()
    p_cov_s = str(
        event_or_env_str(
            event, "pitcher_gmm_covariance_type", "PITCHER_GMM_COVARIANCE_TYPE", ""
        )
    ).strip()
    b_pca_s = str(
        event_or_env_str(event, "batter_pca_n_components", "BATTER_PCA_N_COMPONENTS", "")
    ).strip()
    b_k_s = str(event_or_env_str(event, "batter_n_clusters", "BATTER_N_CLUSTERS", "")).strip()
    b_cov_s = str(
        event_or_env_str(
            event, "batter_gmm_covariance_type", "BATTER_GMM_COVARIANCE_TYPE", ""
        )
    ).strip()

    p_cov = p_cov_s if p_cov_s else cov_raw
    b_cov = b_cov_s if b_cov_s else cov_raw
    err = _validate_cov("PITCHER_GMM_COVARIANCE_TYPE", p_cov)
    if err:
        return err
    err = _validate_cov("BATTER_GMM_COVARIANCE_TYPE", b_cov)
    if err:
        return err

    try:
        p_pca = int(p_pca_s) if p_pca_s else base_pca
        p_k = int(p_k_s) if p_k_s else base_k
        b_pca = int(b_pca_s) if b_pca_s else base_pca
        b_k = int(b_k_s) if b_k_s else base_k
    except ValueError:
        return {
            "statusCode": 400,
            "body": "Pitcher/batter PCA and N_CLUSTERS overrides must be integers when set.",
            "details": {},
        }

    if role in ("pitcher", "all") and (p_pca is None or p_k is None):
        return {
            "statusCode": 400,
            "body": (
                "Pitcher PCA and cluster count required: set PITCHER_PCA_N_COMPONENTS and "
                "PITCHER_N_CLUSTERS, or PCA_N_COMPONENTS and N_CLUSTERS as defaults."
            ),
            "details": {},
        }
    if role in ("batter", "all") and (b_pca is None or b_k is None):
        return {
            "statusCode": 400,
            "body": (
                "Batter PCA and cluster count required: set BATTER_PCA_N_COMPONENTS and "
                "BATTER_N_CLUSTERS, or PCA_N_COMPONENTS and N_CLUSTERS as defaults."
            ),
            "details": {},
        }

    pitcher_cfg = ArchetypeClusteringConfig(
        pca_n_components=p_pca,
        n_clusters=p_k,
        random_state=int(rs_raw),
        n_init=int(n_init_raw),
        covariance_type=p_cov,
    )
    batter_cfg = ArchetypeClusteringConfig(
        pca_n_components=b_pca,
        n_clusters=b_k,
        random_state=int(rs_raw),
        n_init=int(n_init_raw),
        covariance_type=b_cov,
    )

    if role == "pitcher":
        build_kw: Dict[str, Any] = {"config": pitcher_cfg}
    elif role == "batter":
        build_kw = {"config": batter_cfg}
    elif pitcher_cfg == batter_cfg:
        build_kw = {"config": pitcher_cfg}
    else:
        build_kw = {
            "configs_by_role": ArchetypeClusteringConfigsByRole(
                pitcher=pitcher_cfg,
                batter=batter_cfg,
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
