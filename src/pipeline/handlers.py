"""Lambda handlers (thin wrappers around stage logic)."""

from __future__ import annotations

from typing import Any, Dict, Optional

from .runtime import (
    current_utc_year,
    event_or_env_int,
    event_or_env_str,
    yesterday_utc_date_str,
)
from .settings import PipelineSettings


def _parse_optional_int(raw: str) -> Optional[int]:
    s = str(raw).strip() if raw is not None else ""
    return int(s) if s else None


def _parse_optional_float(raw: str) -> Optional[float]:
    s = str(raw).strip() if raw is not None else ""
    return float(s) if s else None


def _parse_optional_bic_range(raw: str) -> Optional[tuple[int, int]]:
    s = str(raw).strip() if raw is not None else ""
    if not s:
        return None
    if ":" not in s:
        raise ValueError(f"BIC_K_RANGE must be 'k_min:k_max'; got {raw!r}.")
    lo_s, hi_s = s.split(":", 1)
    return int(lo_s), int(hi_s)


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


def gold_player_similarity_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ..ml.player_similarity import PlayerSimilarityConfig, build_gold_player_similarity

    cy = current_utc_year()
    cfg = PipelineSettings.from_environ()
    start_year = event_or_env_int(event, "start_year", "START_YEAR", cy - 1)
    end_year = event_or_env_int(event, "end_year", "END_YEAR", cy)
    bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    gold_prefix = event_or_env_str(event, "gold_prefix", "GOLD_PREFIX", cfg.gold_prefix)
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


def _parse_defence_min_qual_str(s: str, default: str | int) -> str | int:
    raw = str(s).strip() if s is not None else ""
    if raw == "":
        v = default
        if isinstance(v, int):
            return v
        raw = str(v).strip()
    return int(raw) if raw.isdigit() else raw


def defence_ingestion_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ..bronze.defence_ingestion import ingest_year_range

    cy = current_utc_year()
    cfg = PipelineSettings.from_environ()
    start_year = event_or_env_int(event, "start_year", "START_YEAR", cy - 3)
    end_year = event_or_env_int(event, "end_year", "END_YEAR", cy)
    s3_bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    s3_prefix = event_or_env_str(
        event, "s3_prefix", "S3_PREFIX", cfg.raw_defence_prefix
    )

    oaa_s = event_or_env_str(event, "oaa_min_att", "OAA_MIN_ATT", "q")
    framing_s = event_or_env_str(event, "framing_min_called", "FRAMING_MIN_CALLED", "q")
    ev = event if isinstance(event, dict) else {}
    result = ingest_year_range(
        start_year,
        end_year,
        s3_bucket,
        s3_prefix,
        oaa_min_att=_parse_defence_min_qual_str(oaa_s, "q"),
        arm_min_throws=event_or_env_int(event, "arm_min_throws", "ARM_MIN_THROWS", 50),
        framing_min_called=_parse_defence_min_qual_str(framing_s, "q"),
        pop_min_2b=event_or_env_int(event, "pop_min_2b", "POP_MIN_2B", 5),
        pop_min_3b=event_or_env_int(event, "pop_min_3b", "POP_MIN_3B", 0),
        fangraphs_qual=ev.get("fangraphs_qual"),
    )
    status_code = 200 if result["status"] == "ok" else (207 if result["status"] == "partial" else 400)
    return {"statusCode": status_code, **result}


def statcast_ingestion_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ..bronze.statcast_ingestion import ingest_date_range

    y = yesterday_utc_date_str()
    cfg = PipelineSettings.from_environ()
    start_date = event_or_env_str(event, "start_date", "START_DATE", y)
    end_date = event_or_env_str(event, "end_date", "END_DATE", y)
    s3_bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    s3_prefix = event_or_env_str(
        event, "s3_prefix", "S3_PREFIX", cfg.raw_statcast_prefix
    )

    result = ingest_date_range(start_date, end_date, s3_bucket, s3_prefix)

    if result["status"] == "error":
        return {
            "statusCode": 400,
            "body": result["message"],
            "errors": result.get("errors", []),
        }
    if result["status"] == "partial":
        return {
            "statusCode": 207,
            "body": result["message"],
            "total_records": result["total_records"],
            "days_ok": result["days_ok"],
            "days_no_data": result["days_no_data"],
            "days_error": result["days_error"],
            "errors": result.get("errors", []),
        }
    return {
        "statusCode": 200,
        "body": result["message"],
        "total_records": result["total_records"],
        "days_ok": result["days_ok"],
        "days_no_data": result["days_no_data"],
    }


def statcast_running_ingestion_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ..bronze.statcast_running_ingestion import ingest_year_range

    cy = current_utc_year()
    cfg = PipelineSettings.from_environ()
    start_year = event_or_env_int(event, "start_year", "START_YEAR", cy - 3)
    end_year = event_or_env_int(event, "end_year", "END_YEAR", cy)
    min_opp = event_or_env_int(event, "min_opp", "MIN_OPP", 10)
    s3_bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    s3_prefix = event_or_env_str(event, "s3_prefix", "S3_PREFIX", cfg.raw_running_prefix)

    result = ingest_year_range(start_year, end_year, s3_bucket, s3_prefix, min_opp=min_opp)
    status_code = 200 if result["status"] == "ok" else (207 if result["status"] == "partial" else 400)
    return {"statusCode": status_code, **result}


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


def bronze_to_silver_features_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ..silver.bronze_to_silver_features import build_bronze_to_silver_features

    y = yesterday_utc_date_str()
    cfg = PipelineSettings.from_environ()
    start_date = event_or_env_str(event, "start_date", "START_DATE", y)
    end_date = event_or_env_str(event, "end_date", "END_DATE", y)
    bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    bronze_prefix = event_or_env_str(event, "bronze_prefix", "RAW_PREFIX", cfg.raw_statcast_prefix)
    silver_prefix = event_or_env_str(event, "silver_prefix", "FEATURE_PREFIX", cfg.feature_prefix)
    raw_running = event_or_env_str(
        event, "raw_running_prefix", "RAW_RUNNING_PREFIX", cfg.raw_running_prefix
    )
    raw_defence = event_or_env_str(
        event, "raw_defence_prefix", "RAW_DEFENCE_PREFIX", cfg.raw_defence_prefix
    )
    yt_raw = event_or_env_str(event, "year_to_date", "YEAR_TO_DATE", "true")
    year_to_date = str(yt_raw).strip().lower() not in ("0", "false", "no")

    result = build_bronze_to_silver_features(
        bucket=bucket,
        bronze_statcast_prefix=bronze_prefix,
        silver_prefix=silver_prefix,
        start_date_str=start_date,
        end_date_str=end_date,
        year_to_date=year_to_date,
        raw_running_prefix=raw_running,
        raw_defence_prefix=raw_defence,
    )

    status_code = 200 if result.get("status") in ("ok", "no_data") else 400
    return {
        "statusCode": status_code,
        "body": result.get("message", ""),
        "details": result,
    }
