"""CLI entrypoints (argparse) for pipeline stages."""

from __future__ import annotations

import argparse
import logging

from .runtime import current_utc_year
from .settings import PipelineSettings

logger = logging.getLogger(__name__)


def run_silver_to_gold_preprocessing_main() -> None:
    from ..gold.silver_to_gold_preprocessing import build_silver_to_gold_preprocessing

    cfg = PipelineSettings.from_environ()
    cy = current_utc_year()
    parser = argparse.ArgumentParser(
        description="Build gold preprocessed player-year feature tables from silver outputs."
    )
    parser.add_argument("--start-year", type=int, default=cy - 1)
    parser.add_argument("--end-year", type=int, default=cy)
    parser.add_argument("--bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument("--silver-prefix", type=str, default=cfg.feature_prefix)
    parser.add_argument("--gold-prefix", type=str, default=cfg.gold_prefix)
    parser.add_argument(
        "--role",
        choices=("all", "batter", "pitcher"),
        default="all",
        help="Run preprocessing for both roles or one specific role.",
    )
    parser.add_argument("--correlation-threshold", type=float, default=0.95)
    parser.add_argument("--near-zero-variance-unique-ratio", type=float, default=0.005)
    args = parser.parse_args()

    result = build_silver_to_gold_preprocessing(
        bucket=args.bucket,
        silver_prefix=args.silver_prefix,
        gold_prefix=args.gold_prefix,
        start_year=args.start_year,
        end_year=args.end_year,
        role_filter=args.role,
        correlation_threshold=args.correlation_threshold,
        near_zero_variance_unique_ratio=args.near_zero_variance_unique_ratio,
    )

    if result["status"] == "error":
        logger.error(result["message"])
        raise SystemExit(1)
    if result["status"] == "no_data":
        logger.warning(result["message"])
    else:
        logger.info(result["message"])


def run_gold_archetype_clustering_main() -> None:
    from ..ml.archetype_clustering import (
        ArchetypeClusteringConfig,
        ArchetypeClusteringConfigsByRole,
        GMM_COVARIANCE_TYPES,
        build_gold_archetype_clustering,
    )

    cfg = PipelineSettings.from_environ()
    cy = current_utc_year()
    parser = argparse.ArgumentParser(
        description=(
            "Fit archetype clustering (StandardScaler, PCA, GaussianMixture) on gold "
            "preprocessed player-year tables and write assignments + model artifacts to S3."
        )
    )
    parser.add_argument("--start-year", type=int, default=cy - 1)
    parser.add_argument("--end-year", type=int, default=cy)
    parser.add_argument("--bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument("--gold-prefix", type=str, default=cfg.gold_prefix)
    parser.add_argument(
        "--role",
        choices=("all", "batter", "pitcher"),
        default="all",
        help="Run clustering for both roles or one specific role.",
    )
    parser.add_argument(
        "--pca-n-components",
        type=int,
        default=None,
        help=(
            "Default PCA dimensionality for both roles when role-specific flags are omitted "
            "(optional if --pitcher-pca-n-components / --batter-pca-n-components supply every needed value)."
        ),
    )
    parser.add_argument(
        "--n-clusters",
        type=int,
        default=None,
        help=(
            "Default GMM n_components for both roles when role-specific flags are omitted "
            "(optional if --pitcher-n-clusters / --batter-n-clusters supply every needed value)."
        ),
    )
    parser.add_argument(
        "--gmm-covariance-type",
        choices=GMM_COVARIANCE_TYPES,
        default="full",
        help="GaussianMixture covariance_type (default: full).",
    )
    parser.add_argument(
        "--pitcher-pca-n-components",
        type=int,
        default=None,
        help="PCA dims for pitchers (or default from --pca-n-components when set).",
    )
    parser.add_argument(
        "--pitcher-n-clusters",
        type=int,
        default=None,
        help="GMM n_components for pitchers (or default from --n-clusters when set).",
    )
    parser.add_argument(
        "--pitcher-gmm-covariance-type",
        choices=GMM_COVARIANCE_TYPES,
        default=None,
        help="Override covariance for pitchers (default: --gmm-covariance-type).",
    )
    parser.add_argument(
        "--batter-pca-n-components",
        type=int,
        default=None,
        help="PCA dims for batters (or default from --pca-n-components when set).",
    )
    parser.add_argument(
        "--batter-n-clusters",
        type=int,
        default=None,
        help="GMM n_components for batters (or default from --n-clusters when set).",
    )
    parser.add_argument(
        "--batter-gmm-covariance-type",
        choices=GMM_COVARIANCE_TYPES,
        default=None,
        help="Override covariance for batters (default: --gmm-covariance-type).",
    )
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--n-init", type=int, default=10)
    args = parser.parse_args()

    base_cov = args.gmm_covariance_type
    p_cov = args.pitcher_gmm_covariance_type or base_cov
    b_cov = args.batter_gmm_covariance_type or base_cov
    base_pca = args.pca_n_components
    base_k = args.n_clusters
    p_pca = (
        args.pitcher_pca_n_components
        if args.pitcher_pca_n_components is not None
        else base_pca
    )
    p_k = args.pitcher_n_clusters if args.pitcher_n_clusters is not None else base_k
    b_pca = (
        args.batter_pca_n_components
        if args.batter_pca_n_components is not None
        else base_pca
    )
    b_k = args.batter_n_clusters if args.batter_n_clusters is not None else base_k

    if args.role in ("pitcher", "all") and (p_pca is None or p_k is None):
        parser.error(
            "Pitcher PCA and cluster count required: set --pitcher-pca-n-components and "
            "--pitcher-n-clusters, or use --pca-n-components and --n-clusters as defaults."
        )
    if args.role in ("batter", "all") and (b_pca is None or b_k is None):
        parser.error(
            "Batter PCA and cluster count required: set --batter-pca-n-components and "
            "--batter-n-clusters, or use --pca-n-components and --n-clusters as defaults."
        )

    pitcher_cfg = ArchetypeClusteringConfig(
        pca_n_components=p_pca,
        n_clusters=p_k,
        random_state=args.random_state,
        n_init=args.n_init,
        covariance_type=p_cov,
    )
    batter_cfg = ArchetypeClusteringConfig(
        pca_n_components=b_pca,
        n_clusters=b_k,
        random_state=args.random_state,
        n_init=args.n_init,
        covariance_type=b_cov,
    )

    if args.role == "pitcher":
        build_kw: dict = {"config": pitcher_cfg}
    elif args.role == "batter":
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
