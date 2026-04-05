#!/usr/bin/env python3
"""Silver -> gold preprocessing for model-ready player-year feature tables."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

from ..pipeline.s3_interaction import (
    feature_player_year_output_key,
    get_s3_client,
    gold_player_year_output_key,
    gold_preprocessing_metadata_key,
    read_parquet_from_s3,
    write_parquet_to_s3,
)

logger = logging.getLogger(__name__)

ID_COLUMNS = ("player_id", "year", "role")

# Pitch-type share columns dropped for archetype training (arsenal summarized by pitch_type_entropy).
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


def is_column_excluded_from_archetype_training_features(col: str) -> bool:
    """
    True for columns that are not used as PCA / GMM features.

    These are removed from gold player-year tables so downstream ML can use all remaining
    numeric columns (except ids and n_pitches_total, which clustering moves to the index).
    """
    # Imputation flags.
    if col.endswith("_was_missing"):
        return True
    # Pitch-type physics columns.
    if col.startswith("pt_"):
        return True
    # Redundant platoon xwoba means, as we have xwoba_allowed_diff.
    if col in ("xwoba_allowed_lhb_mean", "xwoba_allowed_rhb_mean"):
        return True
    # Pitch-type share columns.
    for pt in PITCH_TYPE_SHARE_CODES_EXCLUDED:
        if col == f"pitch_type_{pt}_share":
            return True
    return False


def _drop_columns_not_used_for_archetype_training(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """Drop columns that are not used as PCA / GMM features."""
    to_drop = [c for c in df.columns if is_column_excluded_from_archetype_training_features(c)]
    if not to_drop:
        return df, []
    return df.drop(columns=sorted(to_drop)), sorted(to_drop)


@dataclass(frozen=True)
class GoldPreprocessingConfig:
    """Configurable behavior for silver->gold transforms."""

    # Correlation threshold for dropping correlated columns.
    correlation_threshold: float = 0.95
    # Columns to hard drop (always dropped).
    hard_drop_columns: Tuple[str, ...] = ("woba_value_mean",)
    near_zero_variance_unique_ratio: float = 0.005


@dataclass
class PreprocessingArtifacts:
    """Serializable details for reproducible train/inference transforms."""

    role: str
    year: int
    row_count: int
    id_columns: List[str]
    feature_columns: List[str]
    numeric_columns_filled_zero: List[str]
    hard_dropped_columns: List[str] = field(default_factory=list)
    correlation_dropped_columns: List[Dict[str, str]] = field(default_factory=list)
    near_zero_variance_dropped_columns: List[str] = field(default_factory=list)
    archetype_training_dropped_columns: List[str] = field(default_factory=list)
    dropped_columns: Dict[str, List[str]] = field(default_factory=dict)
    correlation_threshold: float = 0.95


def _replace_inf_with_nan(df: pd.DataFrame) -> pd.DataFrame:
    """Replace infinite values with NaN."""
    out = df.copy()
    num_cols = out.select_dtypes(include=[np.number]).columns
    if len(num_cols) == 0:
        return out
    out.loc[:, num_cols] = out.loc[:, num_cols].replace([np.inf, -np.inf], np.nan)
    return out


def _fill_missing_values(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """Fill missing values with 0.0 for numeric columns and "unknown" for object columns."""
    out = df.copy()
    numeric_cols = [c for c in out.select_dtypes(include=[np.number]).columns if c not in ID_COLUMNS]
    if numeric_cols:
        # Fill numeric columns with 0.0.
        out.loc[:, numeric_cols] = out.loc[:, numeric_cols].fillna(0.0)
    object_cols = [c for c in out.select_dtypes(include=["object", "string"]).columns if c not in ID_COLUMNS]
    for col in object_cols:
        # Fill object columns with "unknown".
        out[col] = out[col].fillna("unknown")
    return out, sorted(numeric_cols)


def _hard_drop(df: pd.DataFrame, hard_drop_columns: Iterable[str]) -> Tuple[pd.DataFrame, List[str]]:
    """Drop columns that are always dropped."""
    out = df.copy()
    dropped = [c for c in hard_drop_columns if c in out.columns]
    if dropped:
        out = out.drop(columns=dropped)
    return out, sorted(dropped)


def _is_preferred_column(col_name: str) -> bool:
    """Check if a column is a preferred column."""
    preferred_prefixes = ("estimated_",)
    preferred_substrings = ("xwoba", "_adj_estimated_")
    return col_name.startswith(preferred_prefixes) or any(s in col_name for s in preferred_substrings)


def _pick_drop_for_correlated_pair(c1: str, c2: str) -> str:
    """Pick a column to drop based on preference."""
    c1_pref = _is_preferred_column(c1)
    c2_pref = _is_preferred_column(c2)
    if c1_pref and not c2_pref:
        return c2
    if c2_pref and not c1_pref:
        return c1
    return max(c1, c2)


def _correlation_prune(
    df: pd.DataFrame, *, threshold: float
) -> Tuple[pd.DataFrame, List[str], List[Dict[str, str]]]:
    """Drop correlated columns."""
    out = df.copy()
    # Get numeric columns that are not ID columns.
    numeric_cols = [
        c
        for c in out.select_dtypes(include=[np.number]).columns
        if c not in ID_COLUMNS
    ]
    if len(numeric_cols) < 2:
        return out, [], []
    corr = out[numeric_cols].corr().abs()
    to_drop: set[str] = set()
    reasons: List[Dict[str, str]] = []
    for i, c1 in enumerate(corr.columns):
        for c2 in corr.columns[i + 1 :]:
            if c1 in to_drop or c2 in to_drop:
                continue
            val = corr.loc[c1, c2]
            if pd.isna(val) or val < threshold:
                continue
            # Pick a column to drop based on preference.
            drop_col = _pick_drop_for_correlated_pair(c1, c2)
            keep_col = c1 if drop_col == c2 else c2
            to_drop.add(drop_col)
            reasons.append(
                {
                    "dropped": drop_col,
                    "kept": keep_col,
                    "correlation": f"{float(val):.6f}",
                }
            )
    if to_drop:
        # Drop the correlated columns.
        out = out.drop(columns=sorted(to_drop))
    return out, sorted(to_drop), reasons


def _near_zero_variance_drop(
    df: pd.DataFrame, *, unique_ratio_threshold: float
) -> Tuple[pd.DataFrame, List[str]]:
    """Drop columns with low variance."""
    out = df.copy()
    n_rows = max(len(out), 1)
    candidates = [c for c in out.select_dtypes(include=[np.number]).columns if c not in ID_COLUMNS]
    to_drop: List[str] = []
    for col in candidates:
        nunique = int(out[col].nunique(dropna=False))
        unique_ratio = nunique / n_rows
        if nunique <= 1 or unique_ratio <= unique_ratio_threshold:
            to_drop.append(col)
    if to_drop:
        out = out.drop(columns=sorted(set(to_drop)))
    return out, sorted(set(to_drop))


def preprocess_role_year_df(
    df: pd.DataFrame,
    *,
    role: str,
    year: int,
    config: GoldPreprocessingConfig,
) -> Tuple[pd.DataFrame, PreprocessingArtifacts]:
    """Apply deterministic silver->gold preprocessing for one role-year frame."""
    # Replace infinite values with NaN.
    out = _replace_inf_with_nan(df)
    # Fill missing values with 0.0 for numeric columns and "unknown" for object columns.
    out, numeric_filled = _fill_missing_values(out)
    # Drop columns that are always dropped.
    out, hard_dropped = _hard_drop(out, config.hard_drop_columns)
    # Drop correlated columns.
    out, corr_dropped, corr_reasons = _correlation_prune(out, threshold=config.correlation_threshold)
    # Drop columns with low variance.
    out, nzv_dropped = _near_zero_variance_drop(
        out, unique_ratio_threshold=config.near_zero_variance_unique_ratio
    )
    # Drop columns that are not used as PCA / GMM features.
    out, archetype_dropped = _drop_columns_not_used_for_archetype_training(out)

    # Check for duplicate columns.
    if out.columns.duplicated().any():
        dupes = sorted(set(out.columns[out.columns.duplicated()].tolist()))
        raise ValueError(f"Duplicate columns after preprocessing: {dupes}")

    # Get feature columns that are not ID columns.
    feature_cols = [c for c in out.columns if c not in ID_COLUMNS]
    # Get numeric feature columns.
    numeric_feature_cols = out[feature_cols].select_dtypes(include=[np.number]).columns.tolist()
    # Check for NaN values in numeric feature columns.
    if out[numeric_feature_cols].isna().any().any():
        raise ValueError(f"NaN values remain in numeric gold features for role={role}, year={year}")

    # Create preprocessing artifacts.
    artifacts = PreprocessingArtifacts(
        role=role,
        year=year,
        row_count=int(len(out)),
        id_columns=[c for c in ID_COLUMNS if c in out.columns],
        feature_columns=sorted(feature_cols),
        numeric_columns_filled_zero=numeric_filled,
        hard_dropped_columns=hard_dropped,
        correlation_dropped_columns=corr_reasons,
        near_zero_variance_dropped_columns=nzv_dropped,
        archetype_training_dropped_columns=archetype_dropped,
        dropped_columns={
            "hard_drop": hard_dropped,
            "correlation_drop": corr_dropped,
            "near_zero_variance_drop": nzv_dropped,
            "archetype_training_drop": archetype_dropped,
        },
        correlation_threshold=float(config.correlation_threshold),
    )
    # Return the preprocessed dataframe and artifacts.
    return out, artifacts


def _year_range(start_year: int, end_year: int) -> Iterable[int]:
    """Generate a range of years between start and end."""
    for y in range(start_year, end_year + 1):
        yield y


def _write_metadata_json(bucket: str, key: str, artifacts: PreprocessingArtifacts) -> None:
    """Write metadata JSON to S3."""
    client = get_s3_client()
    payload = {
        **asdict(artifacts),
        "generated_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def build_silver_to_gold_preprocessing(
    *,
    bucket: str,
    silver_prefix: str,
    gold_prefix: str,
    start_year: int,
    end_year: int,
    role_filter: str = "all",
    correlation_threshold: float = 0.95,
    near_zero_variance_unique_ratio: float = 0.005,
) -> Dict[str, object]:
    """Read silver role/year tables and write preprocessed gold outputs + artifacts."""
    # Check if start year is greater than end year.
    if start_year > end_year:
        return {
            "status": "error",
            "message": f"start_year ({start_year}) must be <= end_year ({end_year})",
            "years_written": [],
            "rows_written": 0,
        }

    # Check if role filter is valid.
    valid_roles = ("batter", "pitcher", "all")
    if role_filter not in valid_roles:
        return {
            "status": "error",
            "message": f"role_filter must be one of {valid_roles}, got '{role_filter}'",
            "years_written": [],
            "rows_written": 0,
        }

    # Create preprocessing configuration.
    cfg = GoldPreprocessingConfig(
        correlation_threshold=correlation_threshold,
        near_zero_variance_unique_ratio=near_zero_variance_unique_ratio,
    )

    # Create roles list.
    roles = ("batter", "pitcher") if role_filter == "all" else (role_filter,)
    # Initialize counters.
    rows_written = 0
    years_written: set[int] = set()
    role_years_processed: List[Dict[str, int | str]] = []

    # Process each role and year.
    for role in roles:
        for year in _year_range(start_year, end_year):
            # Read the silver feature table.
            in_key = feature_player_year_output_key(silver_prefix, role, year)
            df = read_parquet_from_s3(bucket, in_key, missing_key_log="none")
            if df is None or df.empty:
                continue

            # Add role column if not present.
            if "role" not in df.columns:
                df = df.copy()
                df["role"] = role

            # Preprocess the role-year dataframe.
            gold_df, artifacts = preprocess_role_year_df(df, role=role, year=year, config=cfg)
            # Write the preprocessed dataframe to S3.
            out_key = gold_player_year_output_key(gold_prefix, role, year)
            write_parquet_to_s3(gold_df, bucket, out_key, log_write=False)

            # Write the preprocessing artifacts to S3.
            metadata_key = gold_preprocessing_metadata_key(gold_prefix, role, year)
            _write_metadata_json(bucket, metadata_key, artifacts)

            # Update counters.
            rows_written += len(gold_df)
            years_written.add(year)
            role_years_processed.append({"role": role, "year": year, "rows": int(len(gold_df))})
            logger.info(
                "Gold preprocessing wrote %d rows for role=%s year=%d to s3://%s/%s",
                len(gold_df),
                role,
                year,
                bucket,
                out_key,
            )

    if rows_written == 0:
        return {
            "status": "no_data",
            "message": (
                f"No silver feature tables found for roles={roles} years={start_year}..{end_year}"
            ),
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
        }

    sorted_years = sorted(years_written)
    message = (
        f"Silver->gold preprocessing wrote {rows_written} rows across years {sorted_years} "
        f"for roles {list(roles)}"
    )
    return {
        "status": "ok",
        "message": message,
        "years_written": sorted_years,
        "rows_written": rows_written,
        "role_years_processed": role_years_processed,
    }


def main() -> None:
    from ..pipeline.runtime import current_utc_year
    from ..pipeline.settings import PipelineSettings

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


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ..pipeline.runtime import (
        current_utc_year,
        event_or_env_int,
        event_or_env_str,
    )
    from ..pipeline.settings import PipelineSettings

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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
