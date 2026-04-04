#!/usr/bin/env python3
"""Silver -> gold preprocessing for model-ready player-year feature tables."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Dict, Iterable, List, Tuple

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
CATCHER_ONLY_COLUMNS = ("def_pop_time_2b_sec", "def_framing_runs")


@dataclass(frozen=True)
class GoldPreprocessingConfig:
    """Configurable behavior for silver->gold transforms."""

    correlation_threshold: float = 0.95
    hard_drop_columns: Tuple[str, ...] = ("woba_value_mean",)
    add_sparse_missing_indicators: bool = True
    include_pitch_type_missing_indicators: bool = True
    include_catcher_missing_indicators: bool = True
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
    missing_indicator_columns: List[str]
    hard_dropped_columns: List[str] = field(default_factory=list)
    correlation_dropped_columns: List[Dict[str, str]] = field(default_factory=list)
    near_zero_variance_dropped_columns: List[str] = field(default_factory=list)
    dropped_columns: Dict[str, List[str]] = field(default_factory=dict)
    correlation_threshold: float = 0.95


def _replace_inf_with_nan(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    num_cols = out.select_dtypes(include=[np.number]).columns
    if len(num_cols) == 0:
        return out
    out.loc[:, num_cols] = out.loc[:, num_cols].replace([np.inf, -np.inf], np.nan)
    return out


def _sparse_missing_indicator_candidates(df: pd.DataFrame, cfg: GoldPreprocessingConfig) -> List[str]:
    cols: List[str] = []
    for col in df.columns:
        if col in ID_COLUMNS:
            continue
        if cfg.include_pitch_type_missing_indicators and col.startswith("pt_"):
            cols.append(col)
    if cfg.include_catcher_missing_indicators:
        cols.extend(c for c in CATCHER_ONLY_COLUMNS if c in df.columns)
    return sorted(set(cols))


def _add_missing_indicators(
    df: pd.DataFrame,
    cfg: GoldPreprocessingConfig,
) -> Tuple[pd.DataFrame, List[str]]:
    if not cfg.add_sparse_missing_indicators:
        return df, []
    out = df.copy()
    added: List[str] = []
    for col in _sparse_missing_indicator_candidates(out, cfg):
        indicator_col = f"{col}_was_missing"
        out[indicator_col] = out[col].isna().astype(int)
        added.append(indicator_col)
    return out, sorted(added)


def _fill_missing_values(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    out = df.copy()
    numeric_cols = [c for c in out.select_dtypes(include=[np.number]).columns if c not in ID_COLUMNS]
    if numeric_cols:
        out.loc[:, numeric_cols] = out.loc[:, numeric_cols].fillna(0.0)
    object_cols = [c for c in out.select_dtypes(include=["object", "string"]).columns if c not in ID_COLUMNS]
    for col in object_cols:
        out[col] = out[col].fillna("unknown")
    return out, sorted(numeric_cols)


def _hard_drop(df: pd.DataFrame, hard_drop_columns: Iterable[str]) -> Tuple[pd.DataFrame, List[str]]:
    out = df.copy()
    dropped = [c for c in hard_drop_columns if c in out.columns]
    if dropped:
        out = out.drop(columns=dropped)
    return out, sorted(dropped)


def _is_preferred_column(col_name: str) -> bool:
    preferred_prefixes = ("estimated_",)
    preferred_substrings = ("xwoba", "_adj_estimated_")
    return col_name.startswith(preferred_prefixes) or any(s in col_name for s in preferred_substrings)


def _pick_drop_for_correlated_pair(c1: str, c2: str) -> str:
    c1_pref = _is_preferred_column(c1)
    c2_pref = _is_preferred_column(c2)
    if c1_pref and not c2_pref:
        return c2
    if c2_pref and not c1_pref:
        return c1
    return max(c1, c2)


def _correlation_prune(
    df: pd.DataFrame, *, threshold: float, protected_columns: Iterable[str] = ()
) -> Tuple[pd.DataFrame, List[str], List[Dict[str, str]]]:
    out = df.copy()
    protected = set(protected_columns)
    numeric_cols = [
        c
        for c in out.select_dtypes(include=[np.number]).columns
        if c not in ID_COLUMNS and c not in protected
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
            drop_col = _pick_drop_for_correlated_pair(c1, c2)
            if drop_col in protected:
                alt = c1 if drop_col == c2 else c2
                if alt in protected:
                    continue
                drop_col = alt
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
        out = out.drop(columns=sorted(to_drop))
    return out, sorted(to_drop), reasons


def _near_zero_variance_drop(
    df: pd.DataFrame, *, unique_ratio_threshold: float, protected_columns: Iterable[str] = ()
) -> Tuple[pd.DataFrame, List[str]]:
    out = df.copy()
    protected = set(protected_columns)
    n_rows = max(len(out), 1)
    candidates = [c for c in out.select_dtypes(include=[np.number]).columns if c not in ID_COLUMNS]
    to_drop: List[str] = []
    for col in candidates:
        if col in protected:
            continue
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
    out = _replace_inf_with_nan(df)
    out, missing_indicator_columns = _add_missing_indicators(out, config)
    out, numeric_filled = _fill_missing_values(out)
    out, hard_dropped = _hard_drop(out, config.hard_drop_columns)
    out, corr_dropped, corr_reasons = _correlation_prune(
        out, threshold=config.correlation_threshold, protected_columns=missing_indicator_columns
    )
    out, nzv_dropped = _near_zero_variance_drop(
        out,
        unique_ratio_threshold=config.near_zero_variance_unique_ratio,
        protected_columns=missing_indicator_columns,
    )

    if out.columns.duplicated().any():
        dupes = sorted(set(out.columns[out.columns.duplicated()].tolist()))
        raise ValueError(f"Duplicate columns after preprocessing: {dupes}")

    feature_cols = [c for c in out.columns if c not in ID_COLUMNS]
    numeric_feature_cols = out[feature_cols].select_dtypes(include=[np.number]).columns.tolist()
    if out[numeric_feature_cols].isna().any().any():
        raise ValueError(f"NaN values remain in numeric gold features for role={role}, year={year}")

    artifacts = PreprocessingArtifacts(
        role=role,
        year=year,
        row_count=int(len(out)),
        id_columns=[c for c in ID_COLUMNS if c in out.columns],
        feature_columns=sorted(feature_cols),
        numeric_columns_filled_zero=numeric_filled,
        missing_indicator_columns=missing_indicator_columns,
        hard_dropped_columns=hard_dropped,
        correlation_dropped_columns=corr_reasons,
        near_zero_variance_dropped_columns=nzv_dropped,
        dropped_columns={
            "hard_drop": hard_dropped,
            "correlation_drop": corr_dropped,
            "near_zero_variance_drop": nzv_dropped,
        },
        correlation_threshold=float(config.correlation_threshold),
    )
    return out, artifacts


def _year_range(start_year: int, end_year: int) -> Iterable[int]:
    for y in range(start_year, end_year + 1):
        yield y


def _write_metadata_json(bucket: str, key: str, artifacts: PreprocessingArtifacts) -> None:
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
    if start_year > end_year:
        return {
            "status": "error",
            "message": f"start_year ({start_year}) must be <= end_year ({end_year})",
            "years_written": [],
            "rows_written": 0,
        }

    valid_roles = ("batter", "pitcher", "all")
    if role_filter not in valid_roles:
        return {
            "status": "error",
            "message": f"role_filter must be one of {valid_roles}, got '{role_filter}'",
            "years_written": [],
            "rows_written": 0,
        }

    cfg = GoldPreprocessingConfig(
        correlation_threshold=correlation_threshold,
        near_zero_variance_unique_ratio=near_zero_variance_unique_ratio,
    )

    roles = ("batter", "pitcher") if role_filter == "all" else (role_filter,)
    rows_written = 0
    years_written: set[int] = set()
    role_years_processed: List[Dict[str, int | str]] = []

    for role in roles:
        for year in _year_range(start_year, end_year):
            in_key = feature_player_year_output_key(silver_prefix, role, year)
            df = read_parquet_from_s3(bucket, in_key, missing_key_log="none")
            if df is None or df.empty:
                continue

            if "role" not in df.columns:
                df = df.copy()
                df["role"] = role

            gold_df, artifacts = preprocess_role_year_df(df, role=role, year=year, config=cfg)
            out_key = gold_player_year_output_key(gold_prefix, role, year)
            write_parquet_to_s3(gold_df, bucket, out_key, log_write=False)

            metadata_key = gold_preprocessing_metadata_key(gold_prefix, role, year)
            _write_metadata_json(bucket, metadata_key, artifacts)

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
    from ..pipeline.cli import run_silver_to_gold_preprocessing_main

    run_silver_to_gold_preprocessing_main()


def handler(event: dict, context) -> dict:
    from ..pipeline.handlers import silver_to_gold_preprocessing_handler

    return silver_to_gold_preprocessing_handler(event, context)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
