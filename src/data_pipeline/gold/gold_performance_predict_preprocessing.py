#!/usr/bin/env python3
"""
Silver -> gold preprocessing for the player performance prediction training matrix.

Companion to ``gold_archetype_preprocessing`` (which feeds the archetype PCA/GMM
models). This module instead builds one supervised training matrix per role-year:
for each player-season N, a row holding

  - lagged standard stat lines for seasons N-1..N-3 (``lag{k}_*``), each with an
    availability flag,
  - playing-time (and recency) weighted blends of the lagged rate stats
    (``lag_weighted_*``), Marcel-style,
  - the player's season-N age (derived from silver ``bio_age`` on the most recent
    lag season, so no season-N information is needed at inference time),
  - season N-1 Statcast "skill" features (``skill_lag1_*``: exit velo, barrel
    rate, whiff rate, ...),
  - the target: the season-N standard stat line (``target_*``).

Inputs are the silver standard stat-line tables
(``{silver_prefix}/{role}/year=Y/standard_stats_player_year.parquet``) and the
silver Statcast feature tables
(``{silver_prefix}/{role}/year=Y/player_year_features.parquet``). Outputs land at
``{gold_prefix}/performance_prediction/{role}/year=Y/training_matrix.parquet``
with a metadata JSON alongside.

Missing values are left as NaN (with explicit ``lag{k}_available`` /
``skill_lag1_available`` flags) rather than zero-filled: for a regression matrix,
imputation strategy belongs to model training, and 0 is a misleading value for
stats like OPS or ERA.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from ...common.runtime_helpers import current_utc_year, event_or_env_int, event_or_env_str
from ...common.s3_helpers import get_s3_client, read_parquet_from_s3, write_parquet_to_s3
from ...common.settings import PipelineSettings
from ..silver.standard_stats.silver_standard_player_year_stats import outs_from_innings_pitched

logger = logging.getLogger(__name__)

ID_COLUMNS = ("player_id", "year", "role", "player_name", "primary_position")

VALID_ROLES = ("batter", "pitcher")

# Standard stat-line columns carried as lag features, per role. ``innings`` is derived
# from the statsapi ``ip`` column (.1/.2 thirds notation) so it is linear in outs.
ROLE_LAG_STAT_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "batter": ("g", "pa", "avg", "obp", "slg", "ops", "bb_pct", "k_pct", "hr", "sb"),
    "pitcher": ("g", "gs", "innings", "era", "whip", "k_per_9", "bb_per_9", "hr_per_9", "sv"),
}

# Rate stats that get a playing-time weighted blend across the lag seasons. Counting
# and playing-time columns are excluded: summing/weighting them is a different
# operation and the per-lag columns already carry that signal.
ROLE_RATE_STAT_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "batter": ("avg", "obp", "slg", "ops", "bb_pct", "k_pct"),
    "pitcher": ("era", "whip", "k_per_9", "bb_per_9", "hr_per_9"),
}

# Season-N stat line used as regression targets.
ROLE_TARGET_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "batter": ("pa", "avg", "obp", "slg", "ops", "bb_pct", "k_pct", "hr", "sb"),
    "pitcher": ("innings", "era", "whip", "k_per_9", "bb_per_9", "hr_per_9"),
}

# Playing-time column used both as per-lag exposure feature and as the weight for
# the ``lag_weighted_*`` blends.
ROLE_PLAYING_TIME_COLUMN: Dict[str, str] = {"batter": "pa", "pitcher": "innings"}

# Statcast "skill" columns pulled from the silver feature table for season N-1.
# Missing columns are skipped (and recorded in metadata) so silver schema drift
# degrades gracefully instead of failing the build.
ROLE_SKILL_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "batter": (
        "launch_speed_mean",
        "launch_angle_mean",
        "barrel_rate",
        "hard_hit_rate",
        "sweet_spot_percent",
        "whiff_rate",
        "chase_rate",
        "zone_swing_rate",
        "swing_rate",
        "walk_rate",
        "iso_value_mean",
        "estimated_slg_using_speedangle_mean",
        "estimated_woba_using_speedangle_mean",
        "sprint_speed_mean",
        "gb_percent",
        "ld_percent",
        "fb_percent",
        "pull_percent",
    ),
    "pitcher": (
        "release_speed_max",
        "fastball_velo_mean",
        "offspeed_velo_mean",
        "velo_differential",
        "release_extension_mean",
        "batter_whiff_rate",
        "batter_chase_rate",
        "batter_swing_rate",
        "in_zone_rate",
        "edge_percent",
        "meatball_percent",
        "first_pitch_strike_rate",
        "gb_percent_allowed",
        "fb_percent_allowed",
        "xwoba_allowed_lhb_mean",
        "xwoba_allowed_rhb_mean",
        "platoon_xwoba_allowed_diff",
        "pitch_type_entropy",
        "delta_run_exp_mean",
    ),
}


@dataclass(frozen=True)
class PerformancePredictConfig:
    """Configurable behavior for the performance-prediction training matrix."""

    n_lag_seasons: int = 3
    # Marcel-style recency weights for lag1..lagN; each lag's effective weight is
    # recency * playing time. Extra entries are ignored; must cover n_lag_seasons.
    recency_weights: Tuple[float, ...] = (5.0, 4.0, 3.0)

    def __post_init__(self) -> None:
        if self.n_lag_seasons < 1:
            raise ValueError(f"n_lag_seasons must be >= 1, got {self.n_lag_seasons}")
        if len(self.recency_weights) < self.n_lag_seasons:
            raise ValueError(
                f"recency_weights needs >= {self.n_lag_seasons} entries, "
                f"got {self.recency_weights}"
            )


@dataclass
class TrainingMatrixArtifacts:
    """Serializable details for reproducible training-matrix builds."""

    role: str
    year: int
    row_count: int
    rows_dropped_no_lag_history: int
    id_columns: List[str]
    lag_stat_columns: List[str]
    weighted_stat_columns: List[str]
    skill_columns: List[str]
    skill_columns_missing_in_silver: List[str] = field(default_factory=list)
    availability_columns: List[str] = field(default_factory=list)
    target_columns: List[str] = field(default_factory=list)
    n_lag_seasons: int = 3
    recency_weights: List[float] = field(default_factory=list)
    playing_time_column: str = ""


def _prepare_standard_stats(df: pd.DataFrame, *, role: str) -> pd.DataFrame:
    """
    Normalize one silver standard stat-line frame for matrix assembly.

    Coerces stat columns to numerics (the statsapi returns rate stats as strings),
    derives ``innings`` for pitchers, and dedupes on player_id.
    """
    out = df.copy()
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce")
    out = out.loc[out["player_id"].notna()].copy()
    out["player_id"] = out["player_id"].astype("int64")
    out = out.drop_duplicates(subset=["player_id"], keep="first")

    if role == "pitcher" and "ip" in out.columns:
        out["innings"] = outs_from_innings_pitched(out["ip"]) / 3.0

    keep = ["player_id"]
    if "full_name" in out.columns:
        out["player_name"] = out["full_name"]
        keep.append("player_name")
    for col in ROLE_LAG_STAT_COLUMNS[role]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").replace(
                [np.inf, -np.inf], np.nan
            )
            keep.append(col)
    return out[keep]


def _lag_column(k: int, stat: str) -> str:
    return f"lag{k}_{stat}"


def _merge_lag_stats(
    base: pd.DataFrame,
    lag_stats: Dict[int, Optional[pd.DataFrame]],
    *,
    role: str,
    n_lag_seasons: int,
) -> pd.DataFrame:
    """Left-merge each lag season's stat line as ``lag{k}_*`` columns onto ``base``."""
    out = base.copy()
    stat_cols = ROLE_LAG_STAT_COLUMNS[role]
    for k in range(1, n_lag_seasons + 1):
        lag_df = lag_stats.get(k)
        if lag_df is None or lag_df.empty:
            for col in stat_cols:
                out[_lag_column(k, col)] = np.nan
            out[f"lag{k}_available"] = 0.0
            continue
        prepared = _prepare_standard_stats(lag_df, role=role)
        renames = {c: _lag_column(k, c) for c in stat_cols if c in prepared.columns}
        merged_cols = ["player_id", *renames]
        out = out.merge(
            prepared[merged_cols].rename(columns=renames),
            on="player_id",
            how="left",
            indicator=f"_lag{k}_merge",
        )
        out[f"lag{k}_available"] = (out[f"_lag{k}_merge"] == "both").astype(float)
        out = out.drop(columns=[f"_lag{k}_merge"])
        for col in stat_cols:
            if _lag_column(k, col) not in out.columns:
                out[_lag_column(k, col)] = np.nan
    return out


def _add_playing_time_weighted_stats(
    df: pd.DataFrame, *, role: str, config: PerformancePredictConfig
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Add ``lag_weighted_{stat}`` blends over the lag seasons.

    Each lag contributes weight recency_k * playing_time_k; lags where the stat or
    the playing time is missing contribute nothing. All-missing rows stay NaN.
    """
    out = df.copy()
    pt_col = ROLE_PLAYING_TIME_COLUMN[role]
    weighted_cols: List[str] = []
    for stat in ROLE_RATE_STAT_COLUMNS[role]:
        numerator = pd.Series(0.0, index=out.index)
        denominator = pd.Series(0.0, index=out.index)
        for k in range(1, config.n_lag_seasons + 1):
            value = out[_lag_column(k, stat)]
            playing_time = out[_lag_column(k, pt_col)].clip(lower=0.0)
            weight = config.recency_weights[k - 1] * playing_time
            valid = value.notna() & weight.notna() & (weight > 0)
            numerator = numerator + (value * weight).where(valid, 0.0)
            denominator = denominator + weight.where(valid, 0.0)
        name = f"lag_weighted_{stat}"
        out[name] = numerator / denominator.replace(0.0, np.nan)
        weighted_cols.append(name)
    return out, weighted_cols


def _add_age_and_position(
    df: pd.DataFrame,
    lag_features: Dict[int, Optional[pd.DataFrame]],
    *,
    n_lag_seasons: int,
) -> pd.DataFrame:
    """
    Add season-N ``age`` and ``primary_position`` from the most recent lag season's
    silver feature table (``bio_age`` at season N-k, plus k years).

    Using lagged bio rows keeps the matrix buildable for a season that has not been
    played yet (inference), at the cost of missing age for players with no silver
    feature row in any lag season.
    """
    out = df.copy()
    age = pd.Series(np.nan, index=out.index)
    position = pd.Series(np.nan, index=out.index, dtype="object")
    for k in range(1, n_lag_seasons + 1):
        feats = lag_features.get(k)
        if feats is None or feats.empty or "player_id" not in feats.columns:
            continue
        cols = ["player_id"]
        if "bio_age" in feats.columns:
            cols.append("bio_age")
        if "primary_position" in feats.columns:
            cols.append("primary_position")
        if len(cols) == 1:
            continue
        sub = feats[cols].copy()
        sub["player_id"] = pd.to_numeric(sub["player_id"], errors="coerce")
        sub = sub.loc[sub["player_id"].notna()].drop_duplicates(
            subset=["player_id"], keep="first"
        )
        sub["player_id"] = sub["player_id"].astype("int64")
        merged = out[["player_id"]].merge(sub, on="player_id", how="left")
        merged.index = out.index
        if "bio_age" in merged.columns:
            lag_age = pd.to_numeric(merged["bio_age"], errors="coerce") + float(k)
            age = age.fillna(lag_age)
        if "primary_position" in merged.columns:
            position = position.fillna(merged["primary_position"])
    out["age"] = age
    out["primary_position"] = position
    return out


def _merge_skill_features(
    df: pd.DataFrame,
    lag1_features: Optional[pd.DataFrame],
    *,
    role: str,
) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """Left-merge season N-1 Statcast skill columns as ``skill_lag1_*`` onto ``df``."""
    out = df.copy()
    wanted = ROLE_SKILL_COLUMNS[role]
    if lag1_features is None or lag1_features.empty or "player_id" not in lag1_features.columns:
        for col in wanted:
            out[f"skill_lag1_{col}"] = np.nan
        out["skill_lag1_available"] = 0.0
        return out, [f"skill_lag1_{c}" for c in wanted], list(wanted)

    present = [c for c in wanted if c in lag1_features.columns]
    missing = [c for c in wanted if c not in lag1_features.columns]
    sub = lag1_features[["player_id", *present]].copy()
    sub["player_id"] = pd.to_numeric(sub["player_id"], errors="coerce")
    sub = sub.loc[sub["player_id"].notna()].drop_duplicates(subset=["player_id"], keep="first")
    sub["player_id"] = sub["player_id"].astype("int64")
    for col in present:
        sub[col] = pd.to_numeric(sub[col], errors="coerce").replace([np.inf, -np.inf], np.nan)
    sub = sub.rename(columns={c: f"skill_lag1_{c}" for c in present})

    out = out.merge(sub, on="player_id", how="left", indicator="_skill_merge")
    out["skill_lag1_available"] = (out["_skill_merge"] == "both").astype(float)
    out = out.drop(columns=["_skill_merge"])
    for col in missing:
        out[f"skill_lag1_{col}"] = np.nan
    return out, [f"skill_lag1_{c}" for c in wanted], missing


def build_role_year_training_matrix(
    *,
    target_stats: pd.DataFrame,
    lag_stats: Dict[int, Optional[pd.DataFrame]],
    lag_features: Dict[int, Optional[pd.DataFrame]],
    role: str,
    year: int,
    config: PerformancePredictConfig,
) -> Tuple[pd.DataFrame, TrainingMatrixArtifacts]:
    """
    Assemble the training matrix for one role and target season ``year``.

    ``lag_stats`` / ``lag_features`` map lag index k (1..n_lag_seasons) to the silver
    frame for season year-k (None when that season has no data). Rows are one per
    player with a season-``year`` stat line and at least one lag season of stats.
    """
    if role not in VALID_ROLES:
        raise ValueError(f"role must be one of {VALID_ROLES}, got '{role}'")

    base = _prepare_standard_stats(target_stats, role=role)
    target_cols = [c for c in ROLE_TARGET_COLUMNS[role] if c in base.columns]
    base = base.rename(columns={c: f"target_{c}" for c in target_cols})
    # Lag stat columns share names with target-year stat columns, so targets are
    # renamed before the lag merges.
    base = base.drop(
        columns=[c for c in ROLE_LAG_STAT_COLUMNS[role] if c in base.columns]
    )

    out = _merge_lag_stats(base, lag_stats, role=role, n_lag_seasons=config.n_lag_seasons)

    availability_cols = [f"lag{k}_available" for k in range(1, config.n_lag_seasons + 1)]
    has_history = out[availability_cols].sum(axis=1) > 0
    rows_dropped = int((~has_history).sum())
    out = out.loc[has_history].reset_index(drop=True)

    out, weighted_cols = _add_playing_time_weighted_stats(out, role=role, config=config)
    out = _add_age_and_position(out, lag_features, n_lag_seasons=config.n_lag_seasons)
    out, skill_cols, skill_missing = _merge_skill_features(
        out, lag_features.get(1), role=role
    )

    out["role"] = role
    out["year"] = int(year)

    lag_stat_cols = [
        _lag_column(k, c)
        for k in range(1, config.n_lag_seasons + 1)
        for c in ROLE_LAG_STAT_COLUMNS[role]
    ]
    target_out_cols = [f"target_{c}" for c in target_cols]
    ordered = [
        *[c for c in ID_COLUMNS if c in out.columns],
        "age",
        *lag_stat_cols,
        *availability_cols,
        *weighted_cols,
        *skill_cols,
        "skill_lag1_available",
        *target_out_cols,
    ]
    out = out[ordered]

    if out.columns.duplicated().any():
        dupes = sorted(set(out.columns[out.columns.duplicated()].tolist()))
        raise ValueError(f"Duplicate columns in training matrix: {dupes}")

    artifacts = TrainingMatrixArtifacts(
        role=role,
        year=int(year),
        row_count=int(len(out)),
        rows_dropped_no_lag_history=rows_dropped,
        id_columns=[c for c in ID_COLUMNS if c in out.columns],
        lag_stat_columns=lag_stat_cols,
        weighted_stat_columns=weighted_cols,
        skill_columns=skill_cols,
        skill_columns_missing_in_silver=sorted(skill_missing),
        availability_columns=[*availability_cols, "skill_lag1_available"],
        target_columns=target_out_cols,
        n_lag_seasons=config.n_lag_seasons,
        recency_weights=[float(w) for w in config.recency_weights[: config.n_lag_seasons]],
        playing_time_column=ROLE_PLAYING_TIME_COLUMN[role],
    )
    return out, artifacts


def _year_range(start_year: int, end_year: int) -> Iterable[int]:
    """Generate a range of years between start and end."""
    for y in range(start_year, end_year + 1):
        yield y


def _write_metadata_json(bucket: str, key: str, artifacts: TrainingMatrixArtifacts) -> None:
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


class _SilverCache:
    """Memoized silver reads: lag seasons are re-read as targets (and vice versa)."""

    def __init__(self, bucket: str, silver_prefix: str) -> None:
        self._bucket = bucket
        self._prefix = silver_prefix.strip("/")
        self._cache: Dict[Tuple[str, str, int], Optional[pd.DataFrame]] = {}

    def _get(self, kind: str, role: str, year: int, filename: str) -> Optional[pd.DataFrame]:
        cache_key = (kind, role, year)
        if cache_key not in self._cache:
            key = f"{self._prefix}/{role}/year={year}/{filename}"
            self._cache[cache_key] = read_parquet_from_s3(
                self._bucket, key, missing_key_log="none"
            )
        return self._cache[cache_key]

    def standard_stats(self, role: str, year: int) -> Optional[pd.DataFrame]:
        return self._get("std", role, year, "standard_stats_player_year.parquet")

    def features(self, role: str, year: int) -> Optional[pd.DataFrame]:
        return self._get("feat", role, year, "player_year_features.parquet")


def build_performance_training_matrices(
    *,
    bucket: str,
    silver_prefix: str,
    gold_prefix: str,
    start_year: int,
    end_year: int,
    role_filter: str = "all",
    config: Optional[PerformancePredictConfig] = None,
) -> Dict[str, object]:
    """
    Build and write the performance-prediction training matrix for each role-year.

    ``start_year``..``end_year`` are target seasons N; each also reads silver seasons
    N-1..N-n_lag_seasons for the lag features.
    """
    if start_year > end_year:
        return {
            "status": "error",
            "message": f"start_year ({start_year}) must be <= end_year ({end_year})",
            "years_written": [],
            "rows_written": 0,
        }

    valid_filters = (*VALID_ROLES, "all")
    if role_filter not in valid_filters:
        return {
            "status": "error",
            "message": f"role_filter must be one of {valid_filters}, got '{role_filter}'",
            "years_written": [],
            "rows_written": 0,
        }

    cfg = config or PerformancePredictConfig()
    roles = VALID_ROLES if role_filter == "all" else (role_filter,)
    cache = _SilverCache(bucket, silver_prefix)

    rows_written = 0
    years_written: set[int] = set()
    role_years_processed: List[Dict[str, int | str]] = []

    for role in roles:
        for year in _year_range(start_year, end_year):
            target_df = cache.standard_stats(role, year)
            if target_df is None or target_df.empty:
                continue
            lag_stats = {
                k: cache.standard_stats(role, year - k)
                for k in range(1, cfg.n_lag_seasons + 1)
            }
            lag_features = {
                k: cache.features(role, year - k) for k in range(1, cfg.n_lag_seasons + 1)
            }

            matrix, artifacts = build_role_year_training_matrix(
                target_stats=target_df,
                lag_stats=lag_stats,
                lag_features=lag_features,
                role=role,
                year=year,
                config=cfg,
            )
            if matrix.empty:
                logger.info(
                    "No %s rows with lag history for target year=%d; skipping.", role, year
                )
                continue

            out_key = (
                f"{gold_prefix.strip('/')}/performance_prediction/{role}/year={year}"
                "/training_matrix.parquet"
            )
            write_parquet_to_s3(matrix, bucket, out_key, log_write=False)
            metadata_key = (
                f"{gold_prefix.strip('/')}/performance_prediction/{role}/year={year}"
                "/training_matrix_metadata.json"
            )
            _write_metadata_json(bucket, metadata_key, artifacts)

            rows_written += len(matrix)
            years_written.add(year)
            role_years_processed.append({"role": role, "year": year, "rows": int(len(matrix))})
            logger.info(
                "Performance-prediction gold wrote %d rows for role=%s target year=%d to s3://%s/%s",
                len(matrix),
                role,
                year,
                bucket,
                out_key,
            )

    if rows_written == 0:
        return {
            "status": "no_data",
            "message": (
                f"No performance training matrices built for roles={list(roles)} "
                f"target years={start_year}..{end_year}"
            ),
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
        }

    sorted_years = sorted(years_written)
    message = (
        f"Performance-prediction preprocessing wrote {rows_written} rows across target "
        f"years {sorted_years} for roles {list(roles)}"
    )
    return {
        "status": "ok",
        "message": message,
        "years_written": sorted_years,
        "rows_written": rows_written,
        "role_years_processed": role_years_processed,
    }


def main() -> None:
    cfg = PipelineSettings.from_environ()
    cy = current_utc_year()
    parser = argparse.ArgumentParser(
        description=(
            "Build gold performance-prediction training matrices (lagged stats + "
            "Statcast skills -> season-N stat line targets) from silver outputs."
        )
    )
    parser.add_argument("--start-year", type=int, default=cy - 1)
    parser.add_argument("--end-year", type=int, default=cy)
    parser.add_argument("--bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument("--silver-prefix", type=str, default=cfg.feature_prefix)
    parser.add_argument("--gold-prefix", type=str, default=cfg.gold_prefix)
    parser.add_argument(
        "--role",
        choices=("all", *VALID_ROLES),
        default="all",
        help="Build matrices for all roles or one specific role.",
    )
    parser.add_argument("--n-lag-seasons", type=int, default=3)
    args = parser.parse_args()

    result = build_performance_training_matrices(
        bucket=args.bucket,
        silver_prefix=args.silver_prefix,
        gold_prefix=args.gold_prefix,
        start_year=args.start_year,
        end_year=args.end_year,
        role_filter=args.role,
        config=PerformancePredictConfig(n_lag_seasons=args.n_lag_seasons),
    )

    if result["status"] == "error":
        logger.error(result["message"])
        raise SystemExit(1)
    if result["status"] == "no_data":
        logger.warning(result["message"])
    else:
        logger.info(result["message"])


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    cy = current_utc_year()
    cfg = PipelineSettings.from_environ()
    start_year = event_or_env_int(event, "start_year", "START_YEAR", cy - 1)
    end_year = event_or_env_int(event, "end_year", "END_YEAR", cy)
    bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    silver_prefix = event_or_env_str(event, "silver_prefix", "FEATURE_PREFIX", cfg.feature_prefix)
    gold_prefix = event_or_env_str(event, "gold_prefix", "GOLD_PREFIX", cfg.gold_prefix)
    role = event_or_env_str(event, "role", "ROLE", "all")
    n_lag_seasons = event_or_env_int(event, "n_lag_seasons", "N_LAG_SEASONS", 3)

    result = build_performance_training_matrices(
        bucket=bucket,
        silver_prefix=silver_prefix,
        gold_prefix=gold_prefix,
        start_year=start_year,
        end_year=end_year,
        role_filter=role,
        config=PerformancePredictConfig(n_lag_seasons=n_lag_seasons),
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
