#!/usr/bin/env python3
"""
Marcel the Monkey baseline projections — the benchmark every xWAR Engine
projection is measured against.

The recipe is four steps:

  1. a 5/4/3 recency- and playing-time-weighted average of the last three seasons,
  2. regression toward the league mean by an amount that shrinks as playing time
     grows (few PA -> heavy regression; many PA -> almost none),
  3. a flat age adjustment around a peak age of 29, and
  4. scaling rate stats onto a Marcel-projected playing time (PA for batters,
     IP for pitchers) to recover counting stats.

Step 1 is already done upstream: the gold performance-prediction training matrix
(``{gold_prefix}/performance_prediction/{role}/year=Y/training_matrix.parquet``,
built by ``gold_performance_predict_preprocessing``) carries the 5/4/3 x
playing-time weighted rate blends (``lag_weighted_*``), the per-lag stat lines
(``lag{k}_*``), the season-N ``age``, and the actual season-N stat line
(``target_*``). This module adds steps 2-4 to turn those blends into projections,
writes them per role-year, and scores them against the ``target_*`` actuals so
every future model has a fixed line to beat.

Outputs land at
``{gold_prefix}/baselines/marcel/{role}/year=Y/marcel_projections.parquet`` (one
row per player, ``proj_*`` columns alongside the ``target_*`` actuals) with a
``marcel_metrics.json`` metadata sidecar (per-stat MAE / RMSE / bias, league
rates, and the config used).
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from ...common.runtime_helpers import current_utc_year, event_or_env_int, event_or_env_str
from ...common.s3_helpers import get_s3_client, read_parquet_from_s3, write_parquet_to_s3
from ...common.settings import PipelineSettings

logger = logging.getLogger(__name__)

VALID_ROLES = ("batter", "pitcher")

# Rate targets projected directly from a regressed, age-adjusted weighted rate.
# Each has a matching ``lag_weighted_{stat}`` column in the gold training matrix.
# ``ops`` is intentionally excluded and derived as obp + slg so the slash line
# stays internally consistent after regression and aging.
ROLE_RATE_TARGETS: Dict[str, Tuple[str, ...]] = {
    "batter": ("avg", "obp", "slg", "bb_pct", "k_pct"),
    "pitcher": ("era", "whip", "k_per_9", "bb_per_9", "hr_per_9"),
}

# Counting targets projected as (weighted per-PA rate, regressed + aged) x projected PA.
ROLE_COUNT_TARGETS: Dict[str, Tuple[str, ...]] = {
    "batter": ("hr", "sb"),
    "pitcher": (),
}

# Playing-time target: the projection scale for counting stats, and a target itself.
ROLE_PT_TARGET: Dict[str, str] = {"batter": "pa", "pitcher": "innings"}

# Per-lag playing-time column used for weighting and the PT projection.
ROLE_PT_LAG: Dict[str, str] = {"batter": "pa", "pitcher": "innings"}

# Stats where lower is better: the age adjustment is inverted for these so a
# below-peak player is projected to *improve* (a lower value), not inflate.
ROLE_LOWER_IS_BETTER: Dict[str, frozenset] = {
    "batter": frozenset({"k_pct"}),
    "pitcher": frozenset({"era", "whip", "bb_per_9", "hr_per_9"}),
}

# Every season-N target the projection is scored against, in report order.
ROLE_ALL_TARGETS: Dict[str, Tuple[str, ...]] = {
    "batter": ("pa", "avg", "obp", "slg", "ops", "bb_pct", "k_pct", "hr", "sb"),
    "pitcher": ("innings", "era", "whip", "k_per_9", "bb_per_9", "hr_per_9"),
}

# Optional identity columns carried straight through to the projection frame.
_ID_PASSTHROUGH = ("player_name", "primary_position")


@dataclass(frozen=True)
class MarcelConfig:
    """Hyperparameters for one Marcel baseline run.

    Defaults follow the classic Marcel: 5/4/3 recency weights, a peak age of 29
    with a +0.6%/yr climb below it and a -0.3%/yr decline above, and a
    ``0.5*PT_{n-1} + 0.1*PT_{n-2} + base`` playing-time projection. The
    regression amounts are Marcel-scale but tunable — they are the number of
    league-average PA (batters) / IP (pitchers), in the same 5/4/3-weighted units
    as the blended playing time, added when regressing toward the league mean.
    """

    n_lag_seasons: int = 3
    recency_weights: Tuple[float, ...] = (5.0, 4.0, 3.0)
    regression_pt: Dict[str, float] = field(
        default_factory=lambda: {"batter": 1200.0, "pitcher": 130.0}
    )
    pt_lag1_weight: float = 0.5
    pt_lag2_weight: float = 0.1
    pt_base: Dict[str, float] = field(
        default_factory=lambda: {"batter": 200.0, "pitcher": 60.0}
    )
    peak_age: float = 29.0
    age_slope_young: float = 0.006
    age_slope_old: float = 0.003

    def __post_init__(self) -> None:
        if self.n_lag_seasons < 1:
            raise ValueError(f"n_lag_seasons must be >= 1, got {self.n_lag_seasons}")
        if len(self.recency_weights) < self.n_lag_seasons:
            raise ValueError(
                f"recency_weights needs >= {self.n_lag_seasons} entries, "
                f"got {self.recency_weights}"
            )
        for role in VALID_ROLES:
            if self.regression_pt.get(role, 0.0) <= 0:
                raise ValueError(f"regression_pt[{role!r}] must be > 0")


def _num(df: pd.DataFrame, col: str) -> pd.Series:
    """Coerce a column to float, or an all-NaN series if the column is absent."""
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan)
    return pd.Series(np.nan, index=df.index, dtype="float64")


def _weighted_playing_time(df: pd.DataFrame, *, role: str, config: MarcelConfig) -> pd.Series:
    """Recency-weighted playing time ``sum_k recency_k * PT_k`` over the lag seasons.

    This is the denominator of the ``lag_weighted_*`` blends and the "how much do
    we trust this player" quantity that drives regression: it matches how the gold
    matrix weighted the rate blends, so the two stay consistent.
    """
    pt_lag = ROLE_PT_LAG[role]
    total = pd.Series(0.0, index=df.index)
    for k in range(1, config.n_lag_seasons + 1):
        pt = _num(df, f"lag{k}_{pt_lag}").clip(lower=0.0)
        weight = config.recency_weights[k - 1] * pt
        total = total + weight.where(pt.notna() & (weight > 0), 0.0)
    return total


def _weighted_count_rate(df: pd.DataFrame, stat: str, *, role: str, config: MarcelConfig) -> pd.Series:
    """Recency-weighted per-PA rate for a counting stat, from the lag counts.

    ``sum_k recency_k * count_k / sum_k recency_k * PT_k`` over lag seasons where
    both the count and playing time are present. All-missing rows stay NaN (and
    are fully regressed downstream).
    """
    pt_lag = ROLE_PT_LAG[role]
    numerator = pd.Series(0.0, index=df.index)
    denominator = pd.Series(0.0, index=df.index)
    for k in range(1, config.n_lag_seasons + 1):
        count = _num(df, f"lag{k}_{stat}")
        pt = _num(df, f"lag{k}_{pt_lag}").clip(lower=0.0)
        valid = count.notna() & pt.notna() & (pt > 0)
        weight = config.recency_weights[k - 1]
        numerator = numerator + (count * weight).where(valid, 0.0)
        denominator = denominator + (pt * weight).where(valid, 0.0)
    return numerator / denominator.replace(0.0, np.nan)


def _league_rate(rate: pd.Series, weighted_pt: pd.Series) -> float:
    """Playing-time-weighted league mean of a rate, the target of regression.

    Weighting by ``weighted_pt`` keeps from swinging the
    league mean the way an unweighted average would. Falls back to the simple mean
    if no row has positive weight, and to NaN if the rate is entirely missing.
    """
    valid = rate.notna() & weighted_pt.notna() & (weighted_pt > 0)
    if valid.any():
        w = weighted_pt.where(valid, 0.0)
        return float((rate.where(valid, 0.0) * w).sum() / w.sum())
    if rate.notna().any():
        return float(rate.mean())
    return float("nan")


def _regress_to_mean(
    rate: pd.Series, weighted_pt: pd.Series, league_rate: float, regression_pt: float
) -> pd.Series:
    """Regress a rate toward ``league_rate`` by adding ``regression_pt`` league PA.

    ``(rate * pt + league * R) / (pt + R)``: a full-time player (large ``pt``)
    barely moves; a rate that is missing (NaN, ``pt`` treated as 0) collapses to
    the league mean.
    """
    effective_pt = weighted_pt.where(rate.notna(), 0.0).fillna(0.0)
    numerator = rate.fillna(0.0) * effective_pt + league_rate * regression_pt
    denominator = effective_pt + regression_pt
    return numerator / denominator


def _age_factor(age: pd.Series, config: MarcelConfig) -> pd.Series:
    """Flat two-slope age multiplier: >1 below the peak age, <1 above it.

    Marcel's classic form — climb toward a peak, decline after it. Players with an
    unknown age get a neutral factor of 1.0 rather than being dropped.
    """
    age = pd.to_numeric(age, errors="coerce")
    below = 1.0 + (config.peak_age - age) * config.age_slope_young
    above = 1.0 - (age - config.peak_age) * config.age_slope_old
    factor = pd.Series(np.where(age < config.peak_age, below, above), index=age.index)
    # Guard against absurd ages producing a non-positive multiplier.
    factor = factor.clip(lower=0.1)
    return factor.where(age.notna(), 1.0)


def _apply_age(rate: pd.Series, factor: pd.Series, *, lower_is_better: bool) -> pd.Series:
    """Apply the age factor, inverting it for stats where lower is better."""
    return rate / factor if lower_is_better else rate * factor


def _project_playing_time(df: pd.DataFrame, *, role: str, config: MarcelConfig) -> pd.Series:
    """Marcel playing-time projection: ``0.5*PT_{n-1} + 0.1*PT_{n-2} + base``.

    Deliberately regressed toward a league-ish baseline (the ``base`` term) rather
    than tracking recent workload closely — Marcel treats playing time as noisy.
    """
    pt_lag = ROLE_PT_LAG[role]
    lag1 = _num(df, f"lag1_{pt_lag}").clip(lower=0.0).fillna(0.0)
    lag2 = _num(df, f"lag2_{pt_lag}").clip(lower=0.0).fillna(0.0)
    return config.pt_lag1_weight * lag1 + config.pt_lag2_weight * lag2 + config.pt_base[role]


def project_role_year(
    matrix: pd.DataFrame, *, role: str, year: int, config: MarcelConfig
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """Build Marcel projections for one role-year training matrix.

    Returns ``(projections, league_rates)``: one ``proj_*`` row per player (with
    the ``target_*`` actuals carried alongside for scoring), and the league rate
    used to regress each stat.
    """
    if role not in VALID_ROLES:
        raise ValueError(f"role must be one of {VALID_ROLES}, got '{role}'")

    df = matrix.reset_index(drop=True)
    weighted_pt = _weighted_playing_time(df, role=role, config=config)
    age = _num(df, "age")
    factor = _age_factor(age, config)
    lower = ROLE_LOWER_IS_BETTER[role]
    reg_pt = config.regression_pt[role]

    proj = pd.DataFrame(index=df.index)
    proj["player_id"] = df["player_id"].to_numpy()
    for col in _ID_PASSTHROUGH:
        if col in df.columns:
            proj[col] = df[col].to_numpy()
    proj["year"] = int(year)
    proj["role"] = role
    proj["age"] = age.to_numpy()

    league_rates: Dict[str, float] = {}

    # Rate targets: regress the weighted blend toward the league mean, then age-adjust.
    for stat in ROLE_RATE_TARGETS[role]:
        blended = _num(df, f"lag_weighted_{stat}")
        league = _league_rate(blended, weighted_pt)
        league_rates[stat] = league
        regressed = _regress_to_mean(blended, weighted_pt, league, reg_pt)
        proj[f"proj_{stat}"] = _apply_age(regressed, factor, lower_is_better=stat in lower)

    if role == "batter":
        # OPS is on-base plus slugging by definition.
        proj["proj_ops"] = proj["proj_obp"] + proj["proj_slg"]

    # Playing-time projection (also the scale for counting stats).
    pt_target = ROLE_PT_TARGET[role]
    projected_pt = _project_playing_time(df, role=role, config=config)
    proj[f"proj_{pt_target}"] = projected_pt

    # Counting targets: regress + age-adjust the per-PA rate, then scale onto projected PT.
    for stat in ROLE_COUNT_TARGETS[role]:
        rate = _weighted_count_rate(df, stat, role=role, config=config)
        league = _league_rate(rate, weighted_pt)
        league_rates[f"{stat}_per_pa"] = league
        regressed = _regress_to_mean(rate, weighted_pt, league, reg_pt)
        aged = _apply_age(regressed, factor, lower_is_better=stat in lower)
        proj[f"proj_{stat}"] = aged * projected_pt

    # Carry the actuals alongside so the projection file is self-contained for scoring.
    for stat in ROLE_ALL_TARGETS[role]:
        tcol = f"target_{stat}"
        if tcol in df.columns:
            proj[tcol] = _num(df, tcol).to_numpy()

    return proj, league_rates


def score_projections(proj: pd.DataFrame, *, role: str) -> Dict[str, Dict[str, float]]:
    """Per-stat error of ``proj_*`` against ``target_*`` over rows where both exist."""
    metrics: Dict[str, Dict[str, float]] = {}
    for stat in ROLE_ALL_TARGETS[role]:
        pcol, tcol = f"proj_{stat}", f"target_{stat}"
        if pcol not in proj.columns or tcol not in proj.columns:
            continue
        p = pd.to_numeric(proj[pcol], errors="coerce")
        a = pd.to_numeric(proj[tcol], errors="coerce")
        mask = p.notna() & a.notna()
        n = int(mask.sum())
        if n == 0:
            continue
        err = (p[mask] - a[mask]).to_numpy(dtype=float)
        metrics[stat] = {
            "n": n,
            "mae": float(np.mean(np.abs(err))),
            "rmse": float(np.sqrt(np.mean(err**2))),
            "bias": float(np.mean(err)),
            "actual_mean": float(a[mask].mean()),
            "proj_mean": float(p[mask].mean()),
        }
    return metrics


def _projection_metadata(
    *,
    role: str,
    year: int,
    n_players: int,
    league_rates: Dict[str, float],
    metrics: Dict[str, Dict[str, float]],
    config: MarcelConfig,
    source_matrix_key: str,
    projections_key: str,
) -> Dict[str, Any]:
    return {
        "role": role,
        "year": year,
        "method": "marcel_the_monkey_baseline",
        "n_players": n_players,
        "league_rates": league_rates,
        "metrics": metrics,
        "config": asdict(config),
        "source_training_matrix_s3_key": source_matrix_key,
        "projections_s3_key": projections_key,
        "generated_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "notes": (
            "5/4/3-weighted 3-year rates (from the gold training matrix), regressed "
            "to the playing-time-weighted league mean, with a flat age adjustment "
            "around a peak age of 29. Counting stats are the regressed per-PA rate "
            "scaled by a Marcel-projected playing time. Metrics compare proj_* to "
            "the actual season-N target_* stat line."
        ),
    }


def _write_json_to_s3(bucket: str, key: str, payload: Dict[str, Any]) -> None:
    client = get_s3_client()
    body = json.dumps(payload, indent=2, sort_keys=True, default=str).encode("utf-8")
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def build_marcel_baseline(
    *,
    bucket: str,
    gold_prefix: str,
    start_year: int,
    end_year: int,
    role_filter: str = "all",
    config: Optional[MarcelConfig] = None,
) -> Dict[str, Any]:
    """Build, write, and score Marcel projections for each role-year.

    Reads the gold performance-prediction training matrix per role-year, writes
    ``marcel_projections.parquet`` + ``marcel_metrics.json`` under
    ``{gold_prefix}/baselines/marcel/{role}/year=Y/``.
    """
    if start_year > end_year:
        return {
            "status": "error",
            "message": f"start_year ({start_year}) must be <= end_year ({end_year})",
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
        }

    valid_filters = (*VALID_ROLES, "all")
    if role_filter not in valid_filters:
        return {
            "status": "error",
            "message": f"role_filter must be one of {valid_filters}, got '{role_filter}'",
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
        }

    cfg = config or MarcelConfig()
    roles = VALID_ROLES if role_filter == "all" else (role_filter,)
    gp = gold_prefix.strip("/")

    rows_written = 0
    years_written: set[int] = set()
    role_years_processed: List[Dict[str, Any]] = []

    for role in roles:
        for year in range(start_year, end_year + 1):
            in_key = (
                f"{gp}/performance_prediction/{role}/year={year}/training_matrix.parquet"
            )
            matrix = read_parquet_from_s3(bucket, in_key, missing_key_log="none")
            if matrix is None or matrix.empty or "player_id" not in matrix.columns:
                continue

            proj, league_rates = project_role_year(
                matrix, role=role, year=year, config=cfg
            )
            metrics = score_projections(proj, role=role)

            out_key = f"{gp}/baselines/marcel/{role}/year={year}/marcel_projections.parquet"
            write_parquet_to_s3(proj, bucket, out_key, log_write=False)

            meta_key = f"{gp}/baselines/marcel/{role}/year={year}/marcel_metrics.json"
            meta = _projection_metadata(
                role=role,
                year=year,
                n_players=int(len(proj)),
                league_rates=league_rates,
                metrics=metrics,
                config=cfg,
                source_matrix_key=in_key,
                projections_key=out_key,
            )
            _write_json_to_s3(bucket, meta_key, meta)

            rows_written += len(proj)
            years_written.add(year)
            role_years_processed.append(
                {"role": role, "year": year, "rows": int(len(proj)), "metrics": metrics}
            )
            logger.info(
                "Marcel baseline wrote %d projections for role=%s year=%d to s3://%s/%s",
                len(proj),
                role,
                year,
                bucket,
                out_key,
            )

    if rows_written == 0:
        return {
            "status": "no_data",
            "message": (
                f"No training matrices found for roles={list(roles)} "
                f"years={start_year}..{end_year}"
            ),
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
        }

    sorted_years = sorted(years_written)
    message = (
        f"Marcel baseline wrote {rows_written} projections across years {sorted_years} "
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
    cfg = PipelineSettings.from_environ()
    cy = current_utc_year()
    parser = argparse.ArgumentParser(
        description=(
            "Build Marcel the Monkey baseline projections (5/4/3-weighted, "
            "league-regressed, age-adjusted) from the gold performance-prediction "
            "training matrix, and score them against the actual season-N stat line."
        )
    )
    parser.add_argument("--start-year", type=int, default=cy - 1)
    parser.add_argument("--end-year", type=int, default=cy)
    parser.add_argument("--bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument("--gold-prefix", type=str, default=cfg.gold_prefix)
    parser.add_argument(
        "--role",
        choices=("all", *VALID_ROLES),
        default="all",
        help="Build baselines for all roles or one specific role.",
    )
    parser.add_argument(
        "--batter-regression-pa",
        type=float,
        default=None,
        help="League-average PA added when regressing batters (default: 1200).",
    )
    parser.add_argument(
        "--pitcher-regression-ip",
        type=float,
        default=None,
        help="League-average IP added when regressing pitchers (default: 130).",
    )
    args = parser.parse_args()

    base = MarcelConfig()
    regression_pt = dict(base.regression_pt)
    if args.batter_regression_pa is not None:
        regression_pt["batter"] = args.batter_regression_pa
    if args.pitcher_regression_ip is not None:
        regression_pt["pitcher"] = args.pitcher_regression_ip
    marcel_cfg = MarcelConfig(regression_pt=regression_pt)

    result = build_marcel_baseline(
        bucket=args.bucket,
        gold_prefix=args.gold_prefix,
        start_year=args.start_year,
        end_year=args.end_year,
        role_filter=args.role,
        config=marcel_cfg,
    )

    if result["status"] == "error":
        logger.error(result["message"])
        raise SystemExit(1)
    if result["status"] == "no_data":
        logger.warning(result["message"])
    else:
        logger.info(result["message"])


def handler(event: dict, context) -> dict:
    cy = current_utc_year()
    cfg = PipelineSettings.from_environ()
    start_year = event_or_env_int(event, "start_year", "START_YEAR", cy - 1)
    end_year = event_or_env_int(event, "end_year", "END_YEAR", cy)
    bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    gold_prefix = event_or_env_str(event, "gold_prefix", "GOLD_PREFIX", cfg.gold_prefix)
    role = event_or_env_str(event, "role", "ROLE", "all")

    result = build_marcel_baseline(
        bucket=bucket,
        gold_prefix=gold_prefix,
        start_year=start_year,
        end_year=end_year,
        role_filter=role,
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
