#!/usr/bin/env python3
"""
XGBoost walk-forward projections — one gradient-boosted regressor per
(role, target stat), scored against the same line as the Marcel baseline.

For each target year Y the model trains on every gold training matrix from a
season strictly before Y (walk-forward: no future information leaks into the
fit), holds out the most recent training season(s) as an early-stopping eval
set, then predicts the season-Y stat line for every player in the year-Y
matrix. Projections are scored with the Marcel baseline's ``score_projections``
so the two models' ``metrics.json`` sidecars are directly comparable per
role-year-stat.

Inputs come from the gold performance-prediction training matrix
(``{gold_prefix}/performance_prediction/{role}/year=Y/training_matrix.parquet``,
built by ``gold_performance_predict_preprocessing``). Features are the ``age``,
``lag{k}_*`` (+ availability flags), ``lag_weighted_*``, and ``skill_lag1_*``
columns; missing values stay NaN and are routed by XGBoost's native
missing-value handling, matching the matrix's deliberate no-imputation policy.
Rows whose ``target_*`` actuals are absent are still projected (score masking
handles them), so a run against a season with no actuals yet is a true
forecast.

``ops`` is derived as ``proj_obp + proj_slg`` rather than modeled, keeping the
slash line internally consistent (same choice as Marcel). A row with every
feature missing still gets a prediction (XGBoost's default-direction paths),
analogous to Marcel collapsing an unknown player to the league mean.

Outputs per role-year:

- ``{predictions_prefix}/xgboost/{role}/year=Y/xgboost_projections.parquet`` —
  one row per player, ``proj_*`` alongside the ``target_*`` actuals.
- ``{models_prefix}/xgboost/{role}/year=Y/model.joblib`` — a bundle holding the
  per-stat fitted regressors, the frozen feature-column list, and training
  metadata. Pickled boosters are coupled to the xgboost major version (recorded
  in the bundle); switching to xgboost's native JSON serialization is the
  upgrade path if bundles ever need to outlive library upgrades.
- ``{models_prefix}/xgboost/{role}/year=Y/metrics.json`` — per-stat
  MAE / RMSE / bias plus walk-forward training info (train years, eval years,
  best iterations, eval RMSE).
"""

from __future__ import annotations

import argparse
import io
import json
import logging
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import joblib
import numpy as np
import pandas as pd
import xgboost

from ...common.runtime_helpers import current_utc_year, event_or_env_int, event_or_env_str
from ...common.lake_keys import (
    PERFORMANCE_PREDICTION,
    gold_feature_key,
    model_key,
    prediction_key,
)
from ...common.s3_helpers import get_s3_client, read_parquet_from_s3, write_parquet_to_s3
from ...common.settings import PipelineSettings
from ..baselines.marcel_baseline import ROLE_ALL_TARGETS, VALID_ROLES, score_projections

logger = logging.getLogger(__name__)

MODEL_NAME = "xgboost"
PROJECTIONS_FILENAME = "xgboost_projections.parquet"
BUNDLE_FILENAME = "model.joblib"
METRICS_FILENAME = "metrics.json"

# Identity columns of the gold matrix; never features. Kept as a local tuple
# (like Marcel keeps its column maps) so src.ml stays import-independent of
# src.data_pipeline.
_ID_COLUMNS = ("player_id", "year", "role", "player_name", "primary_position")

# Optional identity columns carried straight through to the projection frame.
_ID_PASSTHROUGH = ("player_name", "primary_position")

# Feature families: lag{k}_*, lag{k}_available, lag_weighted_*, skill_lag1_*.
_FEATURE_PREFIXES = ("lag", "skill_lag1_")
_EXTRA_FEATURE_COLUMNS = ("age",)

# A stat needs at least this many rows with a non-missing target to get a model.
_MIN_TRAIN_ROWS_PER_STAT = 20

# When the target year is Y, matrices are loaded back to Y - this many seasons
# unless --history-start-year says otherwise.
_DEFAULT_HISTORY_SEASONS = 10


@dataclass(frozen=True)
class XGBoostProjectionConfig:
    """Hyperparameters for one XGBoost projection run.

    The booster params are shared by every per-stat regressor: shallow trees
    with row/column subsampling and a modest learning rate, sized for a few
    thousand rows and ~100 features per role. ``n_estimators`` is a ceiling —
    early stopping against the most recent held-out training season picks the
    real round count per stat.
    """

    # Booster parameters (per-stat XGBRegressor kwargs).
    n_estimators: int = 1000
    learning_rate: float = 0.05
    max_depth: int = 4
    min_child_weight: float = 5.0
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    reg_lambda: float = 1.0
    reg_alpha: float = 0.0
    objective: str = "reg:squarederror"
    tree_method: str = "hist"
    random_state: int = 42
    n_jobs: int = -1
    # Walk-forward / early stopping.
    min_train_years: int = 2
    validation_years: int = 1
    early_stopping_rounds: int = 50
    # Output behavior.
    derive_ops: bool = True
    clip_predictions_at_zero: bool = True

    def __post_init__(self) -> None:
        if self.n_estimators < 1:
            raise ValueError(f"n_estimators must be >= 1, got {self.n_estimators}")
        if self.learning_rate <= 0:
            raise ValueError(f"learning_rate must be > 0, got {self.learning_rate}")
        if self.max_depth < 1:
            raise ValueError(f"max_depth must be >= 1, got {self.max_depth}")
        if not 0 < self.subsample <= 1:
            raise ValueError(f"subsample must be in (0, 1], got {self.subsample}")
        if not 0 < self.colsample_bytree <= 1:
            raise ValueError(
                f"colsample_bytree must be in (0, 1], got {self.colsample_bytree}"
            )
        if self.min_train_years < 1:
            raise ValueError(f"min_train_years must be >= 1, got {self.min_train_years}")
        if self.validation_years < 0:
            raise ValueError(
                f"validation_years must be >= 0, got {self.validation_years}"
            )
        if self.early_stopping_rounds < 0:
            raise ValueError(
                f"early_stopping_rounds must be >= 0, got {self.early_stopping_rounds}"
            )
        if self.early_stopping_rounds > 0 and self.validation_years < 1:
            raise ValueError(
                "early_stopping_rounds > 0 requires validation_years >= 1"
            )

    def booster_params(self) -> Dict[str, Any]:
        """Constructor kwargs shared by every per-stat XGBRegressor."""
        return {
            "n_estimators": self.n_estimators,
            "learning_rate": self.learning_rate,
            "max_depth": self.max_depth,
            "min_child_weight": self.min_child_weight,
            "subsample": self.subsample,
            "colsample_bytree": self.colsample_bytree,
            "reg_lambda": self.reg_lambda,
            "reg_alpha": self.reg_alpha,
            "objective": self.objective,
            "tree_method": self.tree_method,
            "random_state": self.random_state,
            "n_jobs": self.n_jobs,
        }


def modeled_target_stats(role: str, config: XGBoostProjectionConfig) -> Tuple[str, ...]:
    """Target stats that get their own regressor (ops is derived, not modeled)."""
    stats = ROLE_ALL_TARGETS[role]
    if role == "batter" and config.derive_ops:
        return tuple(s for s in stats if s != "ops")
    return stats


def select_feature_columns(matrix: pd.DataFrame) -> List[str]:
    """Numeric model inputs: ``age`` plus the lag / weighted / skill families.

    Identity columns and ``target_*`` are always excluded; non-numeric columns
    (e.g. a stray string column that happens to match a prefix) are dropped
    rather than coerced. Sorted so the order is deterministic across runs.
    """
    columns: List[str] = []
    for col in matrix.columns:
        if col in _ID_COLUMNS or col.startswith("target_"):
            continue
        if col not in _EXTRA_FEATURE_COLUMNS and not col.startswith(_FEATURE_PREFIXES):
            continue
        if not pd.api.types.is_numeric_dtype(matrix[col]):
            continue
        columns.append(col)
    return sorted(columns)


def _feature_frame(df: pd.DataFrame, feature_columns: List[str]) -> pd.DataFrame:
    """Align a matrix to the frozen feature list as floats with NaN for gaps.

    ``reindex`` makes schema drift degrade gracefully: a column added upstream
    is ignored, a column dropped upstream becomes all-NaN and rides XGBoost's
    missing-value paths instead of crashing.
    """
    aligned = df.reindex(columns=feature_columns)
    out = pd.DataFrame(index=df.index)
    for col in feature_columns:
        out[col] = pd.to_numeric(aligned[col], errors="coerce").replace(
            [np.inf, -np.inf], np.nan
        )
    return out.astype("float64")


def _num(df: pd.DataFrame, col: str) -> pd.Series:
    """Coerce a column to float, or an all-NaN series if the column is absent."""
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan)
    return pd.Series(np.nan, index=df.index, dtype="float64")


def train_role_year_models(
    train_matrix: pd.DataFrame,
    *,
    role: str,
    feature_columns: List[str],
    config: XGBoostProjectionConfig,
) -> Tuple[Dict[str, xgboost.XGBRegressor], Dict[str, Any]]:
    """Fit one regressor per modeled target stat on the pre-target-year pool.

    The most recent ``validation_years`` seasons in ``train_matrix`` become the
    early-stopping eval set (validating on the season most similar to the
    prediction year); the rest are fit rows. When early stopping is disabled,
    no eval season can be spared, or a stat has no eval rows, that stat trains
    on the full pool at the fixed ``n_estimators`` ceiling.

    Returns ``(models_by_stat, training_info)``; stats with fewer than
    ``_MIN_TRAIN_ROWS_PER_STAT`` usable rows are skipped and recorded in
    ``training_info["skipped_stats"]``.
    """
    if role not in VALID_ROLES:
        raise ValueError(f"role must be one of {VALID_ROLES}, got '{role}'")

    df = train_matrix.reset_index(drop=True)
    years = sorted(pd.to_numeric(df["year"], errors="coerce").dropna().unique().astype(int))

    eval_years: List[int] = []
    if config.early_stopping_rounds > 0 and len(years) > config.validation_years:
        eval_years = list(years[-config.validation_years :])
    fit_years = [y for y in years if y not in eval_years]

    year_col = pd.to_numeric(df["year"], errors="coerce")
    fit_rows = year_col.isin(fit_years)
    eval_rows = year_col.isin(eval_years)

    features = _feature_frame(df, feature_columns)

    models: Dict[str, xgboost.XGBRegressor] = {}
    training_info: Dict[str, Any] = {
        "train_years": [int(y) for y in years],
        "fit_years": [int(y) for y in fit_years],
        "eval_years": [int(y) for y in eval_years],
        "n_rows": {},
        "best_iterations": {},
        "eval_rmse": {},
        "skipped_stats": [],
    }

    for stat in modeled_target_stats(role, config):
        target = _num(df, f"target_{stat}")
        fit_mask = fit_rows & target.notna()
        eval_mask = eval_rows & target.notna()
        use_eval = bool(eval_mask.any()) and config.early_stopping_rounds > 0

        # A stat with no eval rows falls back to fitting on the whole pool.
        train_mask = fit_mask if use_eval else (fit_mask | eval_mask)
        n_rows = int(train_mask.sum())
        if n_rows < _MIN_TRAIN_ROWS_PER_STAT:
            training_info["skipped_stats"].append(stat)
            logger.warning(
                "Skipping %s/%s: only %d rows with target_%s (< %d)",
                role,
                stat,
                n_rows,
                stat,
                _MIN_TRAIN_ROWS_PER_STAT,
            )
            continue

        params = config.booster_params()
        if use_eval:
            params["early_stopping_rounds"] = config.early_stopping_rounds
        model = xgboost.XGBRegressor(**params)

        X_train = features.loc[train_mask]
        y_train = target[train_mask]
        if use_eval:
            model.fit(
                X_train,
                y_train,
                eval_set=[(features.loc[eval_mask], target[eval_mask])],
                verbose=False,
            )
            best_iteration = int(model.best_iteration)
            eval_rmse = float(model.best_score)
        else:
            model.fit(X_train, y_train, verbose=False)
            best_iteration = None
            eval_rmse = None

        models[stat] = model
        training_info["n_rows"][stat] = n_rows
        training_info["best_iterations"][stat] = best_iteration
        training_info["eval_rmse"][stat] = eval_rmse

    return models, training_info


def _postprocess_predictions(
    pred: np.ndarray, *, config: XGBoostProjectionConfig
) -> np.ndarray:
    """Every target in both roles is non-negative; clip tree extrapolation below 0."""
    pred = np.asarray(pred, dtype="float64")
    if config.clip_predictions_at_zero:
        return np.clip(pred, 0.0, None)
    return pred


def predict_role_year(
    matrix: pd.DataFrame,
    models: Dict[str, xgboost.XGBRegressor],
    *,
    role: str,
    year: int,
    feature_columns: List[str],
    config: XGBoostProjectionConfig,
) -> pd.DataFrame:
    """Project the season-``year`` stat line for every row of the year matrix.

    Output schema matches the Marcel projections file: identity columns,
    ``proj_*`` for every stat in ``ROLE_ALL_TARGETS[role]`` (NaN for stats whose
    model was skipped), and the ``target_*`` actuals carried alongside so the
    file is self-contained for scoring.
    """
    if role not in VALID_ROLES:
        raise ValueError(f"role must be one of {VALID_ROLES}, got '{role}'")

    df = matrix.reset_index(drop=True)
    features = _feature_frame(df, feature_columns)

    proj = pd.DataFrame(index=df.index)
    proj["player_id"] = df["player_id"].to_numpy()
    for col in _ID_PASSTHROUGH:
        if col in df.columns:
            proj[col] = df[col].to_numpy()
    proj["year"] = int(year)
    proj["role"] = role
    proj["age"] = _num(df, "age").to_numpy()

    derive_ops = role == "batter" and config.derive_ops
    for stat in ROLE_ALL_TARGETS[role]:
        if stat == "ops" and derive_ops:
            # OPS is on-base plus slugging by definition (obp/slg precede it).
            proj["proj_ops"] = proj["proj_obp"] + proj["proj_slg"]
            continue
        model = models.get(stat)
        if model is None:
            proj[f"proj_{stat}"] = np.nan
            continue
        pred = model.predict(features)
        proj[f"proj_{stat}"] = _postprocess_predictions(pred, config=config)

    # Carry the actuals alongside so the projection file is self-contained for scoring.
    for stat in ROLE_ALL_TARGETS[role]:
        tcol = f"target_{stat}"
        if tcol in df.columns:
            proj[tcol] = _num(df, tcol).to_numpy()

    return proj


def _projection_metadata(
    *,
    role: str,
    year: int,
    n_players: int,
    metrics: Dict[str, Dict[str, float]],
    training_info: Dict[str, Any],
    n_features: int,
    config: XGBoostProjectionConfig,
    source_matrix_key: str,
    projections_key: str,
    bundle_key: str,
) -> Dict[str, Any]:
    return {
        "role": role,
        "year": year,
        "method": "xgboost_walk_forward",
        "n_players": n_players,
        "metrics": metrics,
        "train_years": training_info["train_years"],
        "eval_years": training_info["eval_years"],
        "n_train_rows": training_info["n_rows"],
        "n_features": n_features,
        "best_iterations": training_info["best_iterations"],
        "eval_rmse": training_info["eval_rmse"],
        "skipped_stats": training_info["skipped_stats"],
        "config": asdict(config),
        "xgboost_version": xgboost.__version__,
        "source_training_matrix_s3_key": source_matrix_key,
        "projections_s3_key": projections_key,
        "model_bundle_s3_key": bundle_key,
        "generated_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "notes": (
            "One XGBoost regressor per target stat, trained walk-forward on all "
            "gold training matrices from seasons strictly before the target year, "
            "with the most recent training season(s) held out for early stopping. "
            "ops is derived as proj_obp + proj_slg. Metrics compare proj_* to the "
            "actual season-N target_* stat line with the same scorer as the "
            "Marcel baseline."
        ),
    }


def _write_json_to_s3(bucket: str, key: str, payload: Dict[str, Any]) -> None:
    client = get_s3_client()
    body = json.dumps(payload, indent=2, sort_keys=True, default=str).encode("utf-8")
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


def build_xgboost_projections(
    *,
    bucket: str,
    gold_prefix: str,
    predictions_prefix: str,
    models_prefix: str,
    start_year: int,
    end_year: int,
    history_start_year: Optional[int] = None,
    role_filter: str = "all",
    config: Optional[XGBoostProjectionConfig] = None,
) -> Dict[str, Any]:
    """Train, project, score, and write XGBoost projections for each role-year.

    Walk-forward per target year Y in ``[start_year, end_year]``: train on every
    matrix year in ``[history_start_year, Y)`` (default reaches back
    ``start_year - 10``), predict the year-Y matrix, score with the Marcel
    scorer, and write the projections parquet, the joblib model bundle, and the
    ``metrics.json`` sidecar. Target years with fewer than
    ``config.min_train_years`` prior seasons are skipped and reported.
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

    if history_start_year is None:
        history_start_year = start_year - _DEFAULT_HISTORY_SEASONS
    if history_start_year >= start_year:
        return {
            "status": "error",
            "message": (
                f"history_start_year ({history_start_year}) must be < "
                f"start_year ({start_year})"
            ),
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
        }

    cfg = config or XGBoostProjectionConfig()
    roles = VALID_ROLES if role_filter == "all" else (role_filter,)

    rows_written = 0
    years_written: set[int] = set()
    role_years_processed: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    for role in roles:
        # One S3 read per role-year, shared by every target year's training pool.
        matrices: Dict[int, pd.DataFrame] = {}
        matrix_keys: Dict[int, str] = {}
        for year in range(history_start_year, end_year + 1):
            in_key = gold_feature_key(
                gold_prefix, PERFORMANCE_PREDICTION, role, year, "training_matrix.parquet"
            )
            matrix = read_parquet_from_s3(bucket, in_key, missing_key_log="none")
            if matrix is None or matrix.empty or "player_id" not in matrix.columns:
                continue
            matrices[year] = matrix
            matrix_keys[year] = in_key

        for year in range(start_year, end_year + 1):
            if year not in matrices:
                continue
            prior_years = sorted(y for y in matrices if y < year)
            if len(prior_years) < cfg.min_train_years:
                skipped.append(
                    {
                        "role": role,
                        "year": year,
                        "reason": (
                            f"only {len(prior_years)} prior season(s) available, "
                            f"need {cfg.min_train_years}"
                        ),
                    }
                )
                continue

            train_matrix = pd.concat(
                [matrices[y] for y in prior_years], ignore_index=True
            )
            feature_columns = select_feature_columns(train_matrix)
            models, training_info = train_role_year_models(
                train_matrix, role=role, feature_columns=feature_columns, config=cfg
            )
            if not models:
                skipped.append(
                    {"role": role, "year": year, "reason": "no trainable target stats"}
                )
                continue

            proj = predict_role_year(
                matrices[year],
                models,
                role=role,
                year=year,
                feature_columns=feature_columns,
                config=cfg,
            )
            metrics = score_projections(proj, role=role)

            out_key = prediction_key(
                predictions_prefix, MODEL_NAME, role, year, PROJECTIONS_FILENAME
            )
            write_parquet_to_s3(proj, bucket, out_key, log_write=False)

            bundle_key = model_key(models_prefix, MODEL_NAME, role, year, BUNDLE_FILENAME)
            bundle = {
                "models": models,
                "feature_columns": feature_columns,
                "role": role,
                "year": year,
                "train_years": training_info["train_years"],
                "eval_years": training_info["eval_years"],
                "best_iterations": training_info["best_iterations"],
                # Stored as a plain dict so unpickling never needs this module.
                "config": asdict(cfg),
                "xgboost_version": xgboost.__version__,
            }
            _write_joblib_to_s3(bundle, bucket, bundle_key)

            meta_key = model_key(models_prefix, MODEL_NAME, role, year, METRICS_FILENAME)
            meta = _projection_metadata(
                role=role,
                year=year,
                n_players=int(len(proj)),
                metrics=metrics,
                training_info=training_info,
                n_features=len(feature_columns),
                config=cfg,
                source_matrix_key=matrix_keys[year],
                projections_key=out_key,
                bundle_key=bundle_key,
            )
            _write_json_to_s3(bucket, meta_key, meta)

            rows_written += len(proj)
            years_written.add(year)
            role_years_processed.append(
                {"role": role, "year": year, "rows": int(len(proj)), "metrics": metrics}
            )
            logger.info(
                "XGBoost wrote %d projections for role=%s year=%d "
                "(trained on %s) to s3://%s/%s",
                len(proj),
                role,
                year,
                training_info["train_years"],
                bucket,
                out_key,
            )

    skip_note = ""
    if skipped:
        skip_note = " Skipped: " + "; ".join(
            f"{s['role']}/{s['year']} ({s['reason']})" for s in skipped
        )

    if rows_written == 0:
        return {
            "status": "no_data",
            "message": (
                f"No projectable role-years for roles={list(roles)} "
                f"years={start_year}..{end_year} "
                f"(history from {history_start_year}).{skip_note}"
            ),
            "years_written": [],
            "rows_written": 0,
            "role_years_processed": [],
        }

    sorted_years = sorted(years_written)
    message = (
        f"XGBoost wrote {rows_written} projections across years {sorted_years} "
        f"for roles {list(roles)}.{skip_note}"
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
            "Train walk-forward XGBoost projections (one regressor per role and "
            "target stat) from the gold performance-prediction training matrices, "
            "and score them against the actual season-N stat line."
        )
    )
    parser.add_argument("--start-year", type=int, default=cy - 1)
    parser.add_argument("--end-year", type=int, default=cy)
    parser.add_argument(
        "--history-start-year",
        type=int,
        default=None,
        help=(
            "Earliest season loaded into the training pool "
            "(default: start-year minus 10)."
        ),
    )
    parser.add_argument("--bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument("--gold-prefix", type=str, default=cfg.gold_prefix)
    parser.add_argument("--predictions-prefix", type=str, default=cfg.predictions_prefix)
    parser.add_argument("--models-prefix", type=str, default=cfg.models_prefix)
    parser.add_argument(
        "--role",
        choices=("all", *VALID_ROLES),
        default="all",
        help="Build projections for all roles or one specific role.",
    )
    parser.add_argument("--n-estimators", type=int, default=None)
    parser.add_argument("--max-depth", type=int, default=None)
    parser.add_argument("--learning-rate", type=float, default=None)
    parser.add_argument("--early-stopping-rounds", type=int, default=None)
    parser.add_argument("--min-train-years", type=int, default=None)
    args = parser.parse_args()

    overrides = {
        "n_estimators": args.n_estimators,
        "max_depth": args.max_depth,
        "learning_rate": args.learning_rate,
        "early_stopping_rounds": args.early_stopping_rounds,
        "min_train_years": args.min_train_years,
    }
    xgb_cfg = replace(
        XGBoostProjectionConfig(),
        **{k: v for k, v in overrides.items() if v is not None},
    )

    result = build_xgboost_projections(
        bucket=args.bucket,
        gold_prefix=args.gold_prefix,
        predictions_prefix=args.predictions_prefix,
        models_prefix=args.models_prefix,
        start_year=args.start_year,
        end_year=args.end_year,
        history_start_year=args.history_start_year,
        role_filter=args.role,
        config=xgb_cfg,
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
    history_start_year = event_or_env_int(
        event, "history_start_year", "HISTORY_START_YEAR", 0
    )
    bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    gold_prefix = event_or_env_str(event, "gold_prefix", "GOLD_PREFIX", cfg.gold_prefix)
    predictions_prefix = event_or_env_str(
        event, "predictions_prefix", "PREDICTIONS_PREFIX", cfg.predictions_prefix
    )
    models_prefix = event_or_env_str(event, "models_prefix", "MODELS_PREFIX", cfg.models_prefix)
    role = event_or_env_str(event, "role", "ROLE", "all")

    result = build_xgboost_projections(
        bucket=bucket,
        gold_prefix=gold_prefix,
        predictions_prefix=predictions_prefix,
        models_prefix=models_prefix,
        start_year=start_year,
        end_year=end_year,
        history_start_year=history_start_year or None,
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
