#!/usr/bin/env python3
"""
Gold build orchestrator.

Runs every gold-layer preprocessing step in a single call:
  - archetypes:  silver player-year features -> gold preprocessed archetype tables
  - performance: silver stat lines + features -> gold performance-prediction
                 training matrices (lagged seasons -> season-N targets)

Both steps are season (year) scoped and default to last season through the current
season. Each step runs independently: a failure in one does not abort the others,
and the aggregated status reflects the worst outcome.
"""

from __future__ import annotations

import argparse
import logging
import time
from typing import Any, Dict, Sequence

from ...common.runtime_helpers import current_utc_year, event_or_env_int, event_or_env_str
from ...common.settings import PipelineSettings

logger = logging.getLogger(__name__)

ALL_STEPS = ("archetypes", "performance")


def build_all_gold(
    *,
    bucket: str,
    silver_prefix: str,
    gold_prefix: str,
    start_year: int,
    end_year: int,
    role_filter: str = "all",
    correlation_threshold: float = 0.95,
    near_zero_variance_unique_ratio: float = 0.005,
    n_lag_seasons: int = 3,
    steps: Sequence[str] = ALL_STEPS,
) -> Dict[str, Any]:
    """
    Run the selected gold preprocessing steps and aggregate their results.

    Returns a dict with:
      status:  "ok" (all steps ok) | "error" (all steps errored) | "partial" (mix)
      message: per-step status summary, e.g. "archetypes=ok performance=error"
      steps:   {step_name: raw sub-result dict}
      errors:  flattened, step-prefixed error strings
    """
    from .gold_archetype_preprocessing import build_silver_to_gold_preprocessing
    from .gold_performance_predict_preprocessing import (
        PerformancePredictConfig,
        build_performance_training_matrices,
    )

    unknown = [s for s in steps if s not in ALL_STEPS]
    if unknown:
        msg = f"Unknown step(s): {unknown}; valid: {list(ALL_STEPS)}"
        return {"status": "error", "message": msg, "steps": {}, "errors": [msg]}

    # Preserve canonical order and dedupe whatever the caller requested.
    enabled_steps = [s for s in ALL_STEPS if s in steps]
    if not enabled_steps:
        msg = "No steps selected."
        return {"status": "error", "message": msg, "steps": {}, "errors": [msg]}

    runners = {
        "archetypes": lambda: build_silver_to_gold_preprocessing(
            bucket=bucket,
            silver_prefix=silver_prefix,
            gold_prefix=gold_prefix,
            start_year=start_year,
            end_year=end_year,
            role_filter=role_filter,
            correlation_threshold=correlation_threshold,
            near_zero_variance_unique_ratio=near_zero_variance_unique_ratio,
        ),
        "performance": lambda: build_performance_training_matrices(
            bucket=bucket,
            silver_prefix=silver_prefix,
            gold_prefix=gold_prefix,
            start_year=start_year,
            end_year=end_year,
            role_filter=role_filter,
            config=PerformancePredictConfig(n_lag_seasons=n_lag_seasons),
        ),
    }

    results: Dict[str, Any] = {}
    errors: list[str] = []
    for step in enabled_steps:
        logger.info("Gold build: running step %r", step)
        step_started = time.monotonic()
        try:
            res = runners[step]()
        except Exception as exc:  # noqa: BLE001 - one step must not abort the others
            logger.exception("Gold step %r raised", step)
            res = {"status": "error", "message": f"{step} raised: {exc}", "errors": [str(exc)]}
        logger.info(
            "[timing] gold step %r: %.1fs (status=%s)",
            step,
            time.monotonic() - step_started,
            res.get("status"),
        )
        results[step] = res
        sub_errors = res.get("errors") or []
        for err in sub_errors:
            errors.append(f"{step}: {err}")
        if res.get("status") not in ("ok", "no_data") and not sub_errors:
            errors.append(f"{step}: {res.get('message', 'error')}")

    # "no_data" is not a failure: nothing to build still counts as ok for aggregation.
    statuses = {step: results[step].get("status") for step in enabled_steps}
    if all(s in ("ok", "no_data") for s in statuses.values()):
        overall = "ok"
    elif all(s == "error" for s in statuses.values()):
        overall = "error"
    else:
        overall = "partial"

    message = " ".join(f"{step}={statuses[step]}" for step in enabled_steps)
    return {"status": overall, "message": message, "steps": results, "errors": errors}


def main() -> None:
    cfg = PipelineSettings.from_environ()
    cy = current_utc_year()
    parser = argparse.ArgumentParser(
        description=(
            "Run all gold preprocessing steps (archetype preprocessing + "
            "performance-prediction training matrices) in one call."
        )
    )
    parser.add_argument("--start-year", type=int, default=cy - 1)
    parser.add_argument("--end-year", type=int, default=cy)
    parser.add_argument("--bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument("--silver-prefix", type=str, default=cfg.feature_prefix)
    parser.add_argument("--gold-prefix", type=str, default=cfg.gold_prefix)
    parser.add_argument(
        "--role",
        choices=("all", "batter", "pitcher", "catcher"),
        default="all",
        help="Run for all cohorts or one specific role.",
    )
    parser.add_argument("--correlation-threshold", type=float, default=0.95)
    parser.add_argument("--near-zero-variance-unique-ratio", type=float, default=0.005)
    parser.add_argument("--n-lag-seasons", type=int, default=3)
    parser.add_argument(
        "--steps",
        type=str,
        default=",".join(ALL_STEPS),
        help="Comma-separated subset of steps to run (archetypes,performance).",
    )
    args = parser.parse_args()

    steps = [s.strip() for s in args.steps.split(",") if s.strip()]

    result = build_all_gold(
        bucket=args.bucket,
        silver_prefix=args.silver_prefix,
        gold_prefix=args.gold_prefix,
        start_year=args.start_year,
        end_year=args.end_year,
        role_filter=args.role,
        correlation_threshold=args.correlation_threshold,
        near_zero_variance_unique_ratio=args.near_zero_variance_unique_ratio,
        n_lag_seasons=args.n_lag_seasons,
        steps=steps,
    )

    if result["status"] == "error":
        logger.error(result["message"])
        for err in result.get("errors", []):
            logger.error(err)
        raise SystemExit(1)
    if result["status"] == "partial":
        logger.warning(result["message"])
        for err in result.get("errors", []):
            logger.warning(err)
        raise SystemExit(1)
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
    corr_raw = event_or_env_str(event, "correlation_threshold", "CORRELATION_THRESHOLD", "0.95")
    nzv_raw = event_or_env_str(
        event,
        "near_zero_variance_unique_ratio",
        "NEAR_ZERO_VARIANCE_UNIQUE_RATIO",
        "0.005",
    )
    n_lag_seasons = event_or_env_int(event, "n_lag_seasons", "N_LAG_SEASONS", 3)

    # steps: accept a JSON list (event) or a comma-separated string (event/env).
    ev = event if isinstance(event, dict) else {}
    raw_steps = ev.get("steps")
    if isinstance(raw_steps, (list, tuple)):
        steps = [str(s).strip() for s in raw_steps if str(s).strip()]
    else:
        steps_str = event_or_env_str(event, "steps", "STEPS", ",".join(ALL_STEPS))
        steps = [s.strip() for s in steps_str.split(",") if s.strip()]

    result = build_all_gold(
        bucket=bucket,
        silver_prefix=silver_prefix,
        gold_prefix=gold_prefix,
        start_year=start_year,
        end_year=end_year,
        role_filter=role,
        correlation_threshold=float(corr_raw),
        near_zero_variance_unique_ratio=float(nzv_raw),
        n_lag_seasons=n_lag_seasons,
        steps=steps,
    )
    status_code = 200 if result["status"] == "ok" else (207 if result["status"] == "partial" else 400)
    return {"statusCode": status_code, **result}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
