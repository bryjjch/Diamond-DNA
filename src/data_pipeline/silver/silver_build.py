#!/usr/bin/env python3
"""
Silver build orchestrator.

Runs every silver-layer build step in a single call:
  - features: bronze Statcast dailies -> silver player-year archetype feature tables
              (date range, year-to-date by default)
  - standard: bronze standard stat lines -> standalone silver stat-line tables
              (year range)

The features step is date-scoped; standard stats are season (year) scoped. When the
year range is not supplied explicitly it defaults to last season through the current
season, matching the previous standalone standard-stats Lambda. Each step runs
independently: a failure in one does not abort the others, and the aggregated status
reflects the worst outcome.
"""

from __future__ import annotations

import argparse
import logging
import time
from typing import Any, Dict, Optional, Sequence

from ...common.runtime_helpers import current_utc_year, event_or_env_str, yesterday_utc_date_str
from ...common.settings import PipelineSettings

logger = logging.getLogger(__name__)

ALL_STEPS = ("features", "standard")


def build_all_silver(
    *,
    bucket: str,
    bronze_statcast_prefix: str,
    raw_running_prefix: str,
    raw_defence_prefix: str,
    raw_bio_prefix: str,
    raw_standard_stats_prefix: str,
    silver_prefix: str,
    start_date_str: str,
    end_date_str: str,
    year_to_date: bool = True,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    steps: Sequence[str] = ALL_STEPS,
) -> Dict[str, Any]:
    """
    Run the selected silver build steps and aggregate their results.

    Returns a dict with:
      status:  "ok" (all steps ok) | "error" (all steps errored) | "partial" (mix)
      message: per-step status summary, e.g. "features=ok standard=error"
      steps:   {step_name: raw sub-result dict}
      errors:  flattened, step-prefixed error strings
    """
    from .archetype_features.silver_features_build import build_bronze_to_silver_features
    from .standard_stats.silver_standard_player_year_stats import build_standard_stats_range

    unknown_steps = [s for s in steps if s not in ALL_STEPS]
    if unknown_steps:
        msg = f"Unknown step(s): {unknown_steps}; valid: {list(ALL_STEPS)}"
        return {"status": "error", "message": msg, "steps": {}, "errors": [msg]}

    # Preserve canonical order and dedupe whatever the caller requested.
    enabled_steps = [s for s in ALL_STEPS if s in steps]
    if not enabled_steps:
        msg = "No steps selected."
        return {"status": "error", "message": msg, "steps": {}, "errors": [msg]}

    start_yr = start_year if start_year is not None else current_utc_year() - 1
    end_yr = end_year if end_year is not None else current_utc_year()

    runners = {
        "features": lambda: build_bronze_to_silver_features(
            bucket=bucket,
            bronze_statcast_prefix=bronze_statcast_prefix,
            silver_prefix=silver_prefix,
            start_date_str=start_date_str,
            end_date_str=end_date_str,
            year_to_date=year_to_date,
            raw_running_prefix=raw_running_prefix,
            raw_defence_prefix=raw_defence_prefix,
            raw_bio_prefix=raw_bio_prefix,
        ),
        "standard": lambda: build_standard_stats_range(
            bucket=bucket,
            raw_standard_stats_prefix=raw_standard_stats_prefix,
            silver_prefix=silver_prefix,
            start_year=start_yr,
            end_year=end_yr,
        ),
    }

    results: Dict[str, Any] = {}
    errors: list[str] = []
    for step in enabled_steps:
        logger.info("Silver build: running step %r", step)
        step_started = time.monotonic()
        try:
            res = runners[step]()
        except Exception as exc:  # noqa: BLE001 - one step must not abort the others
            logger.exception("Silver step %r raised", step)
            res = {"status": "error", "message": f"{step} raised: {exc}", "errors": [str(exc)]}
        logger.info(
            "[timing] silver step %r: %.1fs (status=%s)",
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
    yesterday = yesterday_utc_date_str()
    parser = argparse.ArgumentParser(
        description=(
            "Run all silver build steps (archetype feature tables + standard stat-line "
            "tables) in one call. Features use the date range; standard stats use the "
            "year range, which defaults to last season through the current season."
        )
    )
    parser.add_argument("--start-date", type=str, default=yesterday)
    parser.add_argument("--end-date", type=str, default=yesterday)
    parser.add_argument(
        "--no-year-to-date",
        action="store_true",
        help="Only load bronze for [start-date, end-date] exactly (no Jan 1 expansion).",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help="Standard-stats start year (default: current year - 1).",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Standard-stats end year (default: current year).",
    )
    parser.add_argument("--bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument("--bronze-prefix", type=str, default=cfg.raw_statcast_prefix)
    parser.add_argument("--running-prefix", type=str, default=cfg.raw_running_prefix)
    parser.add_argument("--defence-prefix", type=str, default=cfg.raw_defence_prefix)
    parser.add_argument("--bio-prefix", type=str, default=cfg.raw_bio_prefix)
    parser.add_argument("--standard-prefix", type=str, default=cfg.raw_standard_stats_prefix)
    parser.add_argument("--silver-prefix", type=str, default=cfg.feature_prefix)
    parser.add_argument(
        "--steps",
        type=str,
        default=",".join(ALL_STEPS),
        help="Comma-separated subset of steps to run (features,standard).",
    )
    args = parser.parse_args()

    steps = [s.strip() for s in args.steps.split(",") if s.strip()]

    result = build_all_silver(
        bucket=args.bucket,
        bronze_statcast_prefix=args.bronze_prefix,
        raw_running_prefix=args.running_prefix,
        raw_defence_prefix=args.defence_prefix,
        raw_bio_prefix=args.bio_prefix,
        raw_standard_stats_prefix=args.standard_prefix,
        silver_prefix=args.silver_prefix,
        start_date_str=args.start_date,
        end_date_str=args.end_date,
        year_to_date=not args.no_year_to_date,
        start_year=args.start_year,
        end_year=args.end_year,
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
    y = yesterday_utc_date_str()
    cfg = PipelineSettings.from_environ()
    start_date = event_or_env_str(event, "start_date", "START_DATE", y)
    end_date = event_or_env_str(event, "end_date", "END_DATE", y)
    bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    bronze_prefix = event_or_env_str(event, "bronze_prefix", "RAW_PREFIX", cfg.raw_statcast_prefix)
    running_prefix = event_or_env_str(
        event, "raw_running_prefix", "RAW_RUNNING_PREFIX", cfg.raw_running_prefix
    )
    defence_prefix = event_or_env_str(
        event, "raw_defence_prefix", "RAW_DEFENCE_PREFIX", cfg.raw_defence_prefix
    )
    bio_prefix = event_or_env_str(event, "raw_bio_prefix", "RAW_BIO_PREFIX", cfg.raw_bio_prefix)
    standard_prefix = event_or_env_str(
        event, "raw_standard_stats_prefix", "RAW_STANDARD_STATS_PREFIX", cfg.raw_standard_stats_prefix
    )
    silver_prefix = event_or_env_str(event, "silver_prefix", "FEATURE_PREFIX", cfg.feature_prefix)
    yt_raw = event_or_env_str(event, "year_to_date", "YEAR_TO_DATE", "true")
    year_to_date = str(yt_raw).strip().lower() not in ("0", "false", "no")

    # Year range is optional; empty -> None so the orchestrator applies its defaults.
    sy_raw = event_or_env_str(event, "start_year", "START_YEAR", "")
    ey_raw = event_or_env_str(event, "end_year", "END_YEAR", "")
    try:
        start_year = int(sy_raw) if str(sy_raw).strip() else None
        end_year = int(ey_raw) if str(ey_raw).strip() else None
    except ValueError:
        return {
            "statusCode": 400,
            "status": "error",
            "message": "START_YEAR and END_YEAR must be integers when set.",
        }

    # steps: accept a JSON list (event) or a comma-separated string (event/env).
    ev = event if isinstance(event, dict) else {}
    raw_steps = ev.get("steps")
    if isinstance(raw_steps, (list, tuple)):
        steps = [str(s).strip() for s in raw_steps if str(s).strip()]
    else:
        steps_str = event_or_env_str(event, "steps", "STEPS", ",".join(ALL_STEPS))
        steps = [s.strip() for s in steps_str.split(",") if s.strip()]

    result = build_all_silver(
        bucket=bucket,
        bronze_statcast_prefix=bronze_prefix,
        raw_running_prefix=running_prefix,
        raw_defence_prefix=defence_prefix,
        raw_bio_prefix=bio_prefix,
        raw_standard_stats_prefix=standard_prefix,
        silver_prefix=silver_prefix,
        start_date_str=start_date,
        end_date_str=end_date,
        year_to_date=year_to_date,
        start_year=start_year,
        end_year=end_year,
        steps=steps,
    )
    status_code = 200 if result["status"] == "ok" else (207 if result["status"] == "partial" else 400)
    return {"statusCode": status_code, **result}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
