"""CLI entrypoints (argparse) for pipeline stages."""

from __future__ import annotations

import argparse
import logging
import os
import sys

from .runtime import current_utc_year, yesterday_utc_date_str
from .settings import PipelineSettings

logger = logging.getLogger(__name__)


def run_statcast_ingestion_main() -> None:
    from ..ingestion.statcast_ingestion import ingest_date_range

    cfg = PipelineSettings.from_environ()
    yesterday = yesterday_utc_date_str()
    parser = argparse.ArgumentParser(
        description="Ingest Statcast pitch data from pybaseball to S3 (date range; one file per day)"
    )
    parser.add_argument("--start-date", type=str, default=yesterday)
    parser.add_argument("--end-date", type=str, default=yesterday)
    parser.add_argument("--s3-bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument(
        "--s3-prefix",
        type=str,
        default=cfg.raw_statcast_prefix,
        help="S3 prefix for bronze Statcast pitches (env: S3_PREFIX or RAW_PREFIX)",
    )
    args = parser.parse_args()

    result = ingest_date_range(args.start_date, args.end_date, args.s3_bucket, args.s3_prefix)

    if result["status"] == "error":
        logger.error(result["message"])
        if result.get("errors"):
            for err in result["errors"]:
                logger.error(err)
        sys.exit(1)
    if result["status"] == "partial":
        logger.warning(result["message"])
        for err in result.get("errors", []):
            logger.warning(err)
        sys.exit(1)
    logger.info(result["message"])
    sys.exit(0)


def run_bronze_to_silver_features_main() -> None:
    from ..features.bronze_to_silver_features import build_bronze_to_silver_features

    cfg = PipelineSettings.from_environ()
    yesterday = yesterday_utc_date_str()
    parser = argparse.ArgumentParser(
        description=(
            "Build silver player-year feature tables from bronze Statcast dailies. "
            "By default loads year-to-date through --end-date for each affected season."
        )
    )
    parser.add_argument("--start-date", type=str, default=yesterday)
    parser.add_argument("--end-date", type=str, default=yesterday)
    parser.add_argument(
        "--no-year-to-date",
        action="store_true",
        help="Only load bronze for [start-date, end-date] exactly (no Jan 1 expansion).",
    )
    parser.add_argument("--bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument("--bronze-prefix", type=str, default=cfg.raw_statcast_prefix)
    parser.add_argument("--silver-prefix", type=str, default=cfg.feature_prefix)
    parser.add_argument("--min-pitches-pitcher", type=int, default=500)
    parser.add_argument("--min-pitches-batter", type=int, default=500)
    parser.add_argument("--min-batted-ball-batter", type=int, default=200)
    parser.add_argument("--hard-hit-speed-mph", type=float, default=95.0)
    parser.add_argument("--min-pitches-per-pitch-type", type=int, default=15)
    parser.add_argument("--raw-running-prefix", type=str, default=cfg.raw_running_prefix)
    parser.add_argument("--sprint-speed-min-opp", type=int, default=10)
    parser.add_argument("--raw-defence-prefix", type=str, default=cfg.raw_defence_prefix)
    args = parser.parse_args()

    result = build_bronze_to_silver_features(
        bucket=args.bucket,
        bronze_statcast_prefix=args.bronze_prefix,
        silver_prefix=args.silver_prefix,
        start_date_str=args.start_date,
        end_date_str=args.end_date,
        year_to_date=not args.no_year_to_date,
        min_pitches_pitcher=args.min_pitches_pitcher,
        min_pitches_batter=args.min_pitches_batter,
        min_batted_ball_batter=args.min_batted_ball_batter,
        hard_hit_speed_mph=args.hard_hit_speed_mph,
        min_pitches_per_pitch_type=args.min_pitches_per_pitch_type,
        raw_running_prefix=args.raw_running_prefix,
        sprint_speed_min_opp=args.sprint_speed_min_opp,
        raw_defence_prefix=args.raw_defence_prefix,
    )

    if result["status"] == "error":
        logger.error(result["message"])
        raise SystemExit(1)
    if result["status"] == "no_data":
        logger.warning(result["message"])
    else:
        logger.info(result["message"])


def run_statcast_running_main() -> None:
    from ..ingestion.statcast_running_ingestion import ingest_year_range

    cfg = PipelineSettings.from_environ()
    cy = current_utc_year()
    parser = argparse.ArgumentParser(
        description="Ingest Statcast sprint speed leaderboard to S3 (year range)."
    )
    parser.add_argument("--start-year", type=int, default=cy - 3)
    parser.add_argument("--end-year", type=int, default=cy)
    parser.add_argument("--min-opp", type=int, default=10)
    parser.add_argument("--s3-bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument("--s3-prefix", type=str, default=cfg.raw_running_prefix)
    args = parser.parse_args()

    result = ingest_year_range(
        args.start_year, args.end_year, args.s3_bucket, args.s3_prefix, min_opp=args.min_opp
    )

    if result["status"] == "error":
        logger.error(result["message"])
        for err in result.get("errors", []):
            logger.error(err)
        sys.exit(1)
    if result["status"] == "partial":
        logger.warning(result["message"])
        for err in result.get("errors", []):
            logger.warning(err)
        sys.exit(1)
    logger.info(result["message"])
    sys.exit(0)


def run_defence_ingestion_main() -> None:
    from ..ingestion.defence_ingestion import ingest_year_range as defence_ingest_year_range

    cfg = PipelineSettings.from_environ()
    cy = current_utc_year()
    parser = argparse.ArgumentParser(description="Ingest defensive metrics to S3 (year range).")
    parser.add_argument("--start-year", type=int, default=cy - 3)
    parser.add_argument("--end-year", type=int, default=cy)
    parser.add_argument("--s3-bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument("--s3-prefix", type=str, default=cfg.raw_defence_prefix)
    parser.add_argument(
        "--oaa-min-att",
        type=str,
        default="q",
        help='Statcast OAA minimum attempts: "q" (qualified) or an integer.',
    )
    parser.add_argument("--arm-min-throws", type=int, default=50)
    parser.add_argument("--framing-min-called", type=str, default="q")
    parser.add_argument("--pop-min-2b", type=int, default=5)
    parser.add_argument("--pop-min-3b", type=int, default=0)
    parser.add_argument("--fangraphs-qual", type=int, default=None)
    args = parser.parse_args()

    oaa_min: str | int = args.oaa_min_att
    if oaa_min != "q" and str(oaa_min).isdigit():
        oaa_min = int(oaa_min)

    framing_min: str | int = args.framing_min_called
    if framing_min != "q" and str(framing_min).isdigit():
        framing_min = int(framing_min)

    result = defence_ingest_year_range(
        args.start_year,
        args.end_year,
        args.s3_bucket,
        args.s3_prefix,
        oaa_min_att=oaa_min,
        arm_min_throws=args.arm_min_throws,
        framing_min_called=framing_min,
        pop_min_2b=args.pop_min_2b,
        pop_min_3b=args.pop_min_3b,
        fangraphs_qual=args.fangraphs_qual,
    )

    if result["status"] == "error":
        logger.error(result["message"])
        for err in result.get("errors", []):
            logger.error(err)
        sys.exit(1)
    if result["status"] == "partial":
        logger.warning(result["message"])
        for err in result.get("errors", []):
            logger.warning(err)
        sys.exit(1)
    logger.info(result["message"])
    sys.exit(0)


def run_build_player_year_archetype_features_main() -> None:
    """Deprecated entry name; delegates to bronze→silver pipeline."""
    run_bronze_to_silver_features_main()
