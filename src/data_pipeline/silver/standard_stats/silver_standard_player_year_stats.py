"""
Bronze -> silver: standard season stat-line tables (own silver table per role).

Reads bronze standard batting/pitching Parquet (landed by
bronze.standard_stats_ingestion), derives a few conventional rate stats that the
MLB Stats API does not return directly, and writes one player-year table per role:

  {silver_prefix}/batter/year=Y/standard_stats_player_year.parquet
  {silver_prefix}/pitcher/year=Y/standard_stats_player_year.parquet

Unlike the bio / defence player-year helpers (which fold columns into the archetype
feature rows), this is a standalone table: the conventional slash line lives on its
own so it can be joined to features/archetypes downstream without polluting the
training feature set.
"""

from __future__ import annotations

import argparse
import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from ....common.runtime_helpers import current_utc_year, event_or_env_int, event_or_env_str
from ....common.s3_helpers import read_parquet_from_s3, write_parquet_to_s3
from ....common.settings import PipelineSettings
from ....common.timing import log_phase

logger = logging.getLogger(__name__)

# silver role -> bronze statsapi group.
_ROLE_TO_GROUP: Dict[str, str] = {"batter": "batting", "pitcher": "pitching"}


def outs_from_innings_pitched(ip: pd.Series) -> pd.Series:
    """
    Convert statsapi innings-pitched (``.1``/``.2`` = ⅓/⅔ of an inning) to whole outs.

    e.g. 12.1 IP -> 37 outs, 12.2 IP -> 38 outs. Used so per-9 rates are exact rather
    than treating the fractional part as a true decimal.
    """
    x = pd.to_numeric(ip, errors="coerce")
    whole = np.floor(x)
    tenths = (x - whole) * 10.0
    # Guard against float dust (e.g. 0.2999) before rounding the .1/.2 fraction to outs.
    frac_outs = np.round(tenths)
    return whole * 3.0 + frac_outs


def _derive_batting(df: pd.DataFrame) -> pd.DataFrame:
    """Add batting rate stats the API omits. avg/obp/slg/ops are trusted as-returned."""
    out = df.copy()
    pa = pd.to_numeric(out.get("pa"), errors="coerce")
    out["bb_pct"] = pd.to_numeric(out.get("bb"), errors="coerce") / pa
    out["k_pct"] = pd.to_numeric(out.get("so"), errors="coerce") / pa
    return out


def _derive_pitching(df: pd.DataFrame) -> pd.DataFrame:
    """Add pitching per-9 rate stats. era/whip are trusted as-returned by the API."""
    out = df.copy()
    outs = outs_from_innings_pitched(out.get("ip"))
    innings = outs / 3.0
    with np.errstate(divide="ignore", invalid="ignore"):
        out["k_per_9"] = pd.to_numeric(out.get("so"), errors="coerce") / innings * 9.0
        out["bb_per_9"] = pd.to_numeric(out.get("bb"), errors="coerce") / innings * 9.0
        out["hr_per_9"] = pd.to_numeric(out.get("hr"), errors="coerce") / innings * 9.0
    return out


_DERIVERS = {"batter": _derive_batting, "pitcher": _derive_pitching}


def build_standard_stats_for_year(
    bucket: str,
    raw_standard_stats_prefix: str,
    silver_prefix: str,
    year: int,
) -> Dict[str, int]:
    """
    Build and write the standard stat-line silver table for one year, both roles.

    Returns a mapping role -> rows written (roles with no bronze data are skipped).
    """
    written: Dict[str, int] = {}
    for role, group in _ROLE_TO_GROUP.items():
        key = f"{raw_standard_stats_prefix.strip('/')}/year={year}/mlb_standard_{group}.parquet"
        df = read_parquet_from_s3(bucket, key, log_read=False, missing_key_log="none")
        if df is None or df.empty or "player_id" not in df.columns:
            logger.info("No bronze standard %s stats for year=%d; skipping.", group, year)
            continue

        df = df.copy()
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce")
        df = df.loc[df["player_id"].notna()].copy()
        df["player_id"] = df["player_id"].astype(int)
        df["year"] = year
        df = _DERIVERS[role](df)

        out_key = (
            f"{silver_prefix.strip('/')}/{role}/year={year}/standard_stats_player_year.parquet"
        )
        logger.info(
            "Writing %d %s standard stat rows to s3://%s/%s", len(df), role, bucket, out_key
        )
        write_parquet_to_s3(df, bucket, out_key, log_write=False)
        written[role] = int(len(df))

    return written


def build_standard_stats_range(
    bucket: str,
    raw_standard_stats_prefix: str,
    silver_prefix: str,
    start_year: int,
    end_year: int,
) -> dict:
    """
    Build the standard stat-line silver tables for each year in [start_year, end_year].
    """
    if start_year > end_year:
        return {
            "status": "error",
            "message": "start_year must be <= end_year",
            "years_written": [],
            "rows_written": 0,
        }

    years_written: List[int] = []
    rows_written = 0
    with log_phase(logger, f"silver standard stats years {start_year}..{end_year}"):
        for year in range(start_year, end_year + 1):
            written = build_standard_stats_for_year(
                bucket, raw_standard_stats_prefix, silver_prefix, year
            )
            year_rows = sum(written.values())
            if year_rows:
                years_written.append(year)
                rows_written += year_rows

    if not years_written:
        msg = f"No bronze standard stats found between {start_year} and {end_year}"
        logger.warning(msg)
        return {
            "status": "no_data",
            "message": msg,
            "years_written": [],
            "rows_written": 0,
        }

    message = (
        f"Bronze->silver standard stats: wrote {rows_written} rows "
        f"across years {years_written}"
    )
    logger.info(message)
    return {
        "status": "ok",
        "message": message,
        "years_written": years_written,
        "rows_written": rows_written,
    }


def main() -> None:
    cfg = PipelineSettings.from_environ()
    cy = current_utc_year()
    parser = argparse.ArgumentParser(
        description="Build silver standard stat-line tables from bronze standard stats (year range)."
    )
    parser.add_argument("--start-year", type=int, default=cy - 1)
    parser.add_argument("--end-year", type=int, default=cy)
    parser.add_argument("--bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument(
        "--raw-standard-stats-prefix", type=str, default=cfg.raw_standard_stats_prefix
    )
    parser.add_argument("--silver-prefix", type=str, default=cfg.feature_prefix)
    args = parser.parse_args()

    result = build_standard_stats_range(
        bucket=args.bucket,
        raw_standard_stats_prefix=args.raw_standard_stats_prefix,
        silver_prefix=args.silver_prefix,
        start_year=args.start_year,
        end_year=args.end_year,
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
    raw_standard_stats_prefix = event_or_env_str(
        event, "raw_standard_stats_prefix", "RAW_STANDARD_STATS_PREFIX", cfg.raw_standard_stats_prefix
    )
    silver_prefix = event_or_env_str(event, "silver_prefix", "FEATURE_PREFIX", cfg.feature_prefix)

    result = build_standard_stats_range(
        bucket=bucket,
        raw_standard_stats_prefix=raw_standard_stats_prefix,
        silver_prefix=silver_prefix,
        start_year=start_year,
        end_year=end_year,
    )
    status_code = 200 if result.get("status") in ("ok", "no_data") else 400
    return {"statusCode": status_code, **result}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
