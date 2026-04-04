#!/usr/bin/env python3
"""
Defensive metrics ingestion (batters / position players).

Fetches Statcast fielding leaderboards, Savant arm strength, FanGraphs fielding (DRS),
and uploads Parquet under:

  {s3_prefix}/year=YYYY/<dataset>.parquet

Default s3_prefix: bronze/defence
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import datetime, timezone
from typing import Any, List, Optional

import pandas as pd
import requests

try:
    from pybaseball import (
        fielding_stats,
        statcast_catcher_poptime,
        statcast_outfield_catch_prob,
        statcast_outs_above_average,
    )
    from pybaseball.utils import sanitize_statcast_columns
except Exception:
    fielding_stats = None
    statcast_catcher_poptime = None
    statcast_outfield_catch_prob = None
    statcast_outs_above_average = None
    sanitize_statcast_columns = None

from .ingest_common import retry_with_backoff

from ..pipeline.runtime import (
    current_utc_year,
    event_or_env_int,
    event_or_env_str,
)
from ..pipeline.s3_interaction import (
    DEFENCE_ARM_STRENGTH_PARQUET,
    DEFENCE_CATCHER_FRAMING_PARQUET,
    DEFENCE_CATCHER_POPTIME_PARQUET,
    DEFENCE_FANGRAPHS_FIELDING_PARQUET,
    DEFENCE_OAA_PARQUET,
    DEFENCE_OUTFIELD_CATCH_PARQUET,
    raw_defence_dataset_key,
    write_parquet_to_s3,
)
from ..pipeline.settings import PipelineSettings

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Statcast OAA leaderboard is split by fielding position (MLB numeric codes). Catchers (2) are excluded.
OAA_POSITION_CODES: List[int] = [3, 4, 5, 6, 7, 8, 9]

SAVANT_ARM_STRENGTH_CSV = (
    "https://baseballsavant.mlb.com/leaderboard/arm-strength"
    "?year={year}&type=player&team=&pos=&minThrows={min_throws}&csv=true"
)
SAVANT_CATCHER_FRAMING_CSV = (
    "https://baseballsavant.mlb.com/leaderboard/catcher-framing"
    "?type=catcher&seasonStart={year}&seasonEnd={year}&team=&min={min_called_p}"
    "&sortColumn=rv_tot&sortDirection=desc&csv=true"
)


def _read_savant_csv(url: str) -> pd.DataFrame:
    """Read a Savant CSV file from the given URL and return a pandas DataFrame."""
    res = requests.get(url, timeout=120)
    res.raise_for_status()
    text = res.content.decode("utf-8-sig")
    return pd.read_csv(io.StringIO(text))


def fetch_statcast_arm_strength(year: int, *, min_throws: int = 50) -> pd.DataFrame:
    """
    Arm strength leaderboard (Savant).
    """
    df = _read_savant_csv(SAVANT_ARM_STRENGTH_CSV.format(year=year, min_throws=min_throws))
    if sanitize_statcast_columns is not None:
        df = sanitize_statcast_columns(df)
    return df


def fetch_statcast_catcher_framing_robust(year: int, *, min_called_p: str | int = "q") -> pd.DataFrame:
    """
    Catcher framing leaderboard (Savant).
    """
    url = SAVANT_CATCHER_FRAMING_CSV.format(year=year, min_called_p=min_called_p)
    df = _read_savant_csv(url)
    if sanitize_statcast_columns is not None:
        df = sanitize_statcast_columns(df)
    if "name" in df.columns:
        df = df.loc[df["name"].notna()].reset_index(drop=True)
    return df


def fetch_oaa_all_positions(year: int, *, min_att: str | int = "q") -> pd.DataFrame:
    """
    Statcast OAA leaderboard (all positions).
    """
    if statcast_outs_above_average is None:
        raise ImportError("pybaseball is required. Install pybaseball in this environment.")

    frames: List[pd.DataFrame] = []
    for pos in OAA_POSITION_CODES:
        df = statcast_outs_above_average(year, pos, min_att=min_att, view="Fielder")
        if df is not None and not df.empty:
            d = df.copy()
            d["oaa_leaderboard_position"] = pos
            frames.append(d)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def ingest_defence_year(
    year: int,
    s3_bucket: str,
    s3_prefix: str,
    *,
    oaa_min_att: str | int = "q",
    arm_min_throws: int = 50,
    framing_min_called: str | int = "q",
    pop_min_2b: int = 5,
    pop_min_3b: int = 0,
    fangraphs_qual: Optional[int] = None,
) -> dict:
    """
    Ingest defence data for a given year.
    """
    current_year = datetime.now(timezone.utc).year
    if year > current_year + 1:
        return {"status": "error", "message": f"Year {year} is too far in the future (current UTC year: {current_year})."}

    if statcast_outs_above_average is None or fielding_stats is None:
        return {"status": "error", "message": "pybaseball is required for defence ingestion."}

    uploads: List[tuple[str, pd.DataFrame]] = []
    errors: List[str] = []

    # OAA stats
    df_oaa = retry_with_backoff(
        f"OAA year={year}",
        lambda: fetch_oaa_all_positions(year, min_att=oaa_min_att),
    )
    if df_oaa is not None and not df_oaa.empty:
        uploads.append((raw_defence_dataset_key(s3_prefix, year, DEFENCE_OAA_PARQUET), df_oaa))
    elif df_oaa is None:
        errors.append(f"{year}: OAA fetch failed")

    # Outfield catch probability
    df_catch = retry_with_backoff(
        f"outfield catch probability year={year}",
        lambda: statcast_outfield_catch_prob(year, min_opp="q"),  # type: ignore[misc]
    )
    if df_catch is not None and not df_catch.empty:
        uploads.append(
            (raw_defence_dataset_key(s3_prefix, year, DEFENCE_OUTFIELD_CATCH_PARQUET), df_catch)
        )
    elif df_catch is None:
        errors.append(f"{year}: outfield catch probability fetch failed")

    # Catcher pop time
    df_pop = retry_with_backoff(
        f"catcher pop time year={year}",
        lambda: statcast_catcher_poptime(year, min_2b_att=pop_min_2b, min_3b_att=pop_min_3b),  # type: ignore[misc]
    )
    if df_pop is not None and not df_pop.empty:
        uploads.append(
            (raw_defence_dataset_key(s3_prefix, year, DEFENCE_CATCHER_POPTIME_PARQUET), df_pop)
        )
    elif df_pop is None:
        errors.append(f"{year}: catcher pop time fetch failed")

    # Catcher framing
    df_framing = retry_with_backoff(
        f"catcher framing year={year}",
        lambda: fetch_statcast_catcher_framing_robust(year, min_called_p=framing_min_called),
    )
    if df_framing is not None and not df_framing.empty:
        uploads.append(
            (raw_defence_dataset_key(s3_prefix, year, DEFENCE_CATCHER_FRAMING_PARQUET), df_framing)
        )
    elif df_framing is None:
        errors.append(f"{year}: catcher framing fetch failed")

    # Arm strength
    df_arm = retry_with_backoff(
        f"arm strength year={year}",
        lambda: fetch_statcast_arm_strength(year, min_throws=arm_min_throws),
    )
    if df_arm is not None and not df_arm.empty:
        uploads.append(
            (raw_defence_dataset_key(s3_prefix, year, DEFENCE_ARM_STRENGTH_PARQUET), df_arm)
        )
    elif df_arm is None:
        errors.append(f"{year}: arm strength fetch failed")

    # FanGraphs fielding
    def _fg() -> pd.DataFrame:
        """Fetch FanGraphs fielding data for a given year."""
        return fielding_stats(year, year, qual=fangraphs_qual, split_seasons=True)  # type: ignore[misc]

    df_fg = retry_with_backoff(f"FanGraphs fielding year={year}", _fg)
    if df_fg is not None and not df_fg.empty:
        uploads.append(
            (raw_defence_dataset_key(s3_prefix, year, DEFENCE_FANGRAPHS_FIELDING_PARQUET), df_fg)
        )
    elif df_fg is None:
        errors.append(f"{year}: FanGraphs fielding fetch failed")

    total_rows = 0
    for key, df in uploads:
        logger.info("Uploading %d rows to s3://%s/%s", len(df), s3_bucket, key)
        write_parquet_to_s3(df, s3_bucket, key, log_write=False)
        total_rows += len(df)

    status = "ok" if not errors else ("partial" if uploads else "error")
    return {
        "status": status,
        "message": f"year={year}: uploaded {len(uploads)} objects, {len(errors)} source errors",
        "uploads": len(uploads),
        "total_rows": total_rows,
        "errors": errors,
    }


def ingest_year_range(
    start_year: int,
    end_year: int,
    s3_bucket: str,
    s3_prefix: str,
    *,
    oaa_min_att: str | int = "q",
    arm_min_throws: int = 50,
    framing_min_called: str | int = "q",
    pop_min_2b: int = 5,
    pop_min_3b: int = 0,
    fangraphs_qual: Optional[int] = None,
) -> dict:
    """
    Ingest defence data for a range of years.
    """
    if start_year > end_year:
        return {"status": "error", "message": "start_year must be <= end_year"}

    years_ok = 0
    years_partial = 0
    years_error = 0
    all_errors: List[str] = []
    total_rows = 0

    for year in range(start_year, end_year + 1):
        r = ingest_defence_year(
            year,
            s3_bucket,
            s3_prefix,
            oaa_min_att=oaa_min_att,
            arm_min_throws=arm_min_throws,
            framing_min_called=framing_min_called,
            pop_min_2b=pop_min_2b,
            pop_min_3b=pop_min_3b,
            fangraphs_qual=fangraphs_qual,
        )
        total_rows += int(r.get("total_rows", 0))
        all_errors.extend(r.get("errors", []))
        st = r["status"]
        if st == "ok":
            years_ok += 1
        elif st == "partial":
            years_partial += 1
        else:
            years_error += 1

    if years_error == end_year - start_year + 1:
        status = "error"
    elif years_partial or years_error:
        status = "partial"
    else:
        status = "ok"

    return {
        "status": status,
        "message": f"Years {start_year}..{end_year}: ok={years_ok} partial={years_partial} error={years_error}",
        "total_rows": total_rows,
        "years_ok": years_ok,
        "years_partial": years_partial,
        "years_error": years_error,
        "errors": all_errors,
    }


def _parse_min_qual_str(s: str, default: str | int) -> str | int:
    """Parse a minimum qualification string into an integer or string."""
    raw = str(s).strip() if s is not None else ""
    if raw == "":
        v = default
        if isinstance(v, int):
            return v
        raw = str(v).strip()
    return int(raw) if raw.isdigit() else raw


def main() -> None:
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

    result = ingest_year_range(
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


def handler(event: dict, context: Any) -> dict:
    cy = current_utc_year()
    cfg = PipelineSettings.from_environ()
    start_year = event_or_env_int(event, "start_year", "START_YEAR", cy - 3)
    end_year = event_or_env_int(event, "end_year", "END_YEAR", cy)
    s3_bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    s3_prefix = event_or_env_str(
        event, "s3_prefix", "S3_PREFIX", cfg.raw_defence_prefix
    )

    oaa_s = event_or_env_str(event, "oaa_min_att", "OAA_MIN_ATT", "q")
    framing_s = event_or_env_str(event, "framing_min_called", "FRAMING_MIN_CALLED", "q")
    ev = event if isinstance(event, dict) else {}
    result = ingest_year_range(
        start_year,
        end_year,
        s3_bucket,
        s3_prefix,
        oaa_min_att=_parse_min_qual_str(oaa_s, "q"),
        arm_min_throws=event_or_env_int(event, "arm_min_throws", "ARM_MIN_THROWS", 50),
        framing_min_called=_parse_min_qual_str(framing_s, "q"),
        pop_min_2b=event_or_env_int(event, "pop_min_2b", "POP_MIN_2B", 5),
        pop_min_3b=event_or_env_int(event, "pop_min_3b", "POP_MIN_3B", 0),
        fangraphs_qual=ev.get("fangraphs_qual"),
    )
    status_code = 200 if result["status"] == "ok" else (207 if result["status"] == "partial" else 400)
    return {"statusCode": status_code, **result}


if __name__ == "__main__":
    main()
