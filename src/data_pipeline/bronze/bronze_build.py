#!/usr/bin/env python3
"""
Bronze ingestion orchestrator.

Runs every bronze-layer ingestion source in a single call:
  - statcast: pitch-by-pitch Statcast data (date range; one file per day)
  - running:  Statcast sprint-speed leaderboard (year range)
  - defence:  defensive metrics leaderboards (year range)
  - bio:      MLB Stats API player bios (year range)
  - standard: MLB Stats API standard season stat lines (year range)

Statcast is date-scoped; the other sources are season (year) scoped. When the
year range is not supplied explicitly, it is derived from the date range so a single
date window drives every source. Each source runs independently: a failure in one does
not abort the others, and the aggregated status reflects the worst outcome.
"""

from __future__ import annotations

import argparse
import logging
import time
from typing import Any, Dict, Optional, Sequence

from ...common.runtime_helpers import event_or_env_int, event_or_env_str, yesterday_utc_date_str
from ...common.settings import PipelineSettings

logger = logging.getLogger(__name__)

ALL_SOURCES = ("statcast", "running", "defence", "bio", "standard")


def _year_from_date_str(date_str: str) -> int:
    from datetime import datetime

    try:
        return datetime.strptime(str(date_str).strip(), "%Y-%m-%d").year
    except ValueError as exc:
        raise ValueError(f"Invalid date {date_str!r}; expected YYYY-MM-DD") from exc

def ingest_all_bronze(
    *,
    s3_bucket: str,
    statcast_prefix: str,
    running_prefix: str,
    defence_prefix: str,
    bio_prefix: str,
    standard_prefix: str,
    start_date_str: str,
    end_date_str: str,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    sources: Sequence[str] = ALL_SOURCES,
    min_opp: int = 10,
    oaa_min_att: str | int = "q",
    arm_min_throws: int = 50,
    framing_min_called: str | int = "q",
    pop_min_2b: int = 5,
    pop_min_3b: int = 0,
) -> Dict[str, Any]:
    """
    Run the selected bronze ingestion sources and aggregate their results.

    Returns a dict with:
      status:  "ok" (all sources ok) | "error" (all sources errored) | "partial" (mix)
      message: per-source status summary, e.g. "statcast=ok running=ok defence=error"
      sources: {source_name: raw sub-result dict}
      errors:  flattened, source-prefixed error strings
    """
    from .bio_ingestion import ingest_year_range as ingest_bio_year_range
    from .statcast_ingestion import ingest_date_range
    from .statcast_running_ingestion import ingest_year_range as ingest_running_year_range
    from .defence_ingestion import ingest_year_range as ingest_defence_year_range
    from .standard_stats_ingestion import ingest_year_range as ingest_standard_year_range

    unknown = [s for s in sources if s not in ALL_SOURCES]
    if unknown:
        msg = f"Unknown source(s): {unknown}; valid: {list(ALL_SOURCES)}"
        return {"status": "error", "message": msg, "sources": {}, "errors": [msg]}

    # Preserve canonical order and dedupe whatever the caller requested.
    enabled = [s for s in ALL_SOURCES if s in sources]
    if not enabled:
        msg = "No sources selected."
        return {"status": "error", "message": msg, "sources": {}, "errors": [msg]}

    sy = start_year if start_year is not None else _year_from_date_str(start_date_str)
    ey = end_year if end_year is not None else _year_from_date_str(end_date_str)

    runners = {
        "statcast": lambda: ingest_date_range(
            start_date_str, end_date_str, s3_bucket, statcast_prefix
        ),
        "running": lambda: ingest_running_year_range(
            sy, ey, s3_bucket, running_prefix, min_opp=min_opp
        ),
        "defence": lambda: ingest_defence_year_range(
            sy,
            ey,
            s3_bucket,
            defence_prefix,
            oaa_min_att=oaa_min_att,
            arm_min_throws=arm_min_throws,
            framing_min_called=framing_min_called,
            pop_min_2b=pop_min_2b,
            pop_min_3b=pop_min_3b,
        ),
        "bio": lambda: ingest_bio_year_range(sy, ey, s3_bucket, bio_prefix),
        "standard": lambda: ingest_standard_year_range(sy, ey, s3_bucket, standard_prefix),
    }

    results: Dict[str, Any] = {}
    errors: list[str] = []
    for src in enabled:
        logger.info("Bronze ingestion: running source %r", src)
        source_started = time.monotonic()
        try:
            res = runners[src]()
        except Exception as exc:  # noqa: BLE001 - one source must not abort the others
            logger.exception("Bronze source %r raised", src)
            res = {"status": "error", "message": f"{src} raised: {exc}", "errors": [str(exc)]}
        logger.info(
            "[timing] bronze source %r: %.1fs (status=%s)",
            src,
            time.monotonic() - source_started,
            res.get("status"),
        )
        results[src] = res
        sub_errors = res.get("errors") or []
        for err in sub_errors:
            errors.append(f"{src}: {err}")
        if res.get("status") not in ("ok", "no_data") and not sub_errors:
            errors.append(f"{src}: {res.get('message', 'error')}")

    # "no_data" is not a failure: nothing to ingest still counts as ok for aggregation.
    statuses = {src: results[src].get("status") for src in enabled}
    if all(s in ("ok", "no_data") for s in statuses.values()):
        overall = "ok"
    elif all(s == "error" for s in statuses.values()):
        overall = "error"
    else:
        overall = "partial"

    message = " ".join(f"{src}={statuses[src]}" for src in enabled)
    return {"status": overall, "message": message, "sources": results, "errors": errors}


def main() -> None:
    cfg = PipelineSettings.from_environ()
    yesterday = yesterday_utc_date_str()
    parser = argparse.ArgumentParser(
        description=(
            "Run all bronze ingestion sources (statcast pitches, sprint-speed running, "
            "defence, player bios) in one call. Statcast uses the date range; the other "
            "sources use the year range, which defaults to the years spanned by the date range."
        )
    )
    parser.add_argument("--start-date", type=str, default=yesterday)
    parser.add_argument("--end-date", type=str, default=yesterday)
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help="Running/defence start year (default: year of --start-date).",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Running/defence end year (default: year of --end-date).",
    )
    parser.add_argument("--s3-bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument("--statcast-prefix", type=str, default=cfg.raw_statcast_prefix)
    parser.add_argument("--running-prefix", type=str, default=cfg.raw_running_prefix)
    parser.add_argument("--defence-prefix", type=str, default=cfg.raw_defence_prefix)
    parser.add_argument("--bio-prefix", type=str, default=cfg.raw_bio_prefix)
    parser.add_argument("--standard-prefix", type=str, default=cfg.raw_standard_stats_prefix)
    parser.add_argument(
        "--sources",
        type=str,
        default=",".join(ALL_SOURCES),
        help="Comma-separated subset of sources to run (statcast,running,defence,bio,standard).",
    )
    # running tuning
    parser.add_argument("--min-opp", type=int, default=10)
    # defence tuning
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
    args = parser.parse_args()

    sources = [s.strip() for s in args.sources.split(",") if s.strip()]

    oaa_min: str | int = args.oaa_min_att
    if oaa_min != "q" and str(oaa_min).isdigit():
        oaa_min = int(oaa_min)

    framing_min: str | int = args.framing_min_called
    if framing_min != "q" and str(framing_min).isdigit():
        framing_min = int(framing_min)

    result = ingest_all_bronze(
        s3_bucket=args.s3_bucket,
        statcast_prefix=args.statcast_prefix,
        running_prefix=args.running_prefix,
        defence_prefix=args.defence_prefix,
        bio_prefix=args.bio_prefix,
        standard_prefix=args.standard_prefix,
        start_date_str=args.start_date,
        end_date_str=args.end_date,
        start_year=args.start_year,
        end_year=args.end_year,
        sources=sources,
        min_opp=args.min_opp,
        oaa_min_att=oaa_min,
        arm_min_throws=args.arm_min_throws,
        framing_min_called=framing_min,
        pop_min_2b=args.pop_min_2b,
        pop_min_3b=args.pop_min_3b,
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
    s3_bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    statcast_prefix = event_or_env_str(
        event, "statcast_prefix", "S3_PREFIX", cfg.raw_statcast_prefix
    )
    running_prefix = event_or_env_str(
        event, "running_prefix", "RAW_RUNNING_PREFIX", cfg.raw_running_prefix
    )
    defence_prefix = event_or_env_str(
        event, "defence_prefix", "RAW_DEFENCE_PREFIX", cfg.raw_defence_prefix
    )
    bio_prefix = event_or_env_str(event, "bio_prefix", "RAW_BIO_PREFIX", cfg.raw_bio_prefix)
    standard_prefix = event_or_env_str(
        event, "standard_prefix", "RAW_STANDARD_STATS_PREFIX", cfg.raw_standard_stats_prefix
    )

    # Year range is optional; empty -> None so the orchestrator derives it from the dates.
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

    # sources: accept a JSON list (event) or a comma-separated string (event/env).
    ev = event if isinstance(event, dict) else {}
    raw_sources = ev.get("sources")
    if isinstance(raw_sources, (list, tuple)):
        sources = [str(s).strip() for s in raw_sources if str(s).strip()]
    else:
        sources_str = event_or_env_str(event, "sources", "SOURCES", ",".join(ALL_SOURCES))
        sources = [s.strip() for s in sources_str.split(",") if s.strip()]

    # "q" (qualified) or an integer threshold; empty falls back to "q".
    oaa_s = str(event_or_env_str(event, "oaa_min_att", "OAA_MIN_ATT", "q")).strip() or "q"
    oaa_min: str | int = int(oaa_s) if oaa_s.isdigit() else oaa_s
    framing_s = (
        str(event_or_env_str(event, "framing_min_called", "FRAMING_MIN_CALLED", "q")).strip() or "q"
    )
    framing_min: str | int = int(framing_s) if framing_s.isdigit() else framing_s

    result = ingest_all_bronze(
        s3_bucket=s3_bucket,
        statcast_prefix=statcast_prefix,
        running_prefix=running_prefix,
        defence_prefix=defence_prefix,
        bio_prefix=bio_prefix,
        standard_prefix=standard_prefix,
        start_date_str=start_date,
        end_date_str=end_date,
        start_year=start_year,
        end_year=end_year,
        sources=sources,
        min_opp=event_or_env_int(event, "min_opp", "MIN_OPP", 10),
        oaa_min_att=oaa_min,
        arm_min_throws=event_or_env_int(event, "arm_min_throws", "ARM_MIN_THROWS", 50),
        framing_min_called=framing_min,
        pop_min_2b=event_or_env_int(event, "pop_min_2b", "POP_MIN_2B", 5),
        pop_min_3b=event_or_env_int(event, "pop_min_3b", "POP_MIN_3B", 0),
    )
    status_code = 200 if result["status"] == "ok" else (207 if result["status"] == "partial" else 400)
    return {"statusCode": status_code, **result}


if __name__ == "__main__":
    main()
