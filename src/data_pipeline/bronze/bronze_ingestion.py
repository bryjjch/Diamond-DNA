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

import logging
from typing import Any, Dict, Optional, Sequence

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
        try:
            res = runners[src]()
        except Exception as exc:  # noqa: BLE001 - one source must not abort the others
            logger.exception("Bronze source %r raised", src)
            res = {"status": "error", "message": f"{src} raised: {exc}", "errors": [str(exc)]}
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
    from ...common.cli import run_bronze_ingestion_main

    run_bronze_ingestion_main()


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ...common.handlers import bronze_ingestion_handler

    return bronze_ingestion_handler(event, context)


if __name__ == "__main__":
    main()
