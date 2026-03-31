"""Lambda handlers (thin wrappers around stage logic)."""

from __future__ import annotations

from typing import Any, Dict

from .runtime import (
    current_utc_year,
    event_or_env_int,
    event_or_env_str,
    yesterday_utc_date_str,
)
from .settings import PipelineSettings


def statcast_ingestion_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ..ingestion.statcast_ingestion import ingest_date_range

    y = yesterday_utc_date_str()
    cfg = PipelineSettings.from_environ()
    start_date = event_or_env_str(event, "start_date", "START_DATE", y)
    end_date = event_or_env_str(event, "end_date", "END_DATE", y)
    s3_bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    s3_prefix = event_or_env_str(
        event, "s3_prefix", "S3_PREFIX", cfg.raw_statcast_prefix
    )

    result = ingest_date_range(start_date, end_date, s3_bucket, s3_prefix)

    if result["status"] == "error":
        return {
            "statusCode": 400,
            "body": result["message"],
            "errors": result.get("errors", []),
        }
    if result["status"] == "partial":
        return {
            "statusCode": 207,
            "body": result["message"],
            "total_records": result["total_records"],
            "days_ok": result["days_ok"],
            "days_no_data": result["days_no_data"],
            "days_error": result["days_error"],
            "errors": result.get("errors", []),
        }
    return {
        "statusCode": 200,
        "body": result["message"],
        "total_records": result["total_records"],
        "days_ok": result["days_ok"],
        "days_no_data": result["days_no_data"],
    }


def statcast_by_player_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ..processing.processing_statcast_by_player import build_by_player_layer

    y = yesterday_utc_date_str()
    cfg = PipelineSettings.from_environ()
    start_date = event_or_env_str(event, "start_date", "START_DATE", y)
    end_date = event_or_env_str(event, "end_date", "END_DATE", y)
    bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    raw_prefix = event_or_env_str(event, "raw_prefix", "RAW_PREFIX", cfg.raw_statcast_prefix)
    processed_prefix = event_or_env_str(
        event, "processed_prefix", "PROCESSED_PREFIX", cfg.processed_prefix
    )

    result = build_by_player_layer(
        start_date,
        end_date,
        s3_bucket=bucket,
        raw_prefix=raw_prefix,
        processed_prefix=processed_prefix,
    )

    status_code = 200 if result.get("status") in ("ok", "no_data") else 400
    return {
        "statusCode": status_code,
        "body": result.get("message", ""),
        "details": result,
    }


def statcast_running_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ..ingestion.statcast_running_ingestion import ingest_year_range

    cy = current_utc_year()
    cfg = PipelineSettings.from_environ()
    start_year = event_or_env_int(
        event, "start_year", "START_YEAR", cy - 3
    )
    end_year = event_or_env_int(event, "end_year", "END_YEAR", cy)
    min_opp = event_or_env_int(event, "min_opp", "MIN_OPP", 10)
    s3_bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    s3_prefix = event_or_env_str(
        event, "s3_prefix", "S3_PREFIX", cfg.raw_running_prefix
    )

    result = ingest_year_range(start_year, end_year, s3_bucket, s3_prefix, min_opp=min_opp)
    status_code = 200 if result["status"] == "ok" else (207 if result["status"] == "partial" else 400)
    return {"statusCode": status_code, **result}


def _parse_min_qual_str(s: str, default: str | int) -> str | int:
    raw = str(s).strip() if s is not None else ""
    if raw == "":
        v = default
        if isinstance(v, int):
            return v
        raw = str(v).strip()
    return int(raw) if raw.isdigit() else raw


def defence_ingestion_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ..ingestion.defence_ingestion import ingest_year_range as defence_ingest_year_range

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
    result = defence_ingest_year_range(
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
