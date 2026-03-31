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
    from ..bronze.statcast_ingestion import ingest_date_range

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


def bronze_to_silver_features_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ..silver.bronze_to_silver_features import build_bronze_to_silver_features

    y = yesterday_utc_date_str()
    cfg = PipelineSettings.from_environ()
    start_date = event_or_env_str(event, "start_date", "START_DATE", y)
    end_date = event_or_env_str(event, "end_date", "END_DATE", y)
    bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    bronze_prefix = event_or_env_str(event, "bronze_prefix", "RAW_PREFIX", cfg.raw_statcast_prefix)
    silver_prefix = event_or_env_str(event, "silver_prefix", "FEATURE_PREFIX", cfg.feature_prefix)
    raw_running = event_or_env_str(
        event, "raw_running_prefix", "RAW_RUNNING_PREFIX", cfg.raw_running_prefix
    )
    raw_defence = event_or_env_str(
        event, "raw_defence_prefix", "RAW_DEFENCE_PREFIX", cfg.raw_defence_prefix
    )
    yt_raw = event_or_env_str(event, "year_to_date", "YEAR_TO_DATE", "true")
    year_to_date = str(yt_raw).strip().lower() not in ("0", "false", "no")

    result = build_bronze_to_silver_features(
        bucket=bucket,
        bronze_statcast_prefix=bronze_prefix,
        silver_prefix=silver_prefix,
        start_date_str=start_date,
        end_date_str=end_date,
        year_to_date=year_to_date,
        raw_running_prefix=raw_running,
        raw_defence_prefix=raw_defence,
    )

    status_code = 200 if result.get("status") in ("ok", "no_data") else 400
    return {
        "statusCode": status_code,
        "body": result.get("message", ""),
        "details": result,
    }


def statcast_by_player_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Backward-compatible name; invokes bronze→silver features."""
    return bronze_to_silver_features_handler(event, context)


def statcast_running_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ..bronze.statcast_running_ingestion import ingest_year_range

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
    from ..bronze.defence_ingestion import ingest_year_range as defence_ingest_year_range

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
