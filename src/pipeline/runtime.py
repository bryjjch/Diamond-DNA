"""Lambda event and environment resolution helpers."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional


def yesterday_utc_date_str() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


def current_utc_year() -> int:
    return datetime.now(timezone.utc).year


def _get_event(event: Any) -> Optional[Mapping[str, Any]]:
    if isinstance(event, dict):
        return event
    return None


def event_or_env_str(
    event: Any,
    event_key: str,
    env_key: str,
    default: str,
    *,
    environ: Optional[Mapping[str, str]] = None,
) -> str:
    ev = _get_event(event)
    if ev is not None:
        v = ev.get(event_key)
        if v is not None and str(v).strip() != "":
            return str(v)
    env = environ if environ is not None else os.environ
    return env.get(env_key, default)


def event_or_env_int(
    event: Any,
    event_key: str,
    env_key: str,
    default: int,
    *,
    environ: Optional[Mapping[str, str]] = None,
) -> int:
    raw = event_or_env_str(event, event_key, env_key, "", environ=environ)
    if raw == "":
        return default
    return int(raw)


def env_str(env_key: str, default: str, *, environ: Optional[Mapping[str, str]] = None) -> str:
    env = environ if environ is not None else os.environ
    return env.get(env_key, default)


def env_int(env_key: str, default: int, *, environ: Optional[Mapping[str, str]] = None) -> int:
    env = environ if environ is not None else os.environ
    v = env.get(env_key)
    if v is None or str(v).strip() == "":
        return default
    return int(v)
