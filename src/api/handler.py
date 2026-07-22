"""AWS Lambda handler for the xWAR Engine HTTP API.

Routes API Gateway HTTP API v2 events to the lake-backed data loader,
returning JSON. Artifacts are loaded lazily from S3 (or a local lake mirror)
and cached for the lifetime of the container.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Tuple

from .data_loader import (
    ARCHETYPE_ROLES,
    PROJECTION_MODELS,
    ROLE_ALL_TARGETS,
    VALID_ROLES,
    LakeStore,
    accuracy_payload,
    default_order,
    default_sort,
    get_store,
    player_detail_payload,
    projection_rows,
    search_players,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_CORS_HEADERS: Dict[str, str] = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}

_RETIRED_PATHS = (
    "/api/clusters",
    "/clusters",
    "/api/leaderboard",
    "/leaderboard",
    "/api/neighbors",
    "/neighbors",
    "/api/player_soft_profile",
    "/player_soft_profile",
)

_MAX_ACCURACY_SPAN = 10


def _response(status: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """Helper to format a JSON response with CORS headers."""
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json", **_CORS_HEADERS},
        "body": json.dumps(body),
    }


def _extract_path_method(event: Dict[str, Any]) -> Tuple[str, str]:
    """Pull the request path and method from an API Gateway HTTP API v2 event."""
    rc = event.get("requestContext") or {}
    http = rc.get("http") or {}
    method = (http.get("method") or event.get("httpMethod") or "GET").upper()
    path = http.get("path") or event.get("rawPath") or event.get("path") or "/"
    return path, method


def _query(event: Dict[str, Any]) -> Dict[str, str]:
    """Extract query string parameters from an API Gateway HTTP API v2 event."""
    return event.get("queryStringParameters") or {}


def _int_param(
    qs: Dict[str, str], name: str, default: Optional[int], lo: int, hi: int
) -> Tuple[Optional[int], Optional[str]]:
    """Parse an integer query param, clamped to [lo, hi]. Returns (value, error)."""
    raw = (qs.get(name) or "").strip()
    if not raw:
        if default is None:
            return None, f"{name} is required"
        return default, None
    try:
        value = int(raw)
    except ValueError:
        return None, f"{name} must be an integer"
    return min(hi, max(lo, value)), None


def _enum_param(
    qs: Dict[str, str], name: str, allowed: Tuple[str, ...], default: Optional[str]
) -> Tuple[Optional[str], Optional[str]]:
    """Parse an enum query param. Returns (value, error)."""
    raw = (qs.get(name) or "").strip().lower()
    if not raw:
        return default, None
    if raw not in allowed:
        return None, f"{name} must be one of: {', '.join(allowed)}"
    return raw, None


def _route_meta(store: LakeStore) -> Dict[str, Any]:
    """Service metadata: defaults, valid params, data source, warmed artifacts."""
    return _response(
        200,
        {
            "ok": True,
            "default_year": store.default_year(),
            "roles": list(VALID_ROLES),
            "archetype_roles": list(ARCHETYPE_ROLES),
            "models": list(PROJECTION_MODELS),
            "source": store.source,
            "cached_artifacts": store.cached_artifacts(),
        },
    )


def _route_projections(store: LakeStore, qs: Dict[str, str]) -> Dict[str, Any]:
    """Leaderboard of projected stat lines for one role, year, and model."""
    role, err = _enum_param(qs, "role", VALID_ROLES, "batter")
    if err:
        return _response(400, {"ok": False, "error": err})
    year, err = _int_param(qs, "year", store.default_year(), 1900, 2100)
    if err:
        return _response(400, {"ok": False, "error": err})
    model, err = _enum_param(qs, "model", PROJECTION_MODELS, "xgboost")
    if err:
        return _response(400, {"ok": False, "error": err})
    sortable = (*ROLE_ALL_TARGETS[role], "player_name", "age")
    sort, err = _enum_param(qs, "sort", sortable, default_sort(role))
    if err:
        return _response(400, {"ok": False, "error": err})
    order, err = _enum_param(qs, "order", ("asc", "desc"), default_order(role, sort))
    if err:
        return _response(400, {"ok": False, "error": err})
    limit, err = _int_param(qs, "limit", 100, 1, 1000)
    if err:
        return _response(400, {"ok": False, "error": err})

    df = store.projections(model, role, year)
    if df is None or df.empty:
        return _response(
            404,
            {"ok": False, "error": f"no {model} projections for role={role} year={year}"},
        )
    players = projection_rows(df, role=role, sort=sort, order=order, limit=limit)
    return _response(
        200,
        {
            "ok": True,
            "role": role,
            "year": year,
            "model": model,
            "sort": sort,
            "order": order,
            "total": len(df),
            "count": len(players),
            "players": players,
        },
    )


def _route_player(store: LakeStore, qs: Dict[str, str]) -> Dict[str, Any]:
    """One player's projection, history, archetype profile, and neighbors."""
    player_id, err = _int_param(qs, "player_id", None, 1, 99_999_999)
    if err:
        return _response(400, {"ok": False, "error": err})
    year, err = _int_param(qs, "year", store.default_year(), 1900, 2100)
    if err:
        return _response(400, {"ok": False, "error": err})
    model, err = _enum_param(qs, "model", PROJECTION_MODELS, "xgboost")
    if err:
        return _response(400, {"ok": False, "error": err})
    role, err = _enum_param(qs, "role", VALID_ROLES, None)
    if err:
        return _response(400, {"ok": False, "error": err})

    payload = player_detail_payload(
        store, player_id=player_id, year=year, model=model, role=role
    )
    if payload is None:
        return _response(
            404,
            {"ok": False, "error": f"no data for player_id={player_id} year={year}"},
        )
    return _response(200, {"ok": True, **payload})


def _route_accuracy(store: LakeStore, qs: Dict[str, str]) -> Dict[str, Any]:
    """Backtest comparison of xgboost vs the Marcel baseline over a year window."""
    role_filter, err = _enum_param(qs, "role", ("all", *VALID_ROLES), "all")
    if err:
        return _response(400, {"ok": False, "error": err})
    default_end = store.default_year()
    end_year, err = _int_param(qs, "end_year", default_end, 1900, 2100)
    if err:
        return _response(400, {"ok": False, "error": err})
    start_year, err = _int_param(qs, "start_year", end_year - 5, 1900, 2100)
    if err:
        return _response(400, {"ok": False, "error": err})
    if start_year > end_year:
        return _response(400, {"ok": False, "error": "start_year must be <= end_year"})
    start_year = max(start_year, end_year - _MAX_ACCURACY_SPAN + 1)

    payload = accuracy_payload(
        store, role_filter=role_filter, start_year=start_year, end_year=end_year
    )
    if payload is None:
        return _response(
            404,
            {
                "ok": False,
                "error": (
                    f"no role-years in {start_year}..{end_year} where both models "
                    "have backtest metrics"
                ),
            },
        )
    return _response(200, {"ok": True, "role": role_filter, **payload})


def _route_search(store: LakeStore, qs: Dict[str, str]) -> Dict[str, Any]:
    """Name-substring player search across projections and archetype cohorts."""
    year, err = _int_param(qs, "year", store.default_year(), 1900, 2100)
    if err:
        return _response(400, {"ok": False, "error": err})
    q = qs.get("q", "")
    hits = search_players(store.name_index(year), q, store.labels(year))
    return _response(200, {"ok": True, "q": q, "results": hits})


def _dispatch(path: str, qs: Dict[str, str]) -> Dict[str, Any]:
    if path in ("/api/health", "/health", "/"):
        return _response(200, {"ok": True, "service": "xwar-engine-api"})
    if path in _RETIRED_PATHS:
        return _response(
            410,
            {"ok": False, "error": "retired; use /api/projections or /api/player"},
        )

    store = get_store()
    if path in ("/api/meta", "/meta"):
        return _route_meta(store)
    if path in ("/api/projections", "/projections"):
        return _route_projections(store, qs)
    if path in ("/api/player", "/player"):
        return _route_player(store, qs)
    if path in ("/api/accuracy", "/accuracy"):
        return _route_accuracy(store, qs)
    if path in ("/api/search", "/search"):
        return _route_search(store, qs)

    return _response(404, {"ok": False, "error": f"no route for {path}"})


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    path, method = _extract_path_method(event)

    if method == "OPTIONS":
        return {"statusCode": 204, "headers": _CORS_HEADERS, "body": ""}

    if method != "GET":
        return _response(405, {"ok": False, "error": f"method {method} not allowed"})

    try:
        return _dispatch(path, _query(event))
    except Exception:
        logger.exception("Unhandled error for %s", path)
        return _response(500, {"ok": False, "error": "internal error"})
