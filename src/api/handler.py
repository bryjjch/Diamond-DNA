"""AWS Lambda handler for the Diamond-DNA HTTP API.

Routes API Gateway HTTP API v2 events to data-loader functions, 
returning JSON. Pre-computed archetype and neighbor tables
are loaded from S3 on cold start and cached for the lifetime of the container.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Tuple

from .data_loader import (
    LakeTables,
    clusters_payload,
    load_lake_tables,
    neighbors_for_player,
    player_leaderboard,
    search_players,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_CORS_HEADERS: Dict[str, str] = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}

_tables_cache: Optional[LakeTables] = None
_load_error: str = ""


def _get_tables() -> Tuple[Optional[LakeTables], str]:
    """Lazy-load the lake tables once per container, mirroring the Flask app."""
    global _tables_cache, _load_error
    if _tables_cache is not None or _load_error:
        return _tables_cache, _load_error
    tables, err = load_lake_tables()
    if tables is None:
        _load_error = err or "failed to load data"
        logger.error("API data load failed: %s", _load_error)
    else:
        _tables_cache = tables
        logger.info(
            "API loaded %s archetype rows, %s neighbor rows",
            len(tables.archetypes),
            len(tables.neighbors),
        )
    return _tables_cache, _load_error


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


def _route_meta() -> Dict[str, Any]:
    """Route metadata for the currently loaded data, including row counts and source info."""
    tables, err = _get_tables()
    if tables is None:
        return _response(503, {"ok": False, "error": err})
    return _response(
        200,
        {
            "ok": True,
            "year": tables.year,
            "source": tables.source,
            "notes": tables.notes,
            "n_archetype_rows": len(tables.archetypes),
            "n_neighbor_rows": len(tables.neighbors),
        },
    )


def _route_clusters() -> Dict[str, Any]:
    """Route the archetype clusters, returning player counts and cluster centers."""
    tables, err = _get_tables()
    if tables is None:
        return _response(503, {"ok": False, "error": err})
    try:
        data = clusters_payload(tables.archetypes)
    except ValueError as exc:
        return _response(500, {"ok": False, "error": str(exc)})
    return _response(200, {"ok": True, "clusters": data})


def _route_search(qs: Dict[str, str]) -> Dict[str, Any]:
    """Route a search query, returning matching players and their archetypes."""
    tables, err = _get_tables()
    if tables is None:
        return _response(503, {"ok": False, "error": err})
    q = qs.get("q", "")
    try:
        hits = search_players(tables.archetypes, q)
    except ValueError as exc:
        return _response(500, {"ok": False, "error": str(exc)})
    return _response(200, {"ok": True, "q": q, "results": hits})


def _route_leaderboard(qs: Dict[str, str]) -> Dict[str, Any]:
    """Route a leaderboard query, returning top players by archetype similarity."""
    tables, err = _get_tables()
    if tables is None:
        return _response(503, {"ok": False, "error": err})
    role = qs.get("role", "batter")
    if role not in ("batter", "pitcher"):
        return _response(400, {"ok": False, "error": "role must be batter or pitcher"})
    try:
        limit = min(2000, max(1, int(qs.get("limit", "500"))))
    except ValueError:
        return _response(400, {"ok": False, "error": "limit must be an integer"})
    players = player_leaderboard(tables.archetypes, role, limit=limit)
    return _response(200, {"ok": True, "role": role, "players": players})


def _route_neighbors(qs: Dict[str, str]) -> Dict[str, Any]:
    """Route a neighbors query, returning the most similar players to a given player ID."""
    tables, err = _get_tables()
    if tables is None:
        return _response(503, {"ok": False, "error": err})
    role = qs.get("role", "batter")
    if role not in ("batter", "pitcher"):
        return _response(400, {"ok": False, "error": "role must be batter or pitcher"})
    try:
        player_id = int(qs.get("player_id", ""))
    except ValueError:
        return _response(400, {"ok": False, "error": "player_id must be an integer"})
    try:
        n = neighbors_for_player(tables.neighbors, player_id=player_id, role=role)
    except ValueError as exc:
        return _response(500, {"ok": False, "error": str(exc)})
    name_row = tables.archetypes.loc[
        (tables.archetypes["player_id"] == player_id)
        & (tables.archetypes["role"].str.lower() == role.lower())
    ]
    player_name = str(name_row.iloc[0]["player_name"]) if len(name_row) else None
    return _response(
        200,
        {
            "ok": True,
            "player_id": player_id,
            "player_name": player_name,
            "role": role,
            "neighbors": n,
        },
    )


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    path, method = _extract_path_method(event)

    if method == "OPTIONS":
        return {"statusCode": 204, "headers": _CORS_HEADERS, "body": ""}

    if method != "GET":
        return _response(405, {"ok": False, "error": f"method {method} not allowed"})

    qs = _query(event)

    if path in ("/api/meta", "/meta"):
        return _route_meta()
    if path in ("/api/clusters", "/clusters"):
        return _route_clusters()
    if path in ("/api/search", "/search"):
        return _route_search(qs)
    if path in ("/api/leaderboard", "/leaderboard"):
        return _route_leaderboard(qs)
    if path in ("/api/neighbors", "/neighbors"):
        return _route_neighbors(qs)
    if path in ("/api/health", "/health", "/"):
        return _response(200, {"ok": True, "service": "diamond-dna-api"})

    return _response(404, {"ok": False, "error": f"no route for {path}"})