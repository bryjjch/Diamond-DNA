"""Flask entrypoint: cluster browser + KNN similar players."""

from __future__ import annotations

import logging
import os
from typing import Any

from flask import Flask, jsonify, render_template, request

from .data_loader import (
    LakeTables,
    clusters_payload,
    load_lake_tables,
    neighbors_for_player,
    player_leaderboard,
    search_players,
)

logger = logging.getLogger(__name__)


def create_app() -> Flask:
    pkg = os.path.dirname(os.path.abspath(__file__))
    app = Flask(
        __name__,
        template_folder=os.path.join(pkg, "templates"),
        static_folder=os.path.join(pkg, "static"),
    )
    app.config["WEBAPP_TABLES"] = None
    app.config["WEBAPP_LOAD_ERROR"] = ""

    @app.before_request
    def _ensure_tables() -> None:
        if app.config["WEBAPP_TABLES"] is not None or app.config["WEBAPP_LOAD_ERROR"]:
            return
        tables, err = load_lake_tables()
        if tables is None:
            app.config["WEBAPP_LOAD_ERROR"] = err or "failed to load data"
            logger.error("Webapp data load failed: %s", app.config["WEBAPP_LOAD_ERROR"])
        else:
            app.config["WEBAPP_TABLES"] = tables
            logger.info("Webapp loaded %s rows archetypes, %s neighbors", len(tables.archetypes), len(tables.neighbors))

    def _tables() -> tuple[LakeTables | None, str]:
        return app.config["WEBAPP_TABLES"], str(app.config["WEBAPP_LOAD_ERROR"])

    @app.get("/")
    def index():
        tables, err = _tables()
        year = tables.year if tables else None
        source = tables.source if tables else ""
        return render_template(
            "index.html",
            load_error=err,
            year=year,
            source=source,
        )

    @app.get("/api/meta")
    def api_meta() -> Any:
        tables, err = _tables()
        if tables is None:
            return jsonify({"ok": False, "error": err}), 503
        return jsonify(
            {
                "ok": True,
                "year": tables.year,
                "source": tables.source,
                "notes": tables.notes,
                "n_archetype_rows": len(tables.archetypes),
                "n_neighbor_rows": len(tables.neighbors),
            }
        )

    @app.get("/api/clusters")
    def api_clusters() -> Any:
        tables, err = _tables()
        if tables is None:
            return jsonify({"ok": False, "error": err}), 503
        try:
            data = clusters_payload(tables.archetypes)
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 500
        return jsonify({"ok": True, "clusters": data})

    @app.get("/api/search")
    def api_search() -> Any:
        tables, err = _tables()
        if tables is None:
            return jsonify({"ok": False, "error": err}), 503
        q = request.args.get("q", "")
        try:
            hits = search_players(tables.archetypes, q)
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 500
        return jsonify({"ok": True, "q": q, "results": hits})

    @app.get("/api/leaderboard")
    def api_leaderboard() -> Any:
        tables, err = _tables()
        if tables is None:
            return jsonify({"ok": False, "error": err}), 503
        role = request.args.get("role", "batter")
        if role not in ("batter", "pitcher"):
            return jsonify({"ok": False, "error": "role must be batter or pitcher"}), 400
        limit = min(2000, max(1, int(request.args.get("limit", "500"))))
        players = player_leaderboard(tables.archetypes, role, limit=limit)
        return jsonify({"ok": True, "role": role, "players": players})

    @app.get("/api/neighbors")
    def api_neighbors() -> Any:
        tables, err = _tables()
        if tables is None:
            return jsonify({"ok": False, "error": err}), 503
        role = request.args.get("role", "batter")
        if role not in ("batter", "pitcher"):
            return jsonify({"ok": False, "error": "role must be batter or pitcher"}), 400
        try:
            player_id = int(request.args.get("player_id", ""))
        except ValueError:
            return jsonify({"ok": False, "error": "player_id must be an integer"}), 400
        try:
            n = neighbors_for_player(tables.neighbors, player_id=player_id, role=role)
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 500
        name_row = tables.archetypes.loc[
            (tables.archetypes["player_id"] == player_id)
            & (tables.archetypes["role"].str.lower() == role.lower())
        ]
        player_name = str(name_row.iloc[0]["player_name"]) if len(name_row) else None
        return jsonify(
            {
                "ok": True,
                "player_id": player_id,
                "player_name": player_name,
                "role": role,
                "neighbors": n,
            }
        )

    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    app = create_app()
    host = os.environ.get("WEBAPP_HOST", "127.0.0.1")
    port = int(os.environ.get("WEBAPP_PORT", "5000"))
    app.run(host=host, port=port, debug=os.environ.get("WEBAPP_DEBUG", "").lower() in ("1", "true", "yes"))


if __name__ == "__main__":
    main()
