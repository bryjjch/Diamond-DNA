import json

import pytest

import src.api.data_loader as data_loader
from src.api.handler import handler
from tests.test_api_data_loader import YEAR, build_lake


def _event(path, qs=None, method="GET"):
    return {
        "requestContext": {"http": {"method": method, "path": path}},
        "queryStringParameters": qs or {},
    }


def _call(path, qs=None, method="GET"):
    resp = handler(_event(path, qs, method), None)
    return resp["statusCode"], json.loads(resp["body"]), resp["body"]


@pytest.fixture
def lake(tmp_path, monkeypatch):
    build_lake(tmp_path)
    monkeypatch.setenv("WEBAPP_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("WEBAPP_YEAR", str(YEAR))
    monkeypatch.setattr(data_loader, "_store", None)
    return tmp_path


def test_health():
    status, body, _ = _call("/api/health")
    assert status == 200
    assert body["ok"] is True


def test_options_preflight():
    resp = handler(_event("/api/projections", method="OPTIONS"), None)
    assert resp["statusCode"] == 204


def test_non_get_rejected():
    status, body, _ = _call("/api/projections", method="POST")
    assert status == 405


def test_unknown_route():
    status, body, _ = _call("/api/nope")
    assert status == 404


def test_retired_routes_return_410():
    for path in ("/api/clusters", "/api/leaderboard", "/api/neighbors", "/api/player_soft_profile"):
        status, body, _ = _call(path)
        assert status == 410
        assert "retired" in body["error"]


def test_meta(lake):
    status, body, _ = _call("/api/meta")
    assert status == 200
    assert body["default_year"] == YEAR
    assert body["models"] == ["xgboost", "marcel"]
    assert body["source"].startswith("local:")


def test_projections_defaults(lake):
    status, body, raw = _call("/api/projections")
    assert status == 200
    assert (body["role"], body["year"], body["model"]) == ("batter", YEAR, "xgboost")
    assert (body["sort"], body["order"]) == ("ops", "desc")
    assert body["total"] == 3 and body["count"] == 3
    assert [p["player_id"] for p in body["players"]] == [10, 11, 30]
    assert "NaN" not in raw


def test_projections_limit_and_order(lake):
    status, body, _ = _call("/api/projections", {"limit": "1", "order": "asc"})
    assert status == 200
    assert body["count"] == 1
    assert body["players"][0]["player_id"] == 30


def test_projections_pitcher_default_sort_is_era_asc(lake):
    status, body, _ = _call("/api/projections", {"role": "pitcher"})
    assert status == 200
    assert (body["sort"], body["order"]) == ("era", "asc")
    assert [p["player_id"] for p in body["players"]] == [20, 21]


def test_projections_marcel_model(lake):
    status, body, _ = _call("/api/projections", {"model": "marcel"})
    assert status == 200
    assert body["total"] == 2


def test_projections_bad_params(lake):
    assert _call("/api/projections", {"role": "shortstop"})[0] == 400
    assert _call("/api/projections", {"model": "zips"})[0] == 400
    assert _call("/api/projections", {"sort": "wins"})[0] == 400
    assert _call("/api/projections", {"year": "next"})[0] == 400


def test_projections_missing_year_404(lake):
    status, body, _ = _call("/api/projections", {"year": "1999"})
    assert status == 404


def test_player_full_profile(lake):
    status, body, raw = _call("/api/player", {"player_id": "10"})
    assert status == 200
    assert body["player_name"] == "Alpha, A"
    profile = body["profiles"][0]
    assert profile["projection"]["stats"]["ops"] == 0.850
    assert profile["history"]["statcast_last_season"]["barrel_rate"] == 0.12
    assert profile["archetype"]["label"] == "The Slugger"
    assert profile["neighbors"][0]["player_id"] == 11
    assert "NaN" not in raw


def test_player_catcher_archetype_fallback(lake):
    status, body, _ = _call("/api/player", {"player_id": "30"})
    assert status == 200
    profile = body["profiles"][0]
    assert profile["role"] == "batter"
    assert profile["archetype"]["archetype_role"] == "catcher"


def test_player_role_filter(lake):
    status, body, _ = _call("/api/player", {"player_id": "20", "role": "pitcher"})
    assert status == 200
    assert body["profiles"][0]["role"] == "pitcher"
    assert body["profiles"][0]["projection"]["stats"]["era"] == 3.50


def test_player_bad_and_missing_params(lake):
    assert _call("/api/player")[0] == 400
    assert _call("/api/player", {"player_id": "abc"})[0] == 400
    assert _call("/api/player", {"player_id": "999"})[0] == 404


def test_accuracy_default_window(lake):
    status, body, _ = _call("/api/accuracy")
    assert status == 200
    assert body["baseline_model"] == "marcel"
    assert body["candidate_model"] == "xgboost"
    assert body["years_compared"] == [2023]
    obp = next(d for d in body["detail"] if d["stat"] == "obp")
    assert obp["mae_pct_improvement"] == pytest.approx(20.0)
    assert body["summary"]


def test_accuracy_no_overlap_404(lake):
    status, body, _ = _call("/api/accuracy", {"start_year": "2024"})
    assert status == 404


def test_accuracy_bad_role(lake):
    assert _call("/api/accuracy", {"role": "catcher"})[0] == 400


def test_search(lake):
    status, body, _ = _call("/api/search", {"q": "alpha"})
    assert status == 200
    assert body["results"][0]["player_id"] == 10
    assert body["results"][0]["cluster_label"] == "The Slugger"


def test_search_empty_query(lake):
    status, body, _ = _call("/api/search")
    assert status == 200
    assert body["results"] == []
