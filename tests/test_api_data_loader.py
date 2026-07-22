import json

import numpy as np
import pandas as pd
import pytest

from src.api.data_loader import (
    LakeStore,
    _parse_cluster_labels_json,
    accuracy_payload,
    cluster_label,
    player_detail_payload,
    player_history_payload,
    player_soft_profile,
    projection_rows,
    search_players,
)

YEAR = 2025


# ---------------------------------------------------------------------------
# Lake fixture: a local dir mirroring the S3 lake layout
# ---------------------------------------------------------------------------


def _write_parquet(root, rel_key, df):
    path = root / rel_key
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _write_json(root, rel_key, payload):
    path = root / rel_key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def _metrics_payload(stats):
    return {
        "metrics": {
            stat: {
                "n": 100,
                "mae": mae,
                "rmse": rmse,
                "bias": bias,
                "actual_mean": 0.0,
                "proj_mean": 0.0,
            }
            for stat, (mae, rmse, bias) in stats.items()
        }
    }


def build_lake(root):
    """Populate ``root`` with a minimal lake mirror: 2 batters + 1 catcher-as-batter,
    1 pitcher, archetypes/neighbors for batter+catcher, one backtest year (2023)."""
    _write_parquet(
        root,
        f"gold/predictions/xgboost/batter/year={YEAR}/xgboost_projections.parquet",
        pd.DataFrame(
            {
                "player_id": [10, 11, 30],
                "player_name": ["Alpha, A", "Beta, B", "Charlie, C"],
                "year": YEAR,
                "role": "batter",
                "age": [27.0, 31.0, 29.0],
                "primary_position": ["CF", "1B", "C"],
                "proj_pa": [600.0, 550.0, 400.0],
                "proj_ops": [0.850, 0.700, 0.650],
                "proj_hr": [30.0, 15.0, 8.0],
                "target_pa": [610.0, np.nan, np.nan],
                "target_ops": [0.900, np.nan, np.nan],
                "target_hr": [35.0, np.nan, np.nan],
            }
        ),
    )
    _write_parquet(
        root,
        f"gold/predictions/marcel/batter/year={YEAR}/marcel_projections.parquet",
        pd.DataFrame(
            {
                "player_id": [10, 11],
                "player_name": ["Alpha, A", "Beta, B"],
                "year": YEAR,
                "role": "batter",
                "age": [27.0, 31.0],
                "primary_position": ["CF", "1B"],
                "proj_pa": [590.0, 540.0],
                "proj_ops": [0.820, 0.710],
                "proj_hr": [28.0, 14.0],
            }
        ),
    )
    _write_parquet(
        root,
        f"gold/predictions/xgboost/pitcher/year={YEAR}/xgboost_projections.parquet",
        pd.DataFrame(
            {
                "player_id": [20, 21],
                "player_name": ["Delta, D", "Echo, E"],
                "year": YEAR,
                "role": "pitcher",
                "age": [25.0, 33.0],
                "primary_position": ["SP", "RP"],
                "proj_innings": [180.0, 60.0],
                "proj_era": [3.50, 4.20],
            }
        ),
    )
    _write_parquet(
        root,
        f"gold/predictions/archetypes/batter/year={YEAR}/player_year_archetypes.parquet",
        pd.DataFrame(
            {
                "player_id": [10, 11],
                "player_name": ["Alpha, A", "Beta, B"],
                "year": YEAR,
                "role": "batter",
                "cluster_id": [0, 1],
                "cluster_id_secondary": [1, 0],
                "prob_primary": [0.80, 0.55],
                "prob_secondary": [0.15, 0.40],
                "prob_0": [0.80, 0.40],
                "prob_1": [0.15, 0.55],
                "prob_2": [0.05, 0.05],
            }
        ),
    )
    _write_json(
        root,
        f"gold/predictions/archetypes/batter/year={YEAR}/cluster_labels.json",
        {
            "labels": {
                "0": {"name": "The Slugger", "description": "Crushes the ball."},
                "1": {"name": "The Speedster", "description": "Runs fast."},
            }
        },
    )
    _write_parquet(
        root,
        f"gold/predictions/archetypes/catcher/year={YEAR}/player_year_archetypes.parquet",
        pd.DataFrame(
            {
                "player_id": [30],
                "player_name": ["Charlie, C"],
                "year": YEAR,
                "role": "catcher",
                "cluster_id": [0],
                "cluster_id_secondary": [1],
                "prob_primary": [0.90],
                "prob_secondary": [0.10],
                "prob_0": [0.90],
                "prob_1": [0.10],
            }
        ),
    )
    _write_parquet(
        root,
        f"gold/predictions/neighbors/batter/year={YEAR}/player_year_similar_neighbors.parquet",
        pd.DataFrame(
            {
                "player_id": [10, 10],
                "player_name": ["Alpha, A", "Alpha, A"],
                "year": YEAR,
                "role": "batter",
                "neighbor_rank": [1, 2],
                "neighbor_player_id": [11, 30],
                "neighbor_player_name": ["Beta, B", "Charlie, C"],
                "distance": [0.4, 0.9],
            }
        ),
    )
    _write_parquet(
        root,
        f"gold/predictions/neighbors/catcher/year={YEAR}/player_year_similar_neighbors.parquet",
        pd.DataFrame(
            {
                "player_id": [30],
                "player_name": ["Charlie, C"],
                "year": YEAR,
                "role": "catcher",
                "neighbor_rank": [1],
                "neighbor_player_id": [10],
                "neighbor_player_name": ["Alpha, A"],
                "distance": [0.7],
            }
        ),
    )
    _write_parquet(
        root,
        f"gold/features/performance_prediction/batter/year={YEAR}/training_matrix.parquet",
        pd.DataFrame(
            {
                "player_id": [10, 30],
                "player_name": ["Alpha, A", "Charlie, C"],
                "year": YEAR,
                "role": "batter",
                "primary_position": ["CF", "C"],
                "age": [27.0, 29.0],
                "lag1_available": [1.0, 1.0],
                "lag1_pa": [610.0, 380.0],
                "lag1_avg": [0.300, 0.250],
                "lag2_available": [1.0, 0.0],
                "lag2_pa": [580.0, np.nan],
                "lag3_available": [0.0, 0.0],
                "lag3_pa": [np.nan, np.nan],
                "skill_lag1_available": [1.0, 0.0],
                "skill_lag1_barrel_rate": [0.12, np.nan],
                # Not needed by the API; must be dropped by column pushdown.
                "lag_weighted_avg": [0.295, 0.248],
                "target_pa": [600.0, 400.0],
            }
        ),
    )
    _write_json(
        root,
        "models/marcel/batter/year=2023/metrics.json",
        _metrics_payload({"obp": (0.020, 0.030, 0.001), "hr": (5.0, 7.0, -0.5)}),
    )
    _write_json(
        root,
        "models/xgboost/batter/year=2023/metrics.json",
        _metrics_payload({"obp": (0.016, 0.024, 0.002), "hr": (5.5, 7.7, 0.3)}),
    )
    return root


@pytest.fixture
def lake_dir(tmp_path):
    return build_lake(tmp_path)


@pytest.fixture
def store(lake_dir):
    return LakeStore(local_dir=str(lake_dir))


# ---------------------------------------------------------------------------
# Cluster label helpers
# ---------------------------------------------------------------------------


def test_cluster_label_uses_lookup_when_present():
    labels = {"batter": {0: {"name": "The Power Slugger", "description": ""}}}
    assert cluster_label(labels, "batter", 0) == "The Power Slugger"


def test_cluster_label_falls_back_when_missing():
    assert cluster_label({}, "batter", 3) == "Cluster 3"
    labels = {"batter": {0: {"name": "The Power Slugger", "description": ""}}}
    assert cluster_label(labels, "batter", 9) == "Cluster 9"
    assert cluster_label(labels, "pitcher", 0) == "Cluster 0"


def test_parse_cluster_labels_json_handles_well_formed_payload():
    raw = b"""
    {
      "role": "batter",
      "year": 2024,
      "labels": {
        "0": {"name": "The Slugger", "description": "..."},
        "1": {"name": "The Speedster", "description": "..."}
      }
    }
    """
    out = _parse_cluster_labels_json(raw, "batter")
    assert out == {
        0: {"name": "The Slugger", "description": "..."},
        1: {"name": "The Speedster", "description": "..."},
    }


def test_parse_cluster_labels_json_drops_bad_entries():
    raw = b"""
    {
      "labels": {
        "0": {"name": "The Slugger", "description": "..."},
        "not_an_int": {"name": "Ignored"},
        "2": {"name": "", "description": "blank name dropped"},
        "3": "legacy string label"
      }
    }
    """
    out = _parse_cluster_labels_json(raw, "batter")
    assert out == {
        0: {"name": "The Slugger", "description": "..."},
        3: {"name": "legacy string label", "description": ""},
    }


def test_parse_cluster_labels_json_empty_on_malformed_payload():
    assert _parse_cluster_labels_json(b"not json", "batter") == {}
    assert _parse_cluster_labels_json(b"{}", "batter") == {}
    assert _parse_cluster_labels_json(b'{"labels": []}', "batter") == {}


def test_player_soft_profile_returns_sorted_membership(store):
    df = store.archetypes("batter", YEAR)
    labels = {"batter": store.role_labels("batter", YEAR)}
    profile = player_soft_profile(df, labels, player_id=11, role="batter")
    assert profile is not None
    assert profile["cluster_id"] == 1
    assert profile["cluster_label"] == "The Speedster"
    probs = profile["probs"]
    assert [p["cluster_id"] for p in probs] == [1, 0, 2]
    assert probs[0]["prob"] == 0.55


def test_player_soft_profile_returns_none_when_player_missing(store):
    df = store.archetypes("batter", YEAR)
    assert player_soft_profile(df, {}, player_id=999, role="batter") is None


# ---------------------------------------------------------------------------
# LakeStore
# ---------------------------------------------------------------------------


def test_store_reads_lake_layout_keys(store):
    assert len(store.projections("xgboost", "batter", YEAR)) == 3
    assert len(store.projections("marcel", "batter", YEAR)) == 2
    assert len(store.archetypes("catcher", YEAR)) == 1
    assert len(store.neighbors("batter", YEAR)) == 2
    assert store.role_labels("batter", YEAR)[0]["name"] == "The Slugger"
    assert store.metrics("marcel", "batter", 2023)["metrics"]["obp"]["mae"] == 0.020


def test_store_returns_none_on_missing_artifacts(store):
    assert store.projections("xgboost", "batter", 1999) is None
    assert store.archetypes("pitcher", YEAR) is None
    assert store.metrics("xgboost", "pitcher", 2023) is None
    assert store.role_labels("catcher", YEAR) == {}


def test_store_caches_fetches_including_misses(store, monkeypatch):
    calls = []
    original = LakeStore._fetch_parquet

    def counting(self, rel_key, columns=None):
        calls.append(rel_key)
        return original(self, rel_key, columns)

    monkeypatch.setattr(LakeStore, "_fetch_parquet", counting)
    store.projections("xgboost", "batter", YEAR)
    store.projections("xgboost", "batter", YEAR)
    store.projections("xgboost", "batter", 1999)
    store.projections("xgboost", "batter", 1999)
    assert len(calls) == 2


def test_training_frame_pushes_down_columns_and_indexes(store):
    df = store.training_frame("batter", YEAR)
    assert "lag_weighted_avg" not in df.columns
    assert "target_pa" not in df.columns
    assert "lag1_pa" in df.columns
    assert "skill_lag1_barrel_rate" in df.columns
    row = store.training_row(10, "batter", YEAR)
    assert row["lag1_pa"] == 610.0
    assert store.training_row(999, "batter", YEAR) is None


def test_name_index_unions_projections_and_archetypes(store):
    index = store.name_index(YEAR)
    keys = set(zip(index["player_id"], index["role"]))
    # Catcher archetype cohort + batter/pitcher projections, deduped per (id, role).
    assert (30, "catcher") in keys
    assert (30, "batter") in keys
    assert (20, "pitcher") in keys
    assert len(index[(index["player_id"] == 10) & (index["role"] == "batter")]) == 1


# ---------------------------------------------------------------------------
# Payload builders
# ---------------------------------------------------------------------------


def test_projection_rows_sorts_limits_and_cleans_nan(store):
    df = store.projections("xgboost", "batter", YEAR)
    rows = projection_rows(df, role="batter", sort="ops", order="desc", limit=2)
    assert [r["player_id"] for r in rows] == [10, 11]
    assert rows[0]["projections"]["ops"] == 0.850
    # Stats absent from the frame come back as None, not KeyError.
    assert rows[0]["projections"]["avg"] is None
    assert rows[0]["actuals"]["ops"] == 0.900
    # All-NaN actuals collapse to None.
    assert rows[1]["actuals"] is None


def test_projection_rows_ascending(store):
    df = store.projections("xgboost", "pitcher", YEAR)
    rows = projection_rows(df, role="pitcher", sort="era", order="asc", limit=10)
    assert [r["player_id"] for r in rows] == [20, 21]


def test_player_history_payload_seasons_and_statcast(store):
    row = store.training_row(10, "batter", YEAR)
    payload = player_history_payload(row, "batter", YEAR)
    seasons = payload["seasons"]
    assert [s["season"] for s in seasons] == [YEAR - 1, YEAR - 2, YEAR - 3]
    assert seasons[0]["available"] is True
    assert seasons[0]["stats"]["pa"] == 610.0
    assert seasons[2]["available"] is False
    assert seasons[2]["stats"]["pa"] is None
    statcast = payload["statcast_last_season"]
    assert statcast["available"] is True
    assert statcast["barrel_rate"] == 0.12


def test_player_detail_payload_full_profile(store):
    payload = player_detail_payload(store, player_id=10, year=YEAR, model="xgboost")
    assert payload["player_name"] == "Alpha, A"
    assert len(payload["profiles"]) == 1
    profile = payload["profiles"][0]
    assert profile["role"] == "batter"
    assert profile["age"] == 27.0
    assert profile["projection"]["stats"]["ops"] == 0.850
    assert profile["history"]["seasons"][0]["stats"]["pa"] == 610.0
    assert profile["archetype"]["archetype_role"] == "batter"
    assert profile["archetype"]["label"] == "The Slugger"
    assert profile["archetype"]["description"] == "Crushes the ball."
    assert [n["player_id"] for n in profile["neighbors"]] == [11, 30]


def test_player_detail_payload_catcher_fallback(store):
    payload = player_detail_payload(store, player_id=30, year=YEAR, model="xgboost")
    profile = payload["profiles"][0]
    assert profile["role"] == "batter"
    assert profile["projection"]["stats"]["ops"] == 0.650
    assert profile["archetype"]["archetype_role"] == "catcher"
    assert [n["player_id"] for n in profile["neighbors"]] == [10]


def test_player_detail_payload_unknown_player(store):
    assert player_detail_payload(store, player_id=999, year=YEAR, model="xgboost") is None


def test_search_players_over_name_index(store):
    hits = search_players(store.name_index(YEAR), "alpha", store.labels(YEAR))
    assert len(hits) == 1
    hit = hits[0]
    assert hit["player_id"] == 10
    assert hit["cluster_label"] == "The Slugger"
    assert hit["secondary_cluster_label"] == "The Speedster"
    # Projection-only players match without cluster enrichment.
    pitcher_hits = search_players(store.name_index(YEAR), "delta", store.labels(YEAR))
    assert pitcher_hits[0]["player_id"] == 20
    assert "cluster_id" not in pitcher_hits[0]


def test_search_players_empty_query(store):
    assert search_players(store.name_index(YEAR), "  ", store.labels(YEAR)) == []


def test_accuracy_payload_from_sidecars(store):
    payload = accuracy_payload(store, role_filter="all", start_year=2020, end_year=2025)
    assert payload["years_compared"] == [2023]
    assert {d["stat"] for d in payload["detail"]} == {"obp", "hr"}
    obp = next(d for d in payload["detail"] if d["stat"] == "obp")
    assert obp["xgboost"]["mae"] == 0.016
    assert obp["marcel"]["mae"] == 0.020
    assert obp["mae_pct_improvement"] == pytest.approx(20.0)
    assert obp["candidate_wins_mae"] is True
    summary = {s["stat"]: s for s in payload["summary"]}
    assert summary["obp"]["win_rate_mae"] == pytest.approx(1.0)
    assert summary["hr"]["win_rate_mae"] == pytest.approx(0.0)


def test_accuracy_payload_none_when_no_overlap(store):
    assert accuracy_payload(store, role_filter="all", start_year=2024, end_year=2025) is None
