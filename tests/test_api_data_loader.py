import pandas as pd

from src.api.data_loader import (
    _parse_cluster_labels_json,
    cluster_label,
    clusters_payload,
    search_players,
)


def test_cluster_label_uses_lookup_when_present():
    labels = {"batter": {0: "The Power Slugger", 2: "The Slap Hitter"}}
    assert cluster_label(labels, "batter", 0) == "The Power Slugger"
    assert cluster_label(labels, "batter", 2) == "The Slap Hitter"


def test_cluster_label_falls_back_when_missing():
    # Empty lookup → generic label.
    assert cluster_label({}, "batter", 3) == "Cluster 3"
    # Role known but cluster_id missing → fall back to "Cluster N".
    assert cluster_label({"batter": {0: "The Power Slugger"}}, "batter", 9) == "Cluster 9"
    # Role missing entirely → fall back.
    assert cluster_label({"batter": {0: "The Power Slugger"}}, "pitcher", 0) == "Cluster 0"


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
    assert out == {0: "The Slugger", 1: "The Speedster"}


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
    assert out == {0: "The Slugger", 3: "legacy string label"}


def test_parse_cluster_labels_json_empty_on_malformed_payload():
    assert _parse_cluster_labels_json(b"not json", "batter") == {}
    assert _parse_cluster_labels_json(b"{}", "batter") == {}
    assert _parse_cluster_labels_json(b'{"labels": []}', "batter") == {}


def _archetype_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "player_id": [10, 11, 12],
            "player_name": ["Alpha, A", "Beta, B", "Gamma, G"],
            "role": ["batter", "batter", "pitcher"],
            "cluster_id": [0, 1, 0],
            "year": [2024, 2024, 2024],
        }
    )


def test_clusters_payload_uses_labels_lookup():
    df = _archetype_frame()
    labels = {"batter": {0: "The Power Slugger"}}
    payload = clusters_payload(df, labels)
    by_role_cid = {(row["role"], row["cluster_id"]): row["label"] for row in payload}
    assert by_role_cid[("batter", 0)] == "The Power Slugger"
    # Missing labels fall back to "Cluster N".
    assert by_role_cid[("batter", 1)] == "Cluster 1"
    assert by_role_cid[("pitcher", 0)] == "Cluster 0"


def test_search_players_includes_cluster_label_from_lookup():
    df = _archetype_frame()
    labels = {"pitcher": {0: "The Power Starter"}}
    hits = search_players(df, "gamma", labels)
    assert len(hits) == 1
    assert hits[0]["cluster_label"] == "The Power Starter"
