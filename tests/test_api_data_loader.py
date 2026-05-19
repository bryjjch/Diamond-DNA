import pandas as pd

from src.api.data_loader import (
    _parse_cluster_labels_json,
    cluster_label,
    clusters_payload,
    player_soft_profile,
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


def _archetype_frame_with_soft() -> pd.DataFrame:
    """Two batters + one pitcher with K=3 cluster probability columns."""
    return pd.DataFrame(
        {
            "player_id": [10, 11, 12],
            "player_name": ["Alpha, A", "Beta, B", "Gamma, G"],
            "role": ["batter", "batter", "pitcher"],
            "cluster_id": [0, 1, 0],
            "cluster_id_secondary": [2, 0, 1],
            "prob_primary": [0.80, 0.55, 0.70],
            "prob_secondary": [0.15, 0.40, 0.25],
            "prob_0": [0.80, 0.40, 0.70],
            "prob_1": [0.05, 0.55, 0.25],
            "prob_2": [0.15, 0.05, 0.05],
            "year": [2024, 2024, 2024],
        }
    )


def test_clusters_payload_includes_soft_fields_when_present():
    df = _archetype_frame_with_soft()
    labels = {
        "batter": {0: "The Slugger", 1: "The Slap Hitter", 2: "The Speedster"},
        "pitcher": {0: "The Power Starter", 1: "The Junkballer"},
    }
    payload = clusters_payload(df, labels)
    by_role_cid = {(p["role"], p["cluster_id"]): p for p in payload}
    alpha = by_role_cid[("batter", 0)]["players"][0]
    assert alpha["prob_primary"] == 0.80
    assert alpha["cluster_id_secondary"] == 2
    assert alpha["prob_secondary"] == 0.15
    assert alpha["secondary_label"] == "The Speedster"
    assert "probs" not in alpha


def test_clusters_payload_omits_soft_fields_on_legacy_frame():
    df = _archetype_frame()  # No prob columns.
    payload = clusters_payload(df, {})
    for cluster in payload:
        for player in cluster["players"]:
            assert "prob_primary" not in player
            assert "cluster_id_secondary" not in player
            assert "probs" not in player
            assert "secondary_label" not in player


def test_search_players_includes_soft_fields_when_present():
    df = _archetype_frame_with_soft()
    labels = {"pitcher": {0: "The Power Starter", 1: "The Junkballer"}}
    hits = search_players(df, "gamma", labels)
    assert len(hits) == 1
    hit = hits[0]
    assert hit["prob_primary"] == 0.70
    assert hit["cluster_id_secondary"] == 1
    assert hit["secondary_cluster_label"] == "The Junkballer"
    assert hit["probs"] == [0.70, 0.25, 0.05]


def test_player_soft_profile_returns_sorted_membership():
    df = _archetype_frame_with_soft()
    labels = {"batter": {0: "The Slugger", 1: "The Slap Hitter", 2: "The Speedster"}}
    profile = player_soft_profile(df, labels, player_id=11, role="batter")
    assert profile is not None
    assert profile["player_id"] == 11
    assert profile["cluster_id"] == 1
    assert profile["cluster_label"] == "The Slap Hitter"
    # Probs sorted desc; cluster_id=1 first with 0.55.
    probs = profile["probs"]
    assert [p["cluster_id"] for p in probs] == [1, 0, 2]
    assert probs[0]["label"] == "The Slap Hitter"
    assert probs[0]["prob"] == 0.55


def test_player_soft_profile_returns_none_on_legacy_frame():
    df = _archetype_frame()
    assert player_soft_profile(df, {}, player_id=10, role="batter") is None


def test_player_soft_profile_returns_none_when_player_missing():
    df = _archetype_frame_with_soft()
    assert player_soft_profile(df, {}, player_id=999, role="batter") is None
