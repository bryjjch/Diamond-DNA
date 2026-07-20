import json

import numpy as np
import pandas as pd
import pytest

from src.ml.archetypes.archetype_labeling import (
    DEFAULT_GEMINI_MODEL,
    _build_gemini_prompt,
    build_cluster_labels,
    compute_cluster_profiles,
)


def test_compute_cluster_profiles_ranks_features_by_abs_z():
    rng = np.random.default_rng(0)
    n = 60
    df = pd.DataFrame(
        {
            "cluster_id": np.repeat([0, 1], n // 2),
            # Feature that strongly separates clusters → should rank near the top.
            "exit_velocity": np.concatenate(
                [rng.normal(95.0, 0.5, n // 2), rng.normal(85.0, 0.5, n // 2)]
            ),
            # Feature with the same distribution across clusters → low |z|.
            "noise": rng.normal(0.0, 1.0, n),
        }
    )
    profiles = compute_cluster_profiles(
        df, ["exit_velocity", "noise"], top_n=2
    )
    assert set(profiles.keys()) == {0, 1}
    # Strongly separating feature should be the top entry for each cluster.
    for cid in (0, 1):
        top = profiles[cid][0]
        assert top["feature"] == "exit_velocity"
        # Cluster 0 is the high-velo cohort → positive z; cluster 1 → negative.
        if cid == 0:
            assert top["z_score"] > 0
        else:
            assert top["z_score"] < 0


def test_compute_cluster_profiles_handles_constant_feature():
    df = pd.DataFrame(
        {
            "cluster_id": [0, 0, 1, 1],
            "constant": [1.0, 1.0, 1.0, 1.0],
            "varying": [10.0, 11.0, 1.0, 2.0],
        }
    )
    profiles = compute_cluster_profiles(df, ["constant", "varying"], top_n=2)
    # Both clusters should still produce two entries; constant feature gets z=0.
    for cid in (0, 1):
        rows = profiles[cid]
        assert len(rows) == 2
        z_by_feat = {r["feature"]: r["z_score"] for r in rows}
        assert z_by_feat["constant"] == 0.0
        assert z_by_feat["varying"] != 0.0


def test_compute_cluster_profiles_validates_inputs():
    df = pd.DataFrame({"cluster_id": [0, 1], "x": [1.0, 2.0]})
    with pytest.raises(ValueError, match="top_n"):
        compute_cluster_profiles(df, ["x"], top_n=0)
    with pytest.raises(ValueError, match="No feature_cols"):
        compute_cluster_profiles(df, ["nonexistent"], top_n=2)
    with pytest.raises(ValueError, match="cluster_id"):
        compute_cluster_profiles(pd.DataFrame({"x": [1.0]}), ["x"], top_n=1)


def test_build_gemini_prompt_lists_each_cluster_id():
    profiles = {
        0: [{"feature": "f1", "z_score": 1.2, "cluster_mean": 0.5, "global_mean": 0.2}],
        2: [{"feature": "f2", "z_score": -1.5, "cluster_mean": 0.1, "global_mean": 0.4}],
    }
    prompt = _build_gemini_prompt(profiles, role="batter", year=2024)
    assert "role='batter'" in prompt
    assert "2024" in prompt
    assert "cluster_id=0" in prompt
    assert "cluster_id=2" in prompt
    assert "f1" in prompt and "f2" in prompt


def test_build_cluster_labels_writes_sidecar(monkeypatch):
    df = pd.DataFrame(
        {
            "cluster_id": [0, 0, 1, 1],
            "feat_a": [1.0, 1.5, 5.0, 5.5],
            "feat_b": [0.1, 0.2, 0.9, 1.0],
        }
    )
    fake_labels = {
        "0": {"name": "The Slow", "description": "Low values across the board."},
        "1": {"name": "The Fast", "description": "High values across the board."},
    }
    json_writes: dict[str, dict] = {}

    monkeypatch.setattr(
        "src.ml.archetypes.archetype_labeling.generate_cluster_labels",
        lambda profiles, **kwargs: fake_labels,
    )

    def fake_write(bucket, key, payload):
        json_writes[key] = payload

    monkeypatch.setattr(
        "src.ml.archetypes.archetype_labeling._write_json_to_s3",
        fake_write,
    )

    payload = build_cluster_labels(
        df,
        role="batter",
        year=2024,
        feature_cols=["feat_a", "feat_b"],
        bucket="b",
        key="gold/predictions/archetypes/batter/year=2024/cluster_labels.json",
    )
    assert payload["role"] == "batter"
    assert payload["year"] == 2024
    assert payload["n_clusters"] == 2
    assert payload["model"] == DEFAULT_GEMINI_MODEL
    assert payload["labels"] == fake_labels
    # Round-trip JSON to confirm the payload is serialisable.
    serialised = json.dumps(payload, default=str)
    assert "cluster_labels.json" in next(iter(json_writes.keys()))
    assert json.loads(serialised)["labels"]["0"]["name"] == "The Slow"
