"""Tests for the vectorized player-year feature table and enrichment joins."""

import math

import numpy as np
import pandas as pd

from src.data_pipeline.silver.archetype_features.defence_player_year_helper import (
    UNKNOWN_POSITION,
    merge_defence_features,
    merge_primary_position_features,
)
from src.data_pipeline.silver.archetype_features.player_year_feature_table import (
    compute_player_year_features,
)
from src.data_pipeline.silver.archetype_features.sprint_helper import apply_sprint_speed_lookup

PARAMS = dict(
    min_pitches_pitcher=500,
    min_pitches_batter=500,
    min_batted_ball_batter=200,
    hard_hit_speed_mph=95.0,
    min_pitches_per_pitch_type=15,
)


def _pitcher_df(n: int = 600, *, player_id: int = 12345, year: int = 2024, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "pitcher": float(player_id),
            "year": year,
            "pitch_type": rng.choice(["FF", "SL", "CH"], size=n),
            "release_speed": rng.normal(93, 3, size=n),
            "release_spin_rate": rng.normal(2200, 150, size=n),
            "release_extension": rng.normal(6.5, 0.3, size=n),
            "pfx_x": rng.normal(0, 0.5, size=n),
            "pfx_z": rng.normal(0, 0.5, size=n),
            "zone": rng.integers(0, 14, size=n),
            "description": ["called_strike"] * n,
            "plate_x": rng.normal(0, 0.5, size=n),
            "plate_z": rng.normal(2.5, 0.5, size=n),
            "delta_run_exp": rng.normal(0, 0.05, size=n),
        }
    )


def _batter_df(
    n: int = 600,
    with_bip: int = 250,
    *,
    player_id: int = 999,
    year: int = 2023,
    n_walks: int = 0,
    n_pa: int = 0,
) -> pd.DataFrame:
    rng = np.random.default_rng(1)
    desc = ["hit_into_play"] * with_bip + ["called_strike"] * (n - with_bip)
    rng.shuffle(desc)
    events = [None] * n
    if n_pa > 0:
        assert n_walks <= n_pa <= n
        ev_list = ["walk"] * n_walks + ["field_out"] * (n_pa - n_walks)
        rng.shuffle(ev_list)
        idx = rng.choice(n, size=n_pa, replace=False)
        for i, e in zip(idx, ev_list):
            events[int(i)] = e
    return pd.DataFrame(
        {
            "batter": float(player_id),
            "year": year,
            "zone": rng.integers(0, 14, size=n),
            "description": desc,
            "launch_speed": rng.normal(88, 15, size=n),
            "launch_angle": rng.normal(12, 20, size=n),
            "iso_value": rng.normal(0.15, 0.1, size=n),
            "estimated_slg_using_speedangle": rng.normal(0.4, 0.15, size=n),
            "woba_value": rng.normal(0.32, 0.1, size=n),
            "estimated_woba_using_speedangle": rng.normal(0.32, 0.1, size=n),
            "events": events,
        }
    )


def test_pitcher_features_minimal():
    feats = compute_player_year_features(_pitcher_df(600), role="pitcher", **PARAMS)
    assert len(feats) == 1
    row = feats.iloc[0]
    assert row["role"] == "pitcher"
    assert row["player_id"] == 12345
    assert row["year"] == 2024
    assert row["n_pitches_total"] == 600
    assert "pfx_x_mean" in feats.columns
    assert "batter_contact_rate" not in feats.columns


def test_pitcher_below_min_pitches_is_gated_out():
    feats = compute_player_year_features(_pitcher_df(400), role="pitcher", **PARAMS)
    assert feats.empty


def test_player_years_aggregate_independently():
    raw = pd.concat(
        [
            _pitcher_df(600, player_id=1, year=2023, seed=1),
            _pitcher_df(700, player_id=1, year=2024, seed=2),
            _pitcher_df(650, player_id=2, year=2024, seed=3),
        ],
        ignore_index=True,
    )
    feats = compute_player_year_features(raw, role="pitcher", **PARAMS).set_index(
        ["player_id", "year"]
    )
    assert len(feats) == 3
    assert feats.at[(1, 2023), "n_pitches_total"] == 600
    assert feats.at[(1, 2024), "n_pitches_total"] == 700
    assert feats.at[(2, 2024), "n_pitches_total"] == 650


def test_batter_features_minimal():
    feats = compute_player_year_features(_batter_df(600, with_bip=300), role="batter", **PARAMS)
    feats = apply_sprint_speed_lookup(feats, {2023: {999: 28.5}})
    assert len(feats) == 1
    row = feats.iloc[0]
    assert row["role"] == "batter"
    assert row["sprint_speed_mean"] == 28.5
    assert "walk_rate" in feats.columns
    assert "contact_rate" not in feats.columns


def test_batter_walk_rate():
    raw = _batter_df(600, with_bip=300, n_walks=20, n_pa=200)
    feats = compute_player_year_features(raw, role="batter", **PARAMS)
    assert feats.iloc[0]["walk_rate"] == 20 / 200


def test_apply_sprint_speed_lookup_year_scoping():
    feats = pd.DataFrame(
        {"player_id": [1, 1, 2], "year": [2023, 2024, 2024], "sprint_speed_mean": [26.0, 26.5, 27.0]}
    )
    out = apply_sprint_speed_lookup(feats, {2024: {1: 29.0}}).set_index(["player_id", "year"])
    # No 2023 leaderboard: pitch-level mean kept. 2024: leaderboard value, NaN when absent.
    assert out.at[(1, 2023), "sprint_speed_mean"] == 26.0
    assert out.at[(1, 2024), "sprint_speed_mean"] == 29.0
    assert math.isnan(out.at[(2, 2024), "sprint_speed_mean"])


def test_merge_defence_and_position_features():
    feats = pd.DataFrame({"player_id": [1, 2], "year": [2024, 2024]})
    defence = {2024: {1: {"def_oaa_total": 5.0, "def_framing_runs": 1.5}}}
    out = merge_defence_features(feats, defence).set_index("player_id")
    assert out.at[1, "def_oaa_total"] == 5.0
    assert math.isnan(out.at[2, "def_oaa_total"])

    out = merge_primary_position_features(feats, {2024: {1: "SS"}}).set_index("player_id")
    assert out.at[1, "primary_position"] == "SS"
    assert out.at[2, "primary_position"] == UNKNOWN_POSITION
