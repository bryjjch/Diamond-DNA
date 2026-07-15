"""
Parity tests: the vectorized ``compute_player_year_features`` must produce the same
player-year feature tables as the loop-based reference ``player_year_features_from_df``.
"""

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
from src.data_pipeline.silver.archetype_features.silver_features_build import (
    player_year_features_from_df,
)
from src.data_pipeline.silver.archetype_features.sprint_helper import apply_sprint_speed_lookup

PARAMS = dict(
    min_pitches_pitcher=30,
    min_pitches_batter=30,
    min_batted_ball_batter=10,
    hard_hit_speed_mph=95.0,
    min_pitches_per_pitch_type=5,
)

# Ids chosen so most players clear the gates while 700/900 stay far below them.
BATTER_IDS = [100, 101, 102, 103, 700]
PITCHER_IDS = [200, 201, 202, 900]


def _synthetic_pitches(n: int = 6000, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    idx = np.arange(n)

    def with_nans(values, frac=0.12):
        out = np.asarray(values, dtype=float)
        out[rng.random(n) < frac] = np.nan
        return out

    def with_nones(values, frac=0.1):
        out = np.asarray(values, dtype=object)
        out[rng.random(n) < frac] = None
        return out

    description = rng.choice(
        [
            "called_strike",
            "ball",
            "swinging_strike",
            "foul",
            "hit_into_play",
            "blocked_ball",
            "foul_tip",
            "missed_bunt",
        ],
        size=n,
    )

    events = np.full(n, None, dtype=object)
    pa_mask = rng.random(n) < 0.25
    events[pa_mask] = rng.choice(
        ["walk", "intent_walk", "field_out", "strikeout", ""], size=int(pa_mask.sum())
    )

    bb_type = np.full(n, None, dtype=object)
    bip_mask = description == "hit_into_play"
    bb_type[bip_mask] = rng.choice(
        ["ground_ball", "line_drive", "fly_ball", "popup", ""], size=int(bip_mask.sum())
    )

    return pd.DataFrame(
        {
            "batter": with_nans(
                rng.choice(BATTER_IDS, size=n, p=[0.249, 0.249, 0.249, 0.249, 0.004]), frac=0.01
            ),
            "pitcher": rng.choice(PITCHER_IDS, size=n, p=[0.332, 0.332, 0.332, 0.004]).astype(
                float
            ),
            "year": rng.choice([2023, 2024], size=n),
            # Unique (game_pk, at_bat_number, pitch_number) triples; pitch 1..4 per PA.
            "game_pk": idx // 40,
            "at_bat_number": (idx % 40) // 4,
            "pitch_number": idx % 4 + 1,
            "description": with_nones(description, frac=0.03),
            "type": with_nones(rng.choice(["S", "B", "X"], size=n)),
            "zone": with_nans(rng.integers(0, 15, size=n), frac=0.15),
            "plate_x": with_nans(rng.normal(0, 0.8, size=n)),
            "plate_z": with_nans(rng.normal(2.5, 0.6, size=n)),
            "sz_top": with_nans(rng.normal(3.4, 0.1, size=n)),
            "sz_bot": with_nans(rng.normal(1.6, 0.1, size=n)),
            # "PO" is an excluded pitch type; None exercises the missing-type quirks.
            "pitch_type": with_nones(
                rng.choice(["FF", "SI", "SL", "CH", "CU", "PO"], size=n, p=[0.3, 0.15, 0.2, 0.15, 0.1, 0.1])
            ),
            "release_speed": with_nans(rng.normal(92, 4, size=n)),
            "release_spin_rate": with_nans(rng.normal(2200, 160, size=n)),
            "release_extension": with_nans(rng.normal(6.4, 0.4, size=n)),
            "pfx_x": with_nans(rng.normal(0, 0.6, size=n)),
            "pfx_z": with_nans(rng.normal(0.8, 0.5, size=n)),
            "delta_run_exp": with_nans(rng.normal(0, 0.12, size=n)),
            "stand": with_nones(rng.choice(["R", "L", "S"], size=n, p=[0.5, 0.45, 0.05])),
            "events": events,
            "bb_type": bb_type,
            "hc_x": with_nans(rng.normal(125, 40, size=n)),
            "hc_y": with_nans(rng.normal(150, 30, size=n)),
            "launch_speed": with_nans(rng.normal(88, 14, size=n), frac=0.35),
            "launch_angle": with_nans(rng.normal(12, 22, size=n), frac=0.35),
            "iso_value": with_nans(rng.normal(0.15, 0.1, size=n)),
            "estimated_slg_using_speedangle": with_nans(rng.normal(0.4, 0.15, size=n)),
            "estimated_woba_using_speedangle": with_nans(rng.normal(0.32, 0.1, size=n)),
            "woba_value": with_nans(rng.normal(0.32, 0.1, size=n)),
            "sprint_speed": with_nans(rng.normal(27, 1.2, size=n), frac=0.2),
        }
    )


def _reference_features(raw, role, sprint_lookup_by_year=None):
    """Build the feature table by looping the reference implementation per player-year."""
    rows = []
    for (pid, year), grp in raw.groupby([role, "year"], dropna=True):
        lookup = None
        if role == "batter" and sprint_lookup_by_year is not None:
            lookup = sprint_lookup_by_year.get(int(year))
        row = player_year_features_from_df(
            df=grp,
            role=role,
            player_id=int(pid),
            year=int(year),
            sprint_speed_lookup=lookup,
            **PARAMS,
        )
        if row is not None:
            rows.append(row)
    return pd.DataFrame(rows).drop(columns=["player_name"])


def _assert_frames_match(vec: pd.DataFrame, ref: pd.DataFrame) -> None:
    assert set(vec.columns) == set(ref.columns)
    vec = vec.sort_values(["player_id", "year"]).reset_index(drop=True)
    ref = ref.sort_values(["player_id", "year"]).reset_index(drop=True)[vec.columns]
    pd.testing.assert_frame_equal(vec, ref, check_dtype=False, rtol=1e-9, atol=1e-12)


def test_pitcher_parity():
    raw = _synthetic_pitches()
    vec = compute_player_year_features(raw, role="pitcher", **PARAMS)
    ref = _reference_features(raw, "pitcher")
    _assert_frames_match(vec, ref)
    # Low-volume pitcher is gated out by min_pitches_pitcher on both paths.
    assert 900 not in set(vec["player_id"])
    assert vec["pitch_type_entropy"].notna().any()


def test_batter_parity_with_sprint_lookup():
    raw = _synthetic_pitches()
    # 2023 has no leaderboard (column-mean fallback); in 2024, 102/103 are missing (NaN).
    lookups = {2024: {100: 29.1, 101: 27.4}}
    vec = apply_sprint_speed_lookup(
        compute_player_year_features(raw, role="batter", **PARAMS), lookups
    )
    ref = _reference_features(raw, "batter", sprint_lookup_by_year=lookups)
    _assert_frames_match(vec, ref)
    assert 700 not in set(vec["player_id"])


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
