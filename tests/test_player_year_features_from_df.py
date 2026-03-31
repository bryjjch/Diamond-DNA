import numpy as np
import pandas as pd

from src.features.build_player_year_archetype_features import player_year_features_from_df


def _pitcher_df(n: int = 600) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    return pd.DataFrame(
        {
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


def test_player_year_features_from_df_pitcher_minimal():
    df = _pitcher_df(600)
    row = player_year_features_from_df(
        df=df,
        role="pitcher",
        player_id=12345,
        year=2024,
        min_pitches_pitcher=500,
        min_pitches_batter=500,
        min_batted_ball_batter=200,
        hard_hit_speed_mph=95.0,
        min_pitches_per_pitch_type=15,
    )
    assert row is not None
    assert row["role"] == "pitcher"
    assert row["player_id"] == 12345
    assert row["year"] == 2024
    assert row["n_pitches_total"] == 600


def _batter_df(n: int = 600, with_bip: int = 250) -> pd.DataFrame:
    rng = np.random.default_rng(1)
    zone = rng.integers(0, 14, size=n)
    desc = ["hit_into_play"] * with_bip + ["called_strike"] * (n - with_bip)
    rng.shuffle(desc)
    return pd.DataFrame(
        {
            "zone": zone,
            "description": desc,
            "launch_speed": rng.normal(88, 15, size=n),
            "launch_angle": rng.normal(12, 20, size=n),
            "iso_value": rng.normal(0.15, 0.1, size=n),
            "estimated_slg_using_speedangle": rng.normal(0.4, 0.15, size=n),
            "woba_value": rng.normal(0.32, 0.1, size=n),
            "estimated_woba_using_speedangle": rng.normal(0.32, 0.1, size=n),
        }
    )


def test_player_year_features_from_df_batter_minimal():
    df = _batter_df(600, with_bip=300)
    row = player_year_features_from_df(
        df=df,
        role="batter",
        player_id=999,
        year=2023,
        min_pitches_pitcher=500,
        min_pitches_batter=500,
        min_batted_ball_batter=200,
        hard_hit_speed_mph=95.0,
        min_pitches_per_pitch_type=15,
        sprint_speed_lookup={999: 28.5},
    )
    assert row is not None
    assert row["sprint_speed_mean"] == 28.5
