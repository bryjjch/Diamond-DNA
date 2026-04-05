import pandas as pd

from src.silver.silver_player_names import (
    build_mlbam_statcast_style_name_map,
    resolve_mlbam_display_name,
)


def test_build_mlbam_statcast_style_name_map():
    cw = pd.DataFrame(
        {
            "key_mlbam": [123, 456],
            "name_last": ["Smith", "Lee"],
            "name_first": ["John", ""],
        }
    )
    m = build_mlbam_statcast_style_name_map(cw)
    assert m[123] == "Smith, John"
    assert m[456] == "Lee"


def test_resolve_mlbam_display_name_hits_map():
    m = {999: "Jones, Aaron", 42: "Kershaw, Clayton"}
    assert resolve_mlbam_display_name(999, m) == "Jones, Aaron"
    assert resolve_mlbam_display_name(42, m) == "Kershaw, Clayton"


def test_resolve_mlbam_display_name_unknown():
    assert resolve_mlbam_display_name(1, {}) == ""
