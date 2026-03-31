# Player-year Archetype Feature Schema (Pitch-derived)

This document describes the output of:
`src/features/build_player_year_archetype_features.py`

## Output rows
Each row corresponds to exactly one player-year:
`(role, player_id, year)`

## Derived flags used
These flags are computed from pitch-level columns:

### `in_zone` (both roles)
Primary: `zone` grid where `zone` in `[1..9]`.

Fallback (when `zone` missing): `plate_z` between `sz_bot` and `sz_top` (inclusive).

### `swing_flag` (both roles)
Computed from `description`:
- swing if `description` contains:
  - `swinging_strike`
  - `foul`
  - `hit_into_play`
  - plus a couple rare bunt cases (`missed_bunt`, `bunt_foul`)

### `whiff_flag` (both roles, pitch-derived)
- whiff if `swing_flag` and `description` contains `swinging_strike`

### `contact_flag` (both roles, pitch-derived)
- contact if `swing_flag` and not `whiff_flag`

### `barrel_flag` (batters only)
Computed from `launch_speed` + `launch_angle` using the default barrel definition:
- `launch_speed >= 98 mph`
- `26 <= launch_angle <= 30 degrees`

### `hard_hit_flag` (batters only)
- `launch_speed >= 95 mph`

## Pitcher output features
Counts:
- `n_pitches_total`

Aggression/context (batter outcomes vs this pitcher):
- `batter_swing_rate`
- `batter_zone_swing_rate`
- `batter_chase_rate`
- `batter_contact_rate`
- `batter_whiff_rate`

Command/location:
- `in_zone_rate`
- `plate_x_mean`, `plate_x_sd`
- `plate_z_mean`, `plate_z_sd`

Stuff (physical):
- `release_speed_mean`, `release_speed_iqr`
- `release_spin_rate_mean`, `release_spin_rate_iqr`
- `release_extension_mean`, `release_extension_iqr`
- `pfx_x_mean`, `pfx_x_iqr`
- `pfx_z_mean`, `pfx_z_iqr`

Run value:
- `delta_pitcher_run_exp_mean` (NA if missing in source parquet)

Pitch repertoire:
- `pitch_type_<PT>_share` for each pitch type observed in the processed slice
- `pitch_type_entropy`

## Batter output features
Counts:
- `n_pitches_total`

Approach (batter outcomes):
- `swing_rate`
- `zone_swing_rate`
- `chase_rate`
- `contact_rate`
- `whiff_rate`

Contact quality / batted-ball metrics:
- `launch_speed_mean`, `launch_speed_iqr`
- `launch_angle_mean`, `launch_angle_iqr`
- `hard_hit_rate` (computed among pitches with non-null `launch_speed` and `launch_angle`)

Power/value proxies:
- `iso_value_mean`
- `estimated_slg_using_speedangle_mean`
- `woba_value_mean`
- `estimated_woba_using_speedangle_mean`

Barrel:
- `barrel_rate` (computed among pitches with non-null `launch_speed` and `launch_angle`)

## Filtering behavior
Rows may be omitted if sample sizes are too small:
- Pitchers: omitted if `n_pitches_total < --min-pitches-pitcher`
- Batters: omitted if
  - `n_pitches_total < --min-pitches-batter`, or
  - number of pitches with both non-null `launch_speed` and `launch_angle`
    is `< --min-batted-ball-batter`

