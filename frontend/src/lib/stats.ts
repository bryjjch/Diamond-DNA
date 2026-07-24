// Presentation metadata for the stat columns served by the API.
// Column sets mirror ROLE_ALL_TARGETS / ROLE_LAG_STAT_COLUMNS on the Python side;
// lowerIsBetter mirrors ROLE_LOWER_IS_BETTER (src/ml/baselines/marcel_baseline.py).

import type { Role } from "./types";

export interface StatDef {
  key: string;
  label: string;
  format: (v: number) => string;
  lowerIsBetter?: boolean;
}

const int = (v: number) => Math.round(v).toLocaleString();
const fixed = (digits: number) => (v: number) => v.toFixed(digits);
// Baseball convention for rate stats: .312 rather than 0.312 (1.024 stays intact).
const rate3 = (v: number) => {
  const s = v.toFixed(3);
  return s.startsWith("0.") ? s.slice(1) : s;
};
const pct1 = (v: number) => `${(v * 100).toFixed(1)}%`;

export const BATTER_STATS: StatDef[] = [
  { key: "pa", label: "PA", format: int },
  { key: "avg", label: "AVG", format: rate3 },
  { key: "obp", label: "OBP", format: rate3 },
  { key: "slg", label: "SLG", format: rate3 },
  { key: "ops", label: "OPS", format: rate3 },
  { key: "bb_pct", label: "BB%", format: pct1 },
  { key: "k_pct", label: "K%", format: pct1, lowerIsBetter: true },
  { key: "hr", label: "HR", format: int },
  { key: "sb", label: "SB", format: int },
];

export const PITCHER_STATS: StatDef[] = [
  { key: "innings", label: "IP", format: fixed(1) },
  { key: "era", label: "ERA", format: fixed(2), lowerIsBetter: true },
  { key: "whip", label: "WHIP", format: fixed(2), lowerIsBetter: true },
  { key: "k_per_9", label: "K/9", format: fixed(2) },
  { key: "bb_per_9", label: "BB/9", format: fixed(2), lowerIsBetter: true },
  { key: "hr_per_9", label: "HR/9", format: fixed(2), lowerIsBetter: true },
];

export const ROLE_STATS: Record<Role, StatDef[]> = {
  batter: BATTER_STATS,
  pitcher: PITCHER_STATS,
};

export const DEFAULT_SORT: Record<Role, string> = { batter: "ops", pitcher: "era" };

/** Playing-time stat used to qualify a player for the actuals leaderboard. */
export const PLAYING_TIME: Record<Role, StatDef> = {
  batter: BATTER_STATS[0],
  pitcher: PITCHER_STATS[0],
};

// Season-history rows carry a few extra counting stats beyond the projected set.
export const HISTORY_STATS: Record<Role, StatDef[]> = {
  batter: [{ key: "g", label: "G", format: int }, ...BATTER_STATS],
  pitcher: [
    { key: "g", label: "G", format: int },
    { key: "gs", label: "GS", format: int },
    ...PITCHER_STATS,
    { key: "sv", label: "SV", format: int },
  ],
};

export function statDef(role: Role, key: string): StatDef | undefined {
  return HISTORY_STATS[role].find((s) => s.key === key);
}

export function formatStat(role: Role, key: string, value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  const def = statDef(role, key);
  return def ? def.format(value) : fmtNumber(value);
}

/** Label for a stat key without role context (accuracy rows mix both roles). */
export function statLabel(key: string): string {
  for (const defs of [HISTORY_STATS.batter, HISTORY_STATS.pitcher]) {
    const def = defs.find((s) => s.key === key);
    if (def) return def.label;
  }
  return key.toUpperCase();
}

export function fmtNumber(v: number | null | undefined, digits = 3): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  if (Number.isInteger(v)) return v.toLocaleString();
  // Trim trailing zeros: 88.4 stays 88.4, not 88.400.
  return Number(v.toFixed(digits)).toString();
}
