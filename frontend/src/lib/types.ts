// Shapes returned by the xWAR Engine HTTP API (src/api/handler.py).
// Keep aligned with the Python response bodies — any drift here breaks the UI.

export type Role = "batter" | "pitcher";

/** Catchers cluster in their own cohort but project as batters. */
export type ArchetypeRole = Role | "catcher";

export type Model = "xgboost" | "marcel";

/** Stat keys vary by role; individual values may be null when missing upstream. */
export type StatMap = Record<string, number | null>;

export interface MetaResponse {
  ok: true;
  default_year: number;
  roles: Role[];
  archetype_roles: ArchetypeRole[];
  models: Model[];
  source: string;
  cached_artifacts: string[];
}

export interface ProjectionPlayer {
  player_id: number;
  player_name: string | null;
  age: number | null;
  primary_position: string | null;
  projections: StatMap | null;
  actuals: StatMap | null;
}

export interface ProjectionsResponse {
  ok: true;
  role: Role;
  year: number;
  model: Model;
  sort: string;
  order: "asc" | "desc";
  total: number;
  count: number;
  players: ProjectionPlayer[];
}

export interface SoftProb {
  cluster_id: number;
  label: string;
  prob: number;
}

export interface Neighbor {
  rank: number;
  player_id: number;
  player_name: string;
  distance: number;
}

export interface ArchetypeBlock {
  archetype_role: ArchetypeRole;
  cluster_id: number;
  label: string;
  description: string;
  probs: SoftProb[];
}

export interface SeasonLag {
  season: number;
  lag: number;
  available: boolean;
  stats: StatMap;
}

export interface StatcastBlock {
  available: boolean;
  [skill: string]: number | boolean | null;
}

export interface PlayerHistory {
  seasons: SeasonLag[];
  statcast_last_season: StatcastBlock | null;
}

export interface PlayerProfile {
  role: Role;
  age: number | null;
  primary_position: string | null;
  projection: { stats: StatMap | null; actuals: StatMap | null } | null;
  history: PlayerHistory | null;
  archetype: ArchetypeBlock | null;
  neighbors: Neighbor[] | null;
}

export interface PlayerResponse {
  ok: true;
  player_id: number;
  player_name: string | null;
  year: number;
  model: Model;
  profiles: PlayerProfile[];
}

export interface ModelErrors {
  mae: number | null;
  rmse: number | null;
  bias: number | null;
}

export interface AccuracyDetailRow {
  role: Role;
  year: number;
  stat: string;
  n: number;
  xgboost: ModelErrors;
  marcel: ModelErrors;
  mae_pct_improvement: number | null;
  rmse_pct_improvement: number | null;
  candidate_wins_mae: boolean;
}

export interface AccuracySummaryRow {
  role: Role;
  stat: string;
  n_years: number;
  mean_mae_pct_improvement: number | null;
  mean_rmse_pct_improvement: number | null;
  win_rate_mae: number | null;
}

export interface AccuracyResponse {
  ok: true;
  role: "all" | Role;
  baseline_model: "marcel";
  candidate_model: "xgboost";
  start_year: number;
  end_year: number;
  years_compared: number[];
  detail: AccuracyDetailRow[];
  summary: AccuracySummaryRow[];
}

export interface SearchHit {
  player_id: number;
  player_name: string;
  role: ArchetypeRole;
  year: number | null;
  // Cluster fields are present only when archetype data exists for the hit.
  cluster_id?: number;
  cluster_label?: string;
  prob_primary?: number | null;
  prob_secondary?: number | null;
  secondary_cluster_label?: string;
}

export interface SearchResponse {
  ok: true;
  q: string;
  results: SearchHit[];
}

export interface HealthResponse {
  ok: true;
  service: string;
}
