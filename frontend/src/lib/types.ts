// Shapes returned by the xWAR Engine HTTP API (src/api/handler.py).
// Keep aligned with the Python response bodies — any drift here breaks the UI.

export type Role = "batter" | "pitcher";

export interface MetaResponse {
  ok: true;
  year: number;
  source: string;
  notes: string;
  n_archetype_rows: number;
  n_neighbor_rows: number;
}

export interface ClusterPlayer {
  player_id: number;
  player_name: string;
  prob_primary?: number;
  cluster_id_secondary?: number;
  prob_secondary?: number;
  secondary_label?: string;
}

export interface Cluster {
  role: Role;
  cluster_id: number;
  label: string;
  description?: string;
  players: ClusterPlayer[];
}

export interface ClustersResponse {
  ok: true;
  clusters: Cluster[];
}

export interface SearchHit {
  player_id: number;
  player_name: string;
  role: Role;
  year: number | null;
  cluster_id: number;
  cluster_label: string;
  prob_primary?: number;
  prob_secondary?: number;
  secondary_cluster_label?: string;
}

export interface SoftProb {
  cluster_id: number;
  label: string;
  prob: number;
}

export interface SoftProfile {
  player_id: number;
  player_name: string;
  role: Role;
  cluster_id: number;
  cluster_label: string;
  probs: SoftProb[];
}

export interface SoftProfileResponse {
  ok: true;
  player_id: number;
  player_name: string;
  role: Role;
  cluster_id: number;
  cluster_label: string;
  probs: SoftProb[];
}

export interface SearchResponse {
  ok: true;
  q: string;
  results: SearchHit[];
}

export interface LeaderboardPlayer {
  player_id: number;
  player_name: string;
}

export interface LeaderboardResponse {
  ok: true;
  role: Role;
  players: LeaderboardPlayer[];
}

export interface Neighbor {
  rank: number;
  player_id: number;
  player_name: string;
  distance: number;
}

export interface NeighborsResponse {
  ok: true;
  player_id: number;
  player_name: string | null;
  role: Role;
  neighbors: Neighbor[];
}

export interface ApiError {
  ok: false;
  error: string;
}
