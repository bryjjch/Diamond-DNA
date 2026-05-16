// Shapes returned by the Diamond-DNA HTTP API (src/api/handler.py).
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
}

export interface Cluster {
  role: Role;
  cluster_id: number;
  label: string;
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
