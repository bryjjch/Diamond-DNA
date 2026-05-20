import type {
  ClustersResponse,
  LeaderboardResponse,
  MetaResponse,
  NeighborsResponse,
  Role,
  SearchResponse,
  SoftProfileResponse,
} from "./types";

const BASE_URL = (import.meta.env.VITE_API_BASE_URL ?? "").replace(/\/$/, "");

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function request<T>(path: string, params?: Record<string, string | number>): Promise<T> {
  const qs = params
    ? "?" +
      Object.entries(params)
        .filter(([, v]) => v !== undefined && v !== null && v !== "")
        .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
        .join("&")
    : "";
  const url = `${BASE_URL}${path}${qs}`;
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  let body: unknown;
  try {
    body = await res.json();
  } catch {
    throw new ApiError(res.status, `Invalid JSON from ${path}`);
  }
  if (!res.ok || (body as { ok?: boolean }).ok === false) {
    const msg = (body as { error?: string }).error ?? `Request failed (${res.status})`;
    throw new ApiError(res.status, msg);
  }
  return body as T;
}

export const api = {
  meta: () => request<MetaResponse>("/api/meta"),
  clusters: () => request<ClustersResponse>("/api/clusters"),
  search: (q: string) => request<SearchResponse>("/api/search", { q }),
  leaderboard: (role: Role, limit = 800) =>
    request<LeaderboardResponse>("/api/leaderboard", { role, limit }),
  neighbors: (role: Role, playerId: number) =>
    request<NeighborsResponse>("/api/neighbors", { role, player_id: playerId }),
  playerSoftProfile: (role: Role, playerId: number) =>
    request<SoftProfileResponse>("/api/player_soft_profile", { role, player_id: playerId }),
};
