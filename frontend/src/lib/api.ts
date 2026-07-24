import type {
  AccuracyResponse,
  HealthResponse,
  MetaResponse,
  Model,
  PlayerResponse,
  ProjectionsResponse,
  Role,
  SearchResponse,
} from "./types";

const BASE_URL = (import.meta.env.VITE_API_BASE_URL ?? "").replace(/\/$/, "");

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function request<T>(
  path: string,
  params?: Record<string, string | number | undefined>,
): Promise<T> {
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
  projections: (p: {
    role: Role;
    year?: number;
    model?: Model;
    sort?: string;
    order?: "asc" | "desc";
    limit?: number;
  }) => request<ProjectionsResponse>("/api/projections", p),
  player: (p: { player_id: number; year?: number; model?: Model; role?: Role }) =>
    request<PlayerResponse>("/api/player", p),
  accuracy: (p?: { role?: "all" | Role; start_year?: number; end_year?: number }) =>
    request<AccuracyResponse>("/api/accuracy", p),
  search: (q: string, year?: number) => request<SearchResponse>("/api/search", { q, year }),
  health: () => request<HealthResponse>("/api/health"),
};
