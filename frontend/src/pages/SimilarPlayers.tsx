import { useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import type { LeaderboardPlayer, Role } from "@/lib/types";
import { useDebouncedValue } from "@/lib/utils";
import { SearchInput } from "@/components/SearchInput";
import { RoleSelect } from "@/components/RoleSelect";
import { ErrorBanner } from "@/components/ErrorBanner";
import { cn } from "@/lib/utils";

export function SimilarPlayers() {
  const [params, setParams] = useSearchParams();
  const role = ((params.get("role") as Role) || "batter") as Role;
  const selectedPlayerId = params.get("player_id")
    ? Number(params.get("player_id"))
    : null;

  const [filter, setFilter] = useState("");
  const debouncedFilter = useDebouncedValue(filter, 150).trim().toLowerCase();

  const leaderboardQuery = useQuery({
    queryKey: ["leaderboard", role],
    queryFn: () => api.leaderboard(role),
  });

  const neighborsQuery = useQuery({
    queryKey: ["neighbors", role, selectedPlayerId],
    queryFn: () => api.neighbors(role, selectedPlayerId!),
    enabled: selectedPlayerId !== null,
  });

  const filteredPlayers = useMemo(() => {
    const players = leaderboardQuery.data?.players ?? [];
    if (!debouncedFilter) return players;
    return players.filter((p) => p.player_name.toLowerCase().includes(debouncedFilter));
  }, [leaderboardQuery.data, debouncedFilter]);

  const selectedPlayer = useMemo<LeaderboardPlayer | null>(() => {
    if (selectedPlayerId === null) return null;
    return (
      leaderboardQuery.data?.players.find((p) => p.player_id === selectedPlayerId) ?? null
    );
  }, [leaderboardQuery.data, selectedPlayerId]);

  function handleRoleChange(next: Role) {
    const newParams = new URLSearchParams(params);
    newParams.set("role", next);
    newParams.delete("player_id");
    setParams(newParams, { replace: true });
  }

  function handleSelectPlayer(playerId: number) {
    const newParams = new URLSearchParams(params);
    newParams.set("role", role);
    newParams.set("player_id", String(playerId));
    setParams(newParams);
  }

  return (
    <section>
      <div className="mb-4 flex flex-wrap items-center gap-3">
        <RoleSelect value={role} onChange={handleRoleChange} />
        <SearchInput
          label="Filter players"
          placeholder="Filter player…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
        />
      </div>

      {leaderboardQuery.error && (
        <ErrorBanner
          title="Failed to load leaderboard."
          message={(leaderboardQuery.error as Error).message}
        />
      )}

      <div className="grid grid-cols-1 gap-6 md:grid-cols-[minmax(200px,1fr)_minmax(280px,1.2fr)]">
        <LeaderboardPane
          players={filteredPlayers}
          selectedId={selectedPlayerId}
          onSelect={handleSelectPlayer}
          loading={leaderboardQuery.isLoading}
        />
        <NeighborsPane
          loading={neighborsQuery.isLoading}
          error={neighborsQuery.error as Error | null}
          data={neighborsQuery.data}
          fallbackName={selectedPlayer?.player_name}
          selected={selectedPlayerId !== null}
        />
      </div>
    </section>
  );
}

function LeaderboardPane({
  players,
  selectedId,
  onSelect,
  loading,
}: {
  players: LeaderboardPlayer[];
  selectedId: number | null;
  onSelect: (id: number) => void;
  loading: boolean;
}) {
  return (
    <div>
      <h2 className="m-0 text-base font-semibold">Players</h2>
      <p className="m-0 mb-3 text-xs text-muted">
        Click a player for nearest neighbors in PCA space.
      </p>
      <ul className="m-0 max-h-[min(60vh,520px)] list-none overflow-y-auto rounded-lg border border-line bg-surface p-0">
        {loading && <li className="px-3 py-2 text-sm text-muted">Loading…</li>}
        {!loading && players.length === 0 && (
          <li className="px-3 py-2 text-sm text-muted">No players.</li>
        )}
        {players.map((p) => (
          <li key={p.player_id} className="border-b border-line last:border-b-0">
            <button
              type="button"
              onClick={() => onSelect(p.player_id)}
              className={cn(
                "block w-full cursor-pointer border-none bg-transparent px-3 py-2 text-left text-sm",
                selectedId === p.player_id
                  ? "bg-[rgba(91,159,212,0.18)] text-fg"
                  : "text-accent hover:bg-[rgba(91,159,212,0.12)] hover:text-fg",
              )}
            >
              {p.player_name}
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}

function NeighborsPane({
  loading,
  error,
  data,
  fallbackName,
  selected,
}: {
  loading: boolean;
  error: Error | null;
  data: Awaited<ReturnType<typeof api.neighbors>> | undefined;
  fallbackName?: string;
  selected: boolean;
}) {
  return (
    <div>
      <h2 className="m-0 text-base font-semibold">Nearest neighbors</h2>
      {!selected && (
        <p className="mt-1 text-sm text-muted">
          Select a player from the list or search.
        </p>
      )}
      {selected && loading && <p className="mt-1 text-sm text-muted">Loading…</p>}
      {selected && error && (
        <p className="mt-1 text-sm text-[color:var(--color-danger-line)]">
          {error.message}
        </p>
      )}
      {selected && data && (
        <>
          <h3 className="mb-2 mt-2 text-base font-semibold">
            {data.player_name ?? fallbackName ?? `Player ${data.player_id}`} — nearest
            in PCA space
          </h3>
          <ol className="m-0 list-none p-0">
            {data.neighbors.map((n) => (
              <li
                key={n.rank}
                className="border-b border-line py-1.5 last:border-b-0"
              >
                <span className="mr-2 tabular-nums text-muted">#{n.rank}</span>
                {n.player_name}{" "}
                <span className="ml-1 text-xs text-muted">
                  Δ {n.distance.toFixed(4)}
                </span>
              </li>
            ))}
          </ol>
        </>
      )}
    </div>
  );
}
