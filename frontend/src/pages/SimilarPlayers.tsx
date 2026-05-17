import { useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Card } from "primereact/card";
import { ListBox, type ListBoxChangeEvent } from "primereact/listbox";
import { Tag } from "primereact/tag";
import { Skeleton } from "primereact/skeleton";
import { ProgressSpinner } from "primereact/progressspinner";
import { api } from "@/lib/api";
import type { LeaderboardPlayer, Neighbor, Role } from "@/lib/types";
import { useDebouncedValue } from "@/lib/utils";
import { SearchInput } from "@/components/SearchInput";
import { RoleSelect } from "@/components/RoleSelect";
import { ErrorBanner } from "@/components/ErrorBanner";

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
    <section className="flex flex-col gap-6">
      <Card>
        <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <RoleSelect value={role} onChange={handleRoleChange} />
          <SearchInput
            label="Filter players"
            placeholder="Filter player…"
            value={filter}
            onChange={setFilter}
          />
        </div>
      </Card>

      {leaderboardQuery.error && (
        <ErrorBanner
          title="Failed to load leaderboard."
          message={(leaderboardQuery.error as Error).message}
        />
      )}

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-[minmax(280px,1fr)_minmax(360px,1.4fr)]">
        <LeaderboardPane
          players={filteredPlayers}
          totalCount={leaderboardQuery.data?.players.length ?? 0}
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
  totalCount,
  selectedId,
  onSelect,
  loading,
}: {
  players: LeaderboardPlayer[];
  totalCount: number;
  selectedId: number | null;
  onSelect: (id: number) => void;
  loading: boolean;
}) {
  return (
    <Card>
      <header className="mb-3 flex items-center justify-between">
        <div>
          <h2 className="m-0 text-base font-semibold">Players</h2>
          <p className="m-0 mt-0.5 text-xs text-muted">
            Click a player to view its nearest PCA neighbors.
          </p>
        </div>
        <Tag
          value={`${players.length}${players.length !== totalCount ? ` / ${totalCount}` : ""}`}
          rounded
        />
      </header>

      {loading && (
        <div className="flex flex-col gap-2">
          {Array.from({ length: 8 }).map((_, i) => (
            <Skeleton key={i} height="2rem" borderRadius="6px" />
          ))}
        </div>
      )}

      {!loading && players.length === 0 && (
        <p className="m-0 py-4 text-center text-sm text-muted">No players match.</p>
      )}

      {!loading && players.length > 0 && (
        <ListBox
          value={selectedId}
          options={players}
          optionLabel="player_name"
          optionValue="player_id"
          onChange={(e: ListBoxChangeEvent) => {
            if (e.value != null) onSelect(e.value as number);
          }}
          listStyle={{ maxHeight: "min(60vh, 540px)" }}
          itemTemplate={(p: LeaderboardPlayer) => (
            <span className="truncate text-sm">{p.player_name}</span>
          )}
        />
      )}
    </Card>
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
  if (!selected) {
    return (
      <Card>
        <div className="flex h-full min-h-[18rem] flex-col items-center justify-center gap-3 py-8 text-center">
          <span className="grid h-14 w-14 place-items-center rounded-full bg-accent-soft text-[color:var(--color-accent-hover)]">
            <i className="pi pi-search-plus text-2xl" />
          </span>
          <div>
            <p className="m-0 text-base font-semibold">Pick a player</p>
            <p className="m-0 mt-1 max-w-sm text-sm text-muted">
              Select someone from the list (or filter by name) to surface their nearest
              neighbors in PCA space.
            </p>
          </div>
        </div>
      </Card>
    );
  }

  return (
    <Card>
      {loading && (
        <div className="flex items-center gap-3 py-2 text-sm text-muted">
          <ProgressSpinner style={{ width: 20, height: 20 }} strokeWidth="6" />
          Computing neighbors…
        </div>
      )}
      {error && (
        <p className="m-0 text-sm text-[color:var(--color-danger-line)]">{error.message}</p>
      )}
      {data && (
        <>
          <header className="mb-3 flex items-center justify-between">
            <div>
              <p className="m-0 text-[11px] font-semibold uppercase tracking-wider text-muted">
                Nearest neighbors
              </p>
              <h2 className="m-0 mt-0.5 truncate text-lg font-semibold">
                {data.player_name ?? fallbackName ?? `Player ${data.player_id}`}
              </h2>
            </div>
            <Tag
              value={`${data.neighbors.length} neighbors`}
              rounded
              severity="success"
            />
          </header>

          <ol className="m-0 list-none p-0">
            {data.neighbors.map((n) => (
              <NeighborRow key={n.rank} neighbor={n} />
            ))}
          </ol>
        </>
      )}
    </Card>
  );
}

function NeighborRow({ neighbor }: { neighbor: Neighbor }) {
  return (
    <li className="flex items-center gap-3 border-b border-line py-2 last:border-b-0">
      <span className="grid h-7 w-7 shrink-0 place-items-center rounded-md bg-[color:var(--color-surface-alt)] text-xs font-semibold tabular-nums text-muted">
        {neighbor.rank}
      </span>
      <span className="min-w-0 flex-1 truncate text-sm font-medium">
        {neighbor.player_name}
      </span>
      <span className="shrink-0 rounded-full bg-accent-soft px-2 py-0.5 text-[11px] font-medium tabular-nums text-[color:var(--color-accent-hover)]">
        Δ {neighbor.distance.toFixed(4)}
      </span>
    </li>
  );
}
