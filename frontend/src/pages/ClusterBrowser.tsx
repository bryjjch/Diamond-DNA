import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card } from "primereact/card";
import { Accordion, AccordionTab } from "primereact/accordion";
import { Tag } from "primereact/tag";
import { ProgressSpinner } from "primereact/progressspinner";
import { Skeleton } from "primereact/skeleton";
import { api } from "@/lib/api";
import type { Cluster, ClusterPlayer, Role, SearchHit } from "@/lib/types";
import { useDebouncedValue } from "@/lib/utils";
import { SearchInput } from "@/components/SearchInput";
import { ErrorBanner } from "@/components/ErrorBanner";
import { SoftProfileModal } from "@/components/SoftProfileModal";

const ROLE_META: Record<Role, { title: string; icon: string }> = {
  batter: { title: "Batters", icon: "pi pi-bullseye" },
  pitcher: { title: "Pitchers", icon: "pi pi-send" },
};

const ROLE_TONE: Record<Role, string> = {
  batter: "var(--color-accent)",
  pitcher: "#0ea5e9",
};

function probBadgeStyle(prob: number, tone: string): React.CSSProperties {
  if (prob >= 0.7) return { background: `color-mix(in srgb, ${tone} 14%, transparent)`, color: tone };
  if (prob >= 0.5) return { background: "color-mix(in srgb, #f59e0b 14%, transparent)", color: "#b45309" };
  return { background: "color-mix(in srgb, currentColor 8%, transparent)", color: "var(--p-text-muted-color, #6b7280)" };
}

interface SelectedPlayer {
  id: number;
  role: Role;
}

export function ClusterBrowser() {
  const [query, setQuery] = useState("");
  const debouncedQuery = useDebouncedValue(query, 200);
  const queryNorm = query.trim().toLowerCase();
  const [selectedPlayer, setSelectedPlayer] = useState<SelectedPlayer | null>(null);

  const clustersQuery = useQuery({
    queryKey: ["clusters"],
    queryFn: api.clusters,
  });

  const searchQuery = useQuery({
    queryKey: ["search", debouncedQuery],
    queryFn: () => api.search(debouncedQuery),
    enabled: debouncedQuery.trim().length > 0,
  });

  const grouped = useMemo(() => {
    const out: Record<Role, Cluster[]> = { batter: [], pitcher: [] };
    for (const c of clustersQuery.data?.clusters ?? []) {
      out[c.role]?.push(c);
    }
    for (const role of ["batter", "pitcher"] as Role[]) {
      out[role].sort((a, b) => a.cluster_id - b.cluster_id);
    }
    return out;
  }, [clustersQuery.data]);

  const stats = useMemo(() => {
    const totals = { batterClusters: 0, pitcherClusters: 0, batters: 0, pitchers: 0 };
    for (const c of clustersQuery.data?.clusters ?? []) {
      if (c.role === "batter") {
        totals.batterClusters += 1;
        totals.batters += c.players.length;
      } else {
        totals.pitcherClusters += 1;
        totals.pitchers += c.players.length;
      }
    }
    return totals;
  }, [clustersQuery.data]);

  return (
    <section className="flex flex-col gap-6">
      <StatRow stats={stats} loading={clustersQuery.isLoading} />

      <Card>
        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
          <div>
            <h2 className="m-0 text-base font-semibold">Find a player</h2>
            <p className="m-0 mt-0.5 text-xs text-muted">
              Search across both batter and pitcher clusters by name.
            </p>
          </div>
          <SearchInput
            label="Search players in clusters"
            placeholder="Search player name…"
            value={query}
            onChange={setQuery}
          />
        </div>
      </Card>

      {clustersQuery.error && (
        <ErrorBanner
          title="Failed to load clusters."
          message={(clustersQuery.error as Error).message}
        />
      )}

      {debouncedQuery.trim() && (
        <SearchResultsPanel
          query={debouncedQuery.trim()}
          results={searchQuery.data?.results ?? []}
          isLoading={searchQuery.isLoading}
          onPlayerClick={(id, role) => setSelectedPlayer({ id, role })}
        />
      )}

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
        {(["batter", "pitcher"] as Role[]).map((role) => (
          <RoleColumn
            key={role}
            role={role}
            clusters={grouped[role]}
            queryNorm={queryNorm}
            loading={clustersQuery.isLoading}
            onPlayerClick={(id) => setSelectedPlayer({ id, role })}
          />
        ))}
      </div>

      {selectedPlayer && (
        <SoftProfileModal
          playerId={selectedPlayer.id}
          role={selectedPlayer.role}
          onClose={() => setSelectedPlayer(null)}
        />
      )}
    </section>
  );
}

function StatRow({
  stats,
  loading,
}: {
  stats: {
    batterClusters: number;
    pitcherClusters: number;
    batters: number;
    pitchers: number;
  };
  loading: boolean;
}) {
  const items = [
    {
      label: "Batter clusters",
      value: stats.batterClusters,
      icon: "pi pi-th-large",
      tone: "var(--color-accent)",
    },
    {
      label: "Pitcher clusters",
      value: stats.pitcherClusters,
      icon: "pi pi-th-large",
      tone: "#0ea5e9",
    },
    {
      label: "Batters profiled",
      value: stats.batters,
      icon: "pi pi-bullseye",
      tone: "var(--color-accent)",
    },
    {
      label: "Pitchers profiled",
      value: stats.pitchers,
      icon: "pi pi-send",
      tone: "#0ea5e9",
    },
  ];
  return (
    <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
      {items.map((it) => (
        <Card key={it.label}>
          <div className="flex items-center justify-between gap-3">
            <div className="min-w-0">
              <p className="m-0 truncate text-[11px] font-semibold uppercase tracking-wider text-muted">
                {it.label}
              </p>
              {loading ? (
                <Skeleton width="3rem" height="1.75rem" className="mt-1" />
              ) : (
                <p className="m-0 mt-1 text-2xl font-semibold tabular-nums">
                  {it.value.toLocaleString()}
                </p>
              )}
            </div>
            <span
              className="grid h-10 w-10 shrink-0 place-items-center rounded-lg"
              style={{
                background: `color-mix(in srgb, ${it.tone} 14%, transparent)`,
                color: it.tone,
              }}
            >
              <i className={`${it.icon} text-base`} />
            </span>
          </div>
        </Card>
      ))}
    </div>
  );
}

function SearchResultsPanel({
  query,
  results,
  isLoading,
  onPlayerClick,
}: {
  query: string;
  results: SearchHit[];
  isLoading: boolean;
  onPlayerClick: (id: number, role: Role) => void;
}) {
  return (
    <Card>
      <div className="mb-2 flex items-center justify-between">
        <div>
          <h3 className="m-0 text-sm font-semibold">
            {isLoading ? "Searching…" : "Matches"}
            <span className="ml-2 font-normal text-muted">for "{query}"</span>
          </h3>
        </div>
        {!isLoading && (
          <Tag value={`${results.length} ${results.length === 1 ? "hit" : "hits"}`} rounded />
        )}
      </div>
      {isLoading && (
        <div className="flex items-center gap-3 py-2 text-sm text-muted">
          <ProgressSpinner style={{ width: 18, height: 18 }} strokeWidth="6" />
          Searching across clusters…
        </div>
      )}
      {!isLoading && results.length === 0 && (
        <p className="m-0 text-sm text-muted">No players matched.</p>
      )}
      {!isLoading && results.length > 0 && (
        <ul className="m-0 grid list-none grid-cols-1 gap-1 p-0 sm:grid-cols-2">
          {results.map((r) => {
            const tone = ROLE_TONE[r.role];
            const pct = r.prob_primary !== undefined ? Math.round(r.prob_primary * 100) : null;
            return (
              <li
                key={`${r.role}-${r.player_id}`}
                className="flex cursor-pointer items-center justify-between gap-3 rounded-md border border-line bg-[color:var(--color-surface-alt)] px-3 py-2 text-sm hover:bg-[color:var(--color-surface-hover,var(--color-surface-alt))]"
                onClick={() => onPlayerClick(r.player_id, r.role)}
              >
                <div className="min-w-0">
                  <p className="m-0 truncate font-medium">{r.player_name}</p>
                  <p className="m-0 truncate text-xs text-muted">{r.cluster_label}</p>
                  {r.secondary_cluster_label && pct !== null && pct < 70 && (
                    <p className="m-0 truncate text-xs text-muted">
                      also fits: {r.secondary_cluster_label}
                    </p>
                  )}
                </div>
                <div className="flex shrink-0 items-center gap-1.5">
                  {pct !== null && (
                    <span
                      className="rounded px-1.5 py-0.5 text-[11px] font-semibold tabular-nums"
                      style={probBadgeStyle(r.prob_primary!, tone)}
                    >
                      {pct}%
                    </span>
                  )}
                  <Tag
                    value={r.role}
                    severity={r.role === "batter" ? "success" : "info"}
                    rounded
                  />
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </Card>
  );
}

function RoleColumn({
  role,
  clusters,
  queryNorm,
  loading,
  onPlayerClick,
}: {
  role: Role;
  clusters: Cluster[];
  queryNorm: string;
  loading: boolean;
  onPlayerClick: (id: number) => void;
}) {
  const meta = ROLE_META[role];
  const tone = ROLE_TONE[role];
  const visible = queryNorm
    ? clusters.filter((c) =>
        c.players.some((p) => p.player_name.toLowerCase().includes(queryNorm)),
      )
    : clusters;

  return (
    <Card>
      <header className="mb-3 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="grid h-8 w-8 place-items-center rounded-md bg-accent-soft text-[color:var(--color-accent-hover)]">
            <i className={meta.icon} />
          </span>
          <div>
            <h2 className="m-0 text-base font-semibold">{meta.title}</h2>
            <p className="m-0 text-xs text-muted">{clusters.length} clusters</p>
          </div>
        </div>
        {queryNorm && (
          <Tag value={`${visible.length} match`} severity="success" rounded />
        )}
      </header>

      {loading && (
        <div className="flex flex-col gap-2">
          {[0, 1, 2, 3].map((i) => (
            <Skeleton key={i} height="2.75rem" borderRadius="10px" />
          ))}
        </div>
      )}

      {!loading && visible.length === 0 && (
        <p className="m-0 py-4 text-center text-sm text-muted">
          No clusters match the current search.
        </p>
      )}

      {!loading && visible.length > 0 && (
        <Accordion multiple activeIndex={queryNorm ? visible.map((_, i) => i) : null}>
          {visible.map((cluster) => {
            const filtered = queryNorm
              ? cluster.players.filter((p) =>
                  p.player_name.toLowerCase().includes(queryNorm),
                )
              : cluster.players;

            return (
              <AccordionTab
                key={cluster.cluster_id}
                header={
                  <div className="flex w-full flex-col gap-0.5 pr-2">
                    <div className="flex w-full items-center justify-between gap-2">
                      <span className="truncate font-medium">{cluster.label}</span>
                      <Tag
                        value={filtered.length}
                        severity={queryNorm ? "success" : undefined}
                        rounded
                      />
                    </div>
                    {cluster.description && (
                      <p className="m-0 text-xs font-normal text-muted line-clamp-2">
                        {cluster.description}
                      </p>
                    )}
                  </div>
                }
              >
                <ul className="m-0 grid max-h-72 list-none grid-cols-1 gap-x-4 overflow-y-auto p-0 text-sm sm:grid-cols-2">
                  {filtered.map((p) => (
                    <PlayerRow
                      key={p.player_id}
                      player={p}
                      queryNorm={queryNorm}
                      tone={tone}
                      onClick={() => onPlayerClick(p.player_id)}
                    />
                  ))}
                </ul>
              </AccordionTab>
            );
          })}
        </Accordion>
      )}
    </Card>
  );
}

function PlayerRow({
  player,
  queryNorm,
  tone,
  onClick,
}: {
  player: ClusterPlayer;
  queryNorm: string;
  tone: string;
  onClick: () => void;
}) {
  const hasSoft = player.prob_primary !== undefined;
  const pct = hasSoft ? Math.round(player.prob_primary! * 100) : null;
  const showSecondary =
    hasSoft && player.prob_primary! < 0.7 && player.secondary_label;
  const secPct = player.prob_secondary !== undefined
    ? Math.round(player.prob_secondary * 100)
    : null;

  return (
    <li
      className="cursor-pointer border-b border-line py-1.5 last:border-b-0 hover:bg-[color:var(--color-surface-alt)]"
      onClick={onClick}
    >
      <div className="flex items-center justify-between gap-2">
        <span className="min-w-0 truncate">{highlight(player.player_name, queryNorm)}</span>
        {pct !== null && (
          <span
            className="shrink-0 rounded px-1.5 py-0.5 text-[11px] font-semibold tabular-nums"
            style={probBadgeStyle(player.prob_primary!, tone)}
          >
            {pct}%
          </span>
        )}
      </div>
      {showSecondary && (
        <p className="m-0 mt-0.5 truncate text-[11px] text-muted">
          + {secPct}% {player.secondary_label}
        </p>
      )}
    </li>
  );
}

function highlight(text: string, queryNorm: string) {
  if (!queryNorm) return text;
  const idx = text.toLowerCase().indexOf(queryNorm);
  if (idx === -1) return text;
  return (
    <>
      {text.slice(0, idx)}
      <mark className="rounded bg-accent-soft px-0.5 text-[color:var(--color-accent-hover)]">
        {text.slice(idx, idx + queryNorm.length)}
      </mark>
      {text.slice(idx + queryNorm.length)}
    </>
  );
}
