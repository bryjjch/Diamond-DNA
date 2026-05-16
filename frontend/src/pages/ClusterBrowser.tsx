import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import type { Cluster, Role, SearchHit } from "@/lib/types";
import { useDebouncedValue } from "@/lib/utils";
import { SearchInput } from "@/components/SearchInput";
import { ErrorBanner } from "@/components/ErrorBanner";

const ROLE_TITLES: Record<Role, string> = { batter: "Batters", pitcher: "Pitchers" };

export function ClusterBrowser() {
  const [query, setQuery] = useState("");
  const debouncedQuery = useDebouncedValue(query, 200);
  const queryNorm = query.trim().toLowerCase();

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

  return (
    <section>
      <div className="mb-4">
        <SearchInput
          label="Search players in clusters"
          placeholder="Search player name…"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
      </div>

      {clustersQuery.error && (
        <ErrorBanner
          title="Failed to load clusters."
          message={(clustersQuery.error as Error).message}
        />
      )}

      {debouncedQuery.trim() && (
        <SearchResultsPanel
          results={searchQuery.data?.results ?? []}
          isLoading={searchQuery.isLoading}
        />
      )}

      <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
        {(["batter", "pitcher"] as Role[]).map((role) => (
          <RoleColumn
            key={role}
            role={role}
            clusters={grouped[role]}
            queryNorm={queryNorm}
            loading={clustersQuery.isLoading}
          />
        ))}
      </div>
    </section>
  );
}

function SearchResultsPanel({
  results,
  isLoading,
}: {
  results: SearchHit[];
  isLoading: boolean;
}) {
  if (!isLoading && results.length === 0) return null;
  return (
    <div className="mb-4 rounded-lg border border-line bg-surface px-4 py-3">
      <h3 className="m-0 text-xs uppercase tracking-wider text-muted">
        {isLoading ? "Searching…" : "Matches"}
      </h3>
      <ul className="m-0 mt-1 list-disc pl-5">
        {results.map((r) => (
          <li key={`${r.role}-${r.player_id}`}>
            <strong>{r.player_name}</strong> — {r.cluster_label}{" "}
            <span className="text-sm text-muted">({r.role})</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function RoleColumn({
  role,
  clusters,
  queryNorm,
  loading,
}: {
  role: Role;
  clusters: Cluster[];
  queryNorm: string;
  loading: boolean;
}) {
  return (
    <div>
      <h2 className="mb-3 text-xs font-semibold uppercase tracking-wider text-muted">
        {ROLE_TITLES[role]}
      </h2>
      {loading && <p className="text-sm text-muted">Loading…</p>}
      {clusters.map((cluster) => (
        <ClusterCard key={cluster.cluster_id} cluster={cluster} queryNorm={queryNorm} />
      ))}
    </div>
  );
}

function ClusterCard({ cluster, queryNorm }: { cluster: Cluster; queryNorm: string }) {
  const filtered = queryNorm
    ? cluster.players.filter((p) => p.player_name.toLowerCase().includes(queryNorm))
    : cluster.players;

  if (queryNorm && filtered.length === 0) return null;

  return (
    <details className="mb-2 overflow-hidden rounded-lg border border-line bg-surface" open={!!queryNorm}>
      <summary className="cursor-pointer list-none px-3 py-2 font-medium [&::-webkit-details-marker]:hidden">
        {cluster.label}{" "}
        <span className="text-sm text-muted">({filtered.length})</span>
      </summary>
      <ul className="m-0 max-h-72 overflow-y-auto py-1 pl-6 pr-3 text-sm">
        {filtered.map((p) => (
          <li key={p.player_id} className="py-0.5">
            {p.player_name}
          </li>
        ))}
      </ul>
    </details>
  );
}
