import { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Tag } from "primereact/tag";
import { ProgressSpinner } from "primereact/progressspinner";
import { api } from "@/lib/api";
import { useSeasonYear } from "@/lib/season";
import { matchesAllTerms, nameTokens, probeTerm } from "@/lib/search";
import { ROLE_TAG_SEVERITY } from "@/lib/theme";
import { cn, useDebouncedValue } from "@/lib/utils";
import { SearchInput } from "@/components/SearchInput";

const MAX_HITS = 10;

/** Search box with a results dropdown; picking a hit navigates to the player page. */
export function PlayerSearch({ className }: { className?: string }) {
  const navigate = useNavigate();
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const wrapperRef = useRef<HTMLDivElement>(null);

  const debounced = useDebouncedValue(query, 200).trim();

  const season = useSeasonYear();

  // Names come back as "Last, First", so a query typed in natural order can't
  // be matched as one substring. Fetch on the most selective term, then require
  // the rest to land somewhere in the name.
  const terms = useMemo(() => nameTokens(debounced), [debounced]);
  const probe = probeTerm(terms);

  const searchQuery = useQuery({
    queryKey: ["search", probe, season.year],
    queryFn: () => api.search(probe, season.year),
    enabled: probe.length > 0 && season.year !== undefined,
  });

  useEffect(() => {
    function onMouseDown(e: MouseEvent) {
      if (!wrapperRef.current?.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", onMouseDown);
    return () => document.removeEventListener("mousedown", onMouseDown);
  }, []);

  // Dedupe players that hit on multiple roles (e.g. two-way, catcher cohort).
  const hits = (() => {
    const seen = new Set<number>();
    const out = [];
    for (const r of searchQuery.data?.results ?? []) {
      if (!matchesAllTerms(r.player_name, terms)) continue;
      if (seen.has(r.player_id)) continue;
      seen.add(r.player_id);
      out.push(r);
      if (out.length >= MAX_HITS) break;
    }
    return out;
  })();

  function goTo(playerId: number) {
    setQuery("");
    setOpen(false);
    navigate(`/player/${playerId}`);
  }

  return (
    <div
      ref={wrapperRef}
      className={cn("relative w-full md:w-72", className)}
      onFocus={() => setOpen(true)}
      onKeyDown={(e) => {
        if (e.key === "Escape") setOpen(false);
      }}
    >
      <SearchInput
        label="Search players"
        placeholder="Search player name…"
        value={query}
        onChange={(next) => {
          setQuery(next);
          setOpen(true);
        }}
        className="md:w-72"
      />

      {open && probe.length > 0 && (
        <div className="absolute left-0 right-0 top-full z-20 mt-1 overflow-hidden rounded-lg border border-line bg-surface shadow-[var(--shadow-card-hover)]">
          {searchQuery.isLoading && (
            <div className="flex items-center gap-2 px-3 py-2.5 text-sm text-muted">
              <ProgressSpinner style={{ width: 16, height: 16 }} strokeWidth="6" />
              Searching…
            </div>
          )}
          {!searchQuery.isLoading && searchQuery.error != null && (
            <p className="m-0 px-3 py-2.5 text-sm text-muted">Search unavailable.</p>
          )}
          {!searchQuery.isLoading && !searchQuery.error && hits.length === 0 && (
            <p className="m-0 px-3 py-2.5 text-sm text-muted">No players matched.</p>
          )}
          {hits.length > 0 && (
            <ul className="m-0 max-h-80 list-none overflow-y-auto p-1">
              {hits.map((r) => (
                <li key={`${r.role}-${r.player_id}`}>
                  <button
                    type="button"
                    className="flex w-full cursor-pointer items-center justify-between gap-2 rounded-md border-0 bg-transparent px-2.5 py-2 text-left text-sm hover:bg-[color:var(--color-surface-alt)]"
                    onClick={() => goTo(r.player_id)}
                  >
                    <span className="min-w-0">
                      <span className="block truncate font-medium">{r.player_name}</span>
                      {r.cluster_label && (
                        <span className="block truncate text-xs text-muted">
                          {r.cluster_label}
                        </span>
                      )}
                    </span>
                    <Tag value={r.role} severity={ROLE_TAG_SEVERITY[r.role]} rounded />
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}
