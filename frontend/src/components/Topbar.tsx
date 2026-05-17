import { useLocation } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Tag } from "primereact/tag";
import { Skeleton } from "primereact/skeleton";
import { api } from "@/lib/api";

const TITLES: Record<string, { title: string; subtitle: string }> = {
  "/": {
    title: "Archetype Clusters",
    subtitle: "Player groupings discovered by K-Means over standardized Statcast features.",
  },
  "/similar": {
    title: "Similar Players",
    subtitle: "Nearest neighbors in PCA space — pick a player to see their closest matches.",
  },
};

export function Topbar() {
  const { pathname } = useLocation();
  const page = TITLES[pathname] ?? TITLES["/"];

  const { data, isLoading, error } = useQuery({
    queryKey: ["meta"],
    queryFn: api.meta,
  });

  return (
    <header className="sticky top-0 z-10 border-b border-line bg-surface/85 backdrop-blur">
      <div className="flex items-center justify-between gap-4 px-6 py-4">
        <div className="min-w-0">
          <h1 className="m-0 truncate text-lg font-semibold tracking-tight text-fg">
            {page.title}
          </h1>
          <p className="m-0 mt-0.5 truncate text-xs text-muted">{page.subtitle}</p>
        </div>

        <div className="flex shrink-0 items-center gap-2">
          {isLoading && <Skeleton width="6rem" height="1.5rem" borderRadius="6px" />}
          {data && (
            <>
              <Tag
                value={`Season ${data.year}`}
                severity="success"
                rounded
                icon="pi pi-calendar"
              />
              <Tag
                value={`${data.n_archetype_rows.toLocaleString()} players`}
                rounded
                icon="pi pi-database"
                style={{
                  background: "var(--color-surface-alt)",
                  color: "var(--color-muted)",
                  border: "1px solid var(--color-line)",
                }}
              />
            </>
          )}
          {error && (
            <Tag
              value="meta unavailable"
              severity="danger"
              rounded
              icon="pi pi-exclamation-triangle"
            />
          )}
        </div>
      </div>
    </header>
  );
}
