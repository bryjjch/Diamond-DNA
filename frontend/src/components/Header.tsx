import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";

export function Header() {
  const { data, isLoading, error } = useQuery({
    queryKey: ["meta"],
    queryFn: api.meta,
  });

  return (
    <header className="border-b border-line bg-surface px-6 py-5">
      <div className="mx-auto max-w-6xl">
        <h1 className="m-0 text-xl font-semibold tracking-tight">Diamond DNA</h1>
        <p className="mt-1 text-sm text-muted">
          Archetype clusters and PCA-space KNN neighbors
          {data ? ` (${data.year})` : isLoading ? " — loading…" : ""}
        </p>
        {data?.source && (
          <p className="mt-1 break-all text-xs text-muted">{data.source}</p>
        )}
        {error && (
          <p className="mt-1 text-xs text-[color:var(--color-danger-line)]">
            Could not load metadata: {(error as Error).message}
          </p>
        )}
      </div>
    </header>
  );
}
