import { useLocation } from "react-router-dom";
import { Tag } from "primereact/tag";
import { Skeleton } from "primereact/skeleton";
import { useMeta } from "@/lib/useMeta";
import { useSeasonYear } from "@/lib/season";

function matchTitle(pathname: string): { title: string; subtitle: string } {
  if (pathname.startsWith("/player/")) {
    return {
      title: "Player Detail",
      subtitle: "Projection, season history, archetype fit, and comparables.",
    };
  }
  if (pathname.startsWith("/accuracy")) {
    return {
      title: "Model Accuracy",
      subtitle: "XGBoost projections backtested against the Marcel baseline.",
    };
  }
  return {
    title: "Projections",
    subtitle: "Projected and actual leaderboards from the XGBoost and Marcel models.",
  };
}

export function Topbar() {
  const { pathname } = useLocation();
  const page = matchTitle(pathname);

  const { data, isLoading, error } = useMeta();
  const season = useSeasonYear();

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
          {(isLoading || season.isLoading) && (
            <Skeleton width="6rem" height="1.5rem" borderRadius="6px" />
          )}
          {season.year !== undefined && (
            <Tag
              value={`Season ${season.year}`}
              severity="success"
              rounded
              icon="pi pi-calendar"
            />
          )}
          {data && (
            <Tag
              value={data.models.join(" · ")}
              rounded
              icon="pi pi-sliders-h"
              style={{
                background: "var(--color-surface-alt)",
                color: "var(--color-muted)",
                border: "1px solid var(--color-line)",
              }}
            />
          )}
          {error != null && (
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
