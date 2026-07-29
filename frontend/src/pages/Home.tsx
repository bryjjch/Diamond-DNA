import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Card } from "primereact/card";
import { Tag } from "primereact/tag";
import { Skeleton } from "primereact/skeleton";
import { api } from "@/lib/api";
import { useMeta } from "@/lib/useMeta";
import { useSeasonYear } from "@/lib/season";
import { PLAYING_TIME } from "@/lib/stats";
import { ROLE_TONE } from "@/lib/theme";
import type { Model, ProjectionPlayer, Role } from "@/lib/types";
import { RoleSelect } from "@/components/RoleSelect";
import { ModelSelect } from "@/components/ModelSelect";
import { PlayerSearch } from "@/components/PlayerSearch";
import { LeaderboardTable } from "@/components/LeaderboardTable";
import { ErrorBanner } from "@/components/ErrorBanner";

const MAX_ROWS = 1000; // API limit cap — fetch once, sort client-side

// Actuals for an in-progress season include players with a handful of PA, who
// would otherwise own every rate-stat leaderboard. Qualify on playing time
// relative to the season's leader so the bar scales as the season fills in.
const QUALIFY_SHARE = 0.4;

export function Home() {
  const [params, setParams] = useSearchParams();
  const role = ((params.get("role") as Role) || "batter") as Role;
  const model = ((params.get("model") as Model) || "xgboost") as Model;

  const meta = useMeta();
  const season = useSeasonYear();
  const year = season.year;

  // One payload per season carries both blocks: `projections` for the model's
  // forecast and `actuals` for what has happened so far in that same season.
  const seasonQuery = useQuery({
    queryKey: ["projections", role, model, year],
    queryFn: () => api.projections({ role, model, year, limit: MAX_ROWS }),
    enabled: year !== undefined,
  });

  const projectedPlayers = useMemo(
    () => (seasonQuery.data?.players ?? []).filter((p) => p.projections !== null),
    [seasonQuery.data],
  );

  const playingTime = PLAYING_TIME[role];
  const { actualPlayers, qualifier } = useMemo(() => {
    const withActuals = (seasonQuery.data?.players ?? []).filter(
      (p) => p.actuals !== null && (p.actuals[playingTime.key] ?? 0) > 0,
    );
    const leader = Math.max(0, ...withActuals.map((p) => p.actuals?.[playingTime.key] ?? 0));
    const cutoff = leader * QUALIFY_SHARE;
    return {
      actualPlayers: withActuals.filter((p) => (p.actuals?.[playingTime.key] ?? 0) >= cutoff),
      qualifier: cutoff,
    };
  }, [seasonQuery.data, playingTime]);

  function setParam(key: string, value: string) {
    const next = new URLSearchParams(params);
    next.set(key, value);
    setParams(next, { replace: true });
  }

  return (
    <section className="flex flex-col gap-6">
      <StatTiles
        loading={season.isLoading || seasonQuery.isLoading}
        year={year}
        playersProjected={seasonQuery.data?.total}
        models={meta.data?.models}
      />

      <Card>
        <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div className="flex flex-wrap items-end gap-6">
            <RoleSelect value={role} onChange={(next) => setParam("role", next)} />
            <ModelSelect value={model} onChange={(next) => setParam("model", next)} />
          </div>
          <div className="flex flex-col gap-1">
            <span className="text-[11px] font-semibold uppercase tracking-wider text-muted">
              Find a player
            </span>
            <PlayerSearch />
          </div>
        </div>
      </Card>

      {meta.error != null && (
        <ErrorBanner
          title="Failed to load metadata."
          message={(meta.error as Error).message}
        />
      )}

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
        <LeaderboardCard
          title={year !== undefined ? `Projected ${year}` : "Projected"}
          subtitle={`Top projected performers (${model})`}
          icon="pi pi-chart-line"
          tone={ROLE_TONE[role]}
          loading={season.isLoading || seasonQuery.isLoading}
          error={seasonQuery.error as Error | null}
          players={projectedPlayers}
          role={role}
          model={model}
          block="projections"
        />
        <LeaderboardCard
          title={year !== undefined ? `${year} Actuals` : "Actuals"}
          subtitle={
            qualifier > 0
              ? `Season to date — min ${playingTime.format(qualifier)} ${playingTime.label}`
              : "Season to date"
          }
          icon="pi pi-history"
          tone={ROLE_TONE[role]}
          loading={season.isLoading || seasonQuery.isLoading}
          error={seasonQuery.error as Error | null}
          players={actualPlayers}
          role={role}
          model={model}
          block="actuals"
        />
      </div>
    </section>
  );
}

function StatTiles({
  loading,
  year,
  playersProjected,
  models,
}: {
  loading: boolean;
  year?: number;
  playersProjected?: number;
  models?: string[];
}) {
  const items = [
    {
      label: "Projection season",
      value: year !== undefined ? String(year) : "—",
      icon: "pi pi-calendar",
      tone: "var(--color-accent)",
    },
    {
      label: "Players projected",
      value: playersProjected !== undefined ? playersProjected.toLocaleString() : "—",
      icon: "pi pi-users",
      tone: "var(--color-sky)",
    },
    {
      label: "Models",
      value: models?.join(" · ") ?? "—",
      icon: "pi pi-sliders-h",
      tone: "var(--color-accent)",
    },
  ];
  return (
    <div className="grid grid-cols-2 gap-3 lg:grid-cols-3">
      {items.map((it) => (
        <Card key={it.label} className="stat-tile">
          <div className="flex items-center justify-between gap-2">
            <div className="min-w-0">
              <p className="m-0 truncate text-[10px] font-semibold uppercase tracking-wider text-muted">
                {it.label}
              </p>
              {loading ? (
                <Skeleton width="2.5rem" height="1.375rem" className="mt-0.5" />
              ) : (
                <p className="m-0 mt-0.5 truncate text-lg font-semibold tabular-nums">
                  {it.value}
                </p>
              )}
            </div>
            <span
              className="grid h-8 w-8 shrink-0 place-items-center rounded-md"
              style={{
                background: `color-mix(in srgb, ${it.tone} 14%, transparent)`,
                color: it.tone,
              }}
            >
              <i className={`${it.icon} text-sm`} />
            </span>
          </div>
        </Card>
      ))}
    </div>
  );
}

function LeaderboardCard({
  title,
  subtitle,
  icon,
  tone,
  loading,
  error,
  players,
  role,
  model,
  block,
}: {
  title: string;
  subtitle: string;
  icon: string;
  tone: string;
  loading: boolean;
  error: Error | null;
  players: ProjectionPlayer[];
  role: Role;
  model: Model;
  block: "projections" | "actuals";
}) {
  return (
    <Card>
      <header className="mb-3 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span
            className="grid h-8 w-8 place-items-center rounded-md"
            style={{
              background: `color-mix(in srgb, ${tone} 14%, transparent)`,
              color: tone,
            }}
          >
            <i className={icon} />
          </span>
          <div>
            <h2 className="m-0 text-base font-semibold">{title}</h2>
            <p className="m-0 text-xs text-muted">{subtitle}</p>
          </div>
        </div>
        {!loading && !error && <Tag value={`${players.length} players`} rounded />}
      </header>

      {loading && (
        <div className="flex flex-col gap-2">
          {Array.from({ length: 8 }).map((_, i) => (
            <Skeleton key={i} height="2rem" borderRadius="6px" />
          ))}
        </div>
      )}

      {!loading && error != null && (
        <ErrorBanner title={`Failed to load ${title}.`} message={error.message} />
      )}

      {!loading && !error && (
        <div className="overflow-x-auto">
          <LeaderboardTable
            key={`${role}-${block}`}
            role={role}
            model={model}
            players={players}
            block={block}
          />
        </div>
      )}
    </Card>
  );
}
