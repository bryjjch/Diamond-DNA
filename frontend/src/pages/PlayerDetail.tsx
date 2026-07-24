import { Link, Navigate, useParams, useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Card } from "primereact/card";
import { Tag } from "primereact/tag";
import { Skeleton } from "primereact/skeleton";
import { TabView, TabPanel } from "primereact/tabview";
import { Accordion, AccordionTab } from "primereact/accordion";
import { api, ApiError } from "@/lib/api";
import { useSeasonYear } from "@/lib/season";
import { fmtNumber, formatStat, HISTORY_STATS, ROLE_STATS } from "@/lib/stats";
import { ROLE_TAG_SEVERITY, ROLE_TONE } from "@/lib/theme";
import type { Model, Neighbor, PlayerProfile } from "@/lib/types";
import { ModelSelect } from "@/components/ModelSelect";
import { PlayerSearch } from "@/components/PlayerSearch";
import { ArchetypeProbs } from "@/components/ArchetypeProbs";
import { ErrorBanner } from "@/components/ErrorBanner";

export function PlayerDetail() {
  const { playerId: playerIdParam } = useParams();
  const [params, setParams] = useSearchParams();
  const model = ((params.get("model") as Model) || "xgboost") as Model;

  const playerId = Number(playerIdParam);
  const invalidId = !Number.isInteger(playerId) || playerId <= 0;

  const season = useSeasonYear();

  const playerQuery = useQuery({
    queryKey: ["player", playerId, model, season.year],
    // No role param: the server returns every profile it finds, which covers
    // two-way players and the catcher-archetype fallback.
    queryFn: () => api.player({ player_id: playerId, model, year: season.year }),
    enabled: !invalidId && season.year !== undefined,
  });

  if (invalidId) return <Navigate to="/" replace />;

  const notFound =
    playerQuery.error instanceof ApiError && playerQuery.error.status === 404;
  const data = playerQuery.data;

  return (
    <section className="flex flex-col gap-6">
      <Card>
        <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div className="min-w-0">
            {playerQuery.isLoading ? (
              <Skeleton width="14rem" height="2rem" />
            ) : (
              <h2 className="m-0 truncate text-xl font-semibold">
                {data?.player_name ?? `Player ${playerId}`}
              </h2>
            )}
            {data && (
              <div className="mt-1.5 flex items-center gap-2">
                <Tag value={`Season ${data.year}`} rounded icon="pi pi-calendar" />
                <Tag value={data.model} rounded severity="success" />
              </div>
            )}
          </div>
          <div className="flex flex-wrap items-end gap-6">
            <ModelSelect
              value={model}
              onChange={(next) => {
                const nextParams = new URLSearchParams(params);
                nextParams.set("model", next);
                setParams(nextParams, { replace: true });
              }}
            />
            <div className="flex flex-col gap-1">
              <span className="text-[11px] font-semibold uppercase tracking-wider text-muted">
                Jump to player
              </span>
              <PlayerSearch />
            </div>
          </div>
        </div>
      </Card>

      {notFound && (
        <Card>
          <div className="flex flex-col items-center gap-3 py-10 text-center">
            <span className="grid h-14 w-14 place-items-center rounded-full bg-accent-soft text-[color:var(--color-accent-hover)]">
              <i className="pi pi-user-minus text-2xl" />
            </span>
            <div>
              <p className="m-0 text-base font-semibold">No data for this player</p>
              <p className="m-0 mt-1 text-sm text-muted">
                Player {playerId} has no projection for the current season.
              </p>
            </div>
            <Link
              to="/"
              className="text-sm font-medium text-[color:var(--color-accent-hover)]"
            >
              ← Back to the leaderboards
            </Link>
          </div>
        </Card>
      )}

      {!notFound && playerQuery.error != null && (
        <ErrorBanner
          title="Failed to load player."
          message={(playerQuery.error as Error).message}
        />
      )}

      {(season.isLoading || playerQuery.isLoading) && (
        <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
          {Array.from({ length: 4 }).map((_, i) => (
            <Skeleton key={i} height="16rem" borderRadius="12px" />
          ))}
        </div>
      )}

      {data && data.profiles.length === 0 && (
        <Card>
          <p className="m-0 py-4 text-center text-sm text-muted">
            No profiles available for this player.
          </p>
        </Card>
      )}

      {data && data.profiles.length === 1 && <ProfileSections profile={data.profiles[0]} />}

      {data && data.profiles.length > 1 && (
        <TabView>
          {data.profiles.map((profile) => (
            <TabPanel
              key={profile.role}
              header={profile.role === "batter" ? "Batter" : "Pitcher"}
            >
              <ProfileSections profile={profile} />
            </TabPanel>
          ))}
        </TabView>
      )}
    </section>
  );
}

function ProfileSections({ profile }: { profile: PlayerProfile }) {
  const tone = ROLE_TONE[profile.role];
  return (
    <div className="grid grid-cols-1 items-start gap-6 xl:grid-cols-2">
      <ProjectionCard profile={profile} />
      <ArchetypeCard profile={profile} tone={tone} />
      <HistoryCard profile={profile} />
      <NeighborsCard neighbors={profile.neighbors} tone={tone} />
    </div>
  );
}

function SectionHeader({
  icon,
  title,
  subtitle,
  extra,
}: {
  icon: string;
  title: string;
  subtitle?: string;
  extra?: React.ReactNode;
}) {
  return (
    <header className="mb-3 flex items-center justify-between gap-2">
      <div className="flex min-w-0 items-center gap-2">
        <span className="grid h-8 w-8 shrink-0 place-items-center rounded-md bg-accent-soft text-[color:var(--color-accent-hover)]">
          <i className={icon} />
        </span>
        <div className="min-w-0">
          <h3 className="m-0 truncate text-base font-semibold">{title}</h3>
          {subtitle && <p className="m-0 truncate text-xs text-muted">{subtitle}</p>}
        </div>
      </div>
      {extra}
    </header>
  );
}

function ProjectionCard({ profile }: { profile: PlayerProfile }) {
  if (!profile.projection) return null;
  const stats = ROLE_STATS[profile.role];
  const meta = [
    profile.primary_position && `Pos ${profile.primary_position}`,
    profile.age !== null && `Age ${profile.age}`,
  ]
    .filter(Boolean)
    .join(" · ");

  return (
    <Card>
      <SectionHeader
        icon="pi pi-chart-line"
        title="Projection"
        subtitle={meta || undefined}
        extra={
          <Tag value={profile.role} severity={ROLE_TAG_SEVERITY[profile.role]} rounded />
        }
      />
      <div className="overflow-x-auto">
        <table className="w-full border-collapse text-sm">
          <thead>
            <tr className="text-left text-[11px] font-semibold uppercase tracking-wider text-muted">
              <th className="border-b border-line py-2 pr-3 font-semibold">Stat</th>
              <th className="border-b border-line px-3 py-2 text-right font-semibold">
                Projected
              </th>
              <th className="border-b border-line px-3 py-2 text-right font-semibold">
                Actual to date
              </th>
            </tr>
          </thead>
          <tbody>
            {stats.map((s) => (
              <tr key={s.key} className="border-b border-line last:border-b-0">
                <td className="py-1.5 pr-3 font-medium">{s.label}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">
                  {formatStat(profile.role, s.key, profile.projection?.stats?.[s.key])}
                </td>
                <td className="px-3 py-1.5 text-right tabular-nums text-muted">
                  {formatStat(profile.role, s.key, profile.projection?.actuals?.[s.key])}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

function ArchetypeCard({ profile, tone }: { profile: PlayerProfile; tone: string }) {
  const archetype = profile.archetype;
  return (
    <Card>
      <SectionHeader
        icon="pi pi-th-large"
        title={archetype ? archetype.label : "Archetype"}
        subtitle="Soft cluster membership (GMM)"
        extra={
          archetype && archetype.archetype_role !== profile.role ? (
            <Tag
              value={archetype.archetype_role}
              severity={ROLE_TAG_SEVERITY[archetype.archetype_role]}
              rounded
            />
          ) : undefined
        }
      />
      {!archetype && (
        <p className="m-0 py-4 text-center text-sm text-muted">
          No archetype profile for this player.
        </p>
      )}
      {archetype && (
        <>
          {archetype.description && (
            <p className="mb-3 mt-0 text-sm text-muted">{archetype.description}</p>
          )}
          <ArchetypeProbs
            probs={archetype.probs}
            primaryClusterId={archetype.cluster_id}
            tone={tone}
          />
        </>
      )}
    </Card>
  );
}

function HistoryCard({ profile }: { profile: PlayerProfile }) {
  const history = profile.history;
  const stats = HISTORY_STATS[profile.role];
  const seasons = [...(history?.seasons ?? [])].sort((a, b) => b.season - a.season);
  const statcast = history?.statcast_last_season;
  const statcastEntries = statcast?.available
    ? Object.entries(statcast).filter(
        (entry): entry is [string, number] =>
          entry[0] !== "available" && typeof entry[1] === "number",
      )
    : [];

  return (
    <Card>
      <SectionHeader icon="pi pi-history" title="Season history" />
      {seasons.length === 0 && (
        <p className="m-0 py-4 text-center text-sm text-muted">No season history.</p>
      )}
      {seasons.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr className="text-[11px] font-semibold uppercase tracking-wider text-muted">
                <th className="border-b border-line py-2 pr-3 text-left font-semibold">
                  Season
                </th>
                {stats.map((s) => (
                  <th
                    key={s.key}
                    className="border-b border-line px-2 py-2 text-right font-semibold"
                  >
                    {s.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {seasons.map((season) => (
                <tr
                  key={season.season}
                  className={`border-b border-line last:border-b-0 ${
                    season.available ? "" : "opacity-45"
                  }`}
                >
                  <td className="py-1.5 pr-3 font-medium tabular-nums">{season.season}</td>
                  {season.available ? (
                    stats.map((s) => (
                      <td key={s.key} className="px-2 py-1.5 text-right tabular-nums">
                        {formatStat(profile.role, s.key, season.stats?.[s.key])}
                      </td>
                    ))
                  ) : (
                    <td
                      colSpan={stats.length}
                      className="px-2 py-1.5 text-left text-xs text-muted"
                    >
                      no season data
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {statcastEntries.length > 0 && (
        <Accordion className="mt-4">
          <AccordionTab header="Statcast (last season)">
            <dl className="m-0 grid grid-cols-2 gap-x-4 gap-y-1.5 text-sm sm:grid-cols-3">
              {statcastEntries.map(([key, value]) => (
                <div key={key} className="flex items-baseline justify-between gap-2">
                  <dt className="truncate text-xs text-muted">{labelize(key)}</dt>
                  <dd className="m-0 shrink-0 font-medium tabular-nums">
                    {fmtNumber(value)}
                  </dd>
                </div>
              ))}
            </dl>
          </AccordionTab>
        </Accordion>
      )}
    </Card>
  );
}

function NeighborsCard({
  neighbors,
  tone,
}: {
  neighbors: Neighbor[] | null;
  tone: string;
}) {
  return (
    <Card>
      <SectionHeader
        icon="pi pi-users"
        title="Similar players"
        subtitle="Nearest neighbors in PCA feature space"
        extra={
          neighbors && neighbors.length > 0 ? (
            <Tag value={`${neighbors.length}`} rounded />
          ) : undefined
        }
      />
      {(!neighbors || neighbors.length === 0) && (
        <p className="m-0 py-4 text-center text-sm text-muted">
          No comparables for this player.
        </p>
      )}
      {neighbors && neighbors.length > 0 && (
        <ol className="m-0 list-none p-0">
          {neighbors.map((n) => (
            <li
              key={n.rank}
              className="flex items-center gap-3 border-b border-line py-2 last:border-b-0"
            >
              <span className="grid h-7 w-7 shrink-0 place-items-center rounded-md bg-[color:var(--color-surface-alt)] text-xs font-semibold tabular-nums text-muted">
                {n.rank}
              </span>
              <Link
                to={`/player/${n.player_id}`}
                className="min-w-0 flex-1 truncate text-sm font-medium text-[color:var(--color-accent-hover)] no-underline hover:underline"
              >
                {n.player_name}
              </Link>
              <span
                className="shrink-0 rounded-full px-2 py-0.5 text-[11px] font-medium tabular-nums"
                style={{
                  background: `color-mix(in srgb, ${tone} 12%, transparent)`,
                  color: tone,
                }}
              >
                Δ {n.distance.toFixed(3)}
              </span>
            </li>
          ))}
        </ol>
      )}
    </Card>
  );
}

function labelize(key: string): string {
  return key
    .split("_")
    .map((part) => (part === "pct" ? "%" : part.charAt(0).toUpperCase() + part.slice(1)))
    .join(" ");
}
