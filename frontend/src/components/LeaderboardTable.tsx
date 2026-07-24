import { useState } from "react";
import { Link } from "react-router-dom";
import { DataTable } from "primereact/datatable";
import { Column } from "primereact/column";
import { DEFAULT_SORT, ROLE_STATS } from "@/lib/stats";
import type { Model, ProjectionPlayer, Role } from "@/lib/types";

interface Props {
  role: Role;
  model: Model;
  players: ProjectionPlayer[];
  /** Which stat block of each row to render/sort. */
  block: "projections" | "actuals";
}

/**
 * Sortable leaderboard over a fetched /projections payload. Sorting is
 * client-side: the API can only server-sort projected stats, and ≤1000 rows
 * sort instantly in-browser anyway. Remount (key on role+block) to reset sort.
 */
export function LeaderboardTable({ role, model, players, block }: Props) {
  const stats = ROLE_STATS[role];
  const defaultStat = stats.find((s) => s.key === DEFAULT_SORT[role]);
  const [sortField, setSortField] = useState(`${block}.${DEFAULT_SORT[role]}`);
  const [sortOrder, setSortOrder] = useState<number>(defaultStat?.lowerIsBetter ? 1 : -1);

  return (
    <DataTable
      value={players}
      dataKey="player_id"
      size="small"
      stripedRows
      paginator
      rows={25}
      rowsPerPageOptions={[10, 25, 50, 100]}
      sortField={sortField}
      sortOrder={sortOrder as 1 | -1}
      onSort={(e) => {
        setSortField(e.sortField);
        setSortOrder(e.sortOrder ?? -1);
      }}
      emptyMessage="No players available."
      className="text-sm"
    >
      <Column
        field="player_name"
        header="Player"
        sortable
        body={(p: ProjectionPlayer) => (
          <Link
            to={`/player/${p.player_id}?model=${model}`}
            className="font-medium text-[color:var(--color-accent-hover)] no-underline hover:underline"
          >
            {p.player_name ?? `Player ${p.player_id}`}
          </Link>
        )}
      />
      <Column
        field="age"
        header="Age"
        sortable
        align="right"
        alignHeader="right"
        body={(p: ProjectionPlayer) => (
          <span className="tabular-nums">{p.age ?? "—"}</span>
        )}
      />
      <Column
        field="primary_position"
        header="Pos"
        sortable
        body={(p: ProjectionPlayer) => p.primary_position ?? "—"}
      />
      {stats.map((s) => (
        <Column
          key={s.key}
          field={`${block}.${s.key}`}
          header={s.label}
          sortable
          align="right"
          alignHeader="right"
          body={(p: ProjectionPlayer) => {
            const v = p[block]?.[s.key];
            return (
              <span className="tabular-nums">
                {v === null || v === undefined ? "—" : s.format(v)}
              </span>
            );
          }}
        />
      ))}
    </DataTable>
  );
}
