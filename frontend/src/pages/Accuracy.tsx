import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Card } from "primereact/card";
import { Tag } from "primereact/tag";
import { Skeleton } from "primereact/skeleton";
import { SelectButton } from "primereact/selectbutton";
import { DataTable } from "primereact/datatable";
import { Column } from "primereact/column";
import { Accordion, AccordionTab } from "primereact/accordion";
import { api } from "@/lib/api";
import { useSeasonYear } from "@/lib/season";
import { fmtNumber, statLabel } from "@/lib/stats";
import type { AccuracyDetailRow, AccuracySummaryRow, Role } from "@/lib/types";
import { ErrorBanner } from "@/components/ErrorBanner";

type RoleFilter = "all" | Role;

const ROLE_OPTIONS = [
  { label: "All", value: "all" },
  { label: "Batters", value: "batter" },
  { label: "Pitchers", value: "pitcher" },
];

// Bar fills use the brighter hues; numeric text uses darker steps of the same
// hues so the values stay legible on the light surface.
const POSITIVE = "var(--color-accent)";
const NEGATIVE = "#dc2626";
const POSITIVE_TEXT = "#047857";
const NEGATIVE_TEXT = "#b91c1c";

export function Accuracy() {
  const [params, setParams] = useSearchParams();
  const role = ((params.get("role") as RoleFilter) || "all") as RoleFilter;

  const season = useSeasonYear();
  // The live season is only partly played, so its errors aren't comparable with
  // full-season backtests — stop at the last completed season.
  const endYear = season.year !== undefined ? season.year - 1 : undefined;

  const accuracyQuery = useQuery({
    queryKey: ["accuracy", role, endYear],
    queryFn: () => api.accuracy({ role, end_year: endYear }),
    enabled: endYear !== undefined,
  });

  const data = accuracyQuery.data;
  const summary = data?.summary ?? [];
  const detail = data?.detail ?? [];
  // Shared bar scale so cell bars are comparable across the whole table.
  const maxAbsPct = Math.max(
    1,
    ...summary.flatMap((r) => [
      Math.abs(r.mean_mae_pct_improvement ?? 0),
      Math.abs(r.mean_rmse_pct_improvement ?? 0),
    ]),
  );

  return (
    <section className="flex flex-col gap-6">
      <Card>
        <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div>
            <h2 className="m-0 text-base font-semibold">XGBoost vs. Marcel baseline</h2>
            <p className="m-0 mt-0.5 text-xs text-muted">
              Backtest over completed seasons — positive % means XGBoost beats the Marcel
              baseline on that error metric.
            </p>
            {data && (
              <div className="mt-2 flex flex-wrap items-center gap-2">
                <Tag
                  value={`${data.start_year}–${data.end_year}`}
                  rounded
                  icon="pi pi-calendar"
                />
                <Tag
                  value={`${data.years_compared.length} seasons compared`}
                  rounded
                  severity="success"
                />
              </div>
            )}
          </div>
          <div className="flex flex-col gap-1">
            <span className="text-[11px] font-semibold uppercase tracking-wider text-muted">
              Role
            </span>
            <SelectButton
              value={role}
              onChange={(e) => {
                if (!e.value) return;
                const next = new URLSearchParams(params);
                next.set("role", e.value as string);
                setParams(next, { replace: true });
              }}
              options={ROLE_OPTIONS}
              allowEmpty={false}
            />
          </div>
        </div>
      </Card>

      {accuracyQuery.error != null && (
        <ErrorBanner
          title="Failed to load accuracy comparison."
          message={(accuracyQuery.error as Error).message}
        />
      )}

      {(season.isLoading || accuracyQuery.isLoading) && (
        <Card>
          <div className="flex flex-col gap-2">
            {Array.from({ length: 8 }).map((_, i) => (
              <Skeleton key={i} height="2rem" borderRadius="6px" />
            ))}
          </div>
        </Card>
      )}

      {data && (
        <>
          <Card>
            <header className="mb-3">
              <h3 className="m-0 text-base font-semibold">Summary by stat</h3>
              <p className="m-0 mt-0.5 text-xs text-muted">
                Mean improvement over the compared seasons; win rate is the share of
                seasons where XGBoost had the lower MAE.
              </p>
            </header>
            <div className="overflow-x-auto">
              <DataTable
                value={summary}
                size="small"
                stripedRows
                sortMode="single"
                sortField="mean_mae_pct_improvement"
                sortOrder={-1}
                emptyMessage="No summary rows."
              >
                {role === "all" && (
                  <Column field="role" header="Role" sortable style={{ width: "6rem" }} />
                )}
                <Column
                  field="stat"
                  header="Stat"
                  sortable
                  body={(r: AccuracySummaryRow) => (
                    <span className="font-medium">{statLabel(r.stat)}</span>
                  )}
                />
                <Column
                  field="n_years"
                  header="Years"
                  sortable
                  align="right"
                  alignHeader="right"
                />
                <Column
                  field="mean_mae_pct_improvement"
                  header="MAE Δ%"
                  sortable
                  body={(r: AccuracySummaryRow) => (
                    <PctImprovementCell value={r.mean_mae_pct_improvement} max={maxAbsPct} />
                  )}
                />
                <Column
                  field="mean_rmse_pct_improvement"
                  header="RMSE Δ%"
                  sortable
                  body={(r: AccuracySummaryRow) => (
                    <PctImprovementCell
                      value={r.mean_rmse_pct_improvement}
                      max={maxAbsPct}
                    />
                  )}
                />
                <Column
                  field="win_rate_mae"
                  header="MAE win rate"
                  sortable
                  align="right"
                  alignHeader="right"
                  body={(r: AccuracySummaryRow) => (
                    <span className="tabular-nums">
                      {r.win_rate_mae === null
                        ? "—"
                        : `${Math.round(r.win_rate_mae * 100)}%`}
                    </span>
                  )}
                />
              </DataTable>
            </div>
          </Card>

          <Accordion>
            <AccordionTab header={`Per-year detail (${detail.length} rows)`}>
              <div className="overflow-x-auto">
                <DataTable
                  value={detail}
                  size="small"
                  stripedRows
                  paginator
                  rows={25}
                  sortMode="single"
                  sortField="year"
                  sortOrder={-1}
                  emptyMessage="No detail rows."
                >
                  <Column field="role" header="Role" sortable />
                  <Column field="year" header="Year" sortable align="right" alignHeader="right" />
                  <Column
                    field="stat"
                    header="Stat"
                    sortable
                    body={(r: AccuracyDetailRow) => statLabel(r.stat)}
                  />
                  <Column field="n" header="n" sortable align="right" alignHeader="right" />
                  <Column
                    field="xgboost.mae"
                    header="XGB MAE"
                    sortable
                    align="right"
                    alignHeader="right"
                    body={(r: AccuracyDetailRow) => (
                      <span className="tabular-nums">{fmtNumber(r.xgboost.mae)}</span>
                    )}
                  />
                  <Column
                    field="marcel.mae"
                    header="Marcel MAE"
                    sortable
                    align="right"
                    alignHeader="right"
                    body={(r: AccuracyDetailRow) => (
                      <span className="tabular-nums">{fmtNumber(r.marcel.mae)}</span>
                    )}
                  />
                  <Column
                    field="mae_pct_improvement"
                    header="MAE Δ%"
                    sortable
                    align="right"
                    alignHeader="right"
                    body={(r: AccuracyDetailRow) => (
                      <SignedPct value={r.mae_pct_improvement} />
                    )}
                  />
                  <Column
                    field="rmse_pct_improvement"
                    header="RMSE Δ%"
                    sortable
                    align="right"
                    alignHeader="right"
                    body={(r: AccuracyDetailRow) => (
                      <SignedPct value={r.rmse_pct_improvement} />
                    )}
                  />
                  <Column
                    field="candidate_wins_mae"
                    header="XGB wins"
                    sortable
                    align="center"
                    alignHeader="center"
                    body={(r: AccuracyDetailRow) =>
                      r.candidate_wins_mae ? (
                        <i
                          className="pi pi-check-circle"
                          style={{ color: POSITIVE_TEXT }}
                          aria-label="XGBoost wins"
                        />
                      ) : (
                        <i
                          className="pi pi-minus-circle text-subtle"
                          aria-label="Marcel wins"
                        />
                      )
                    }
                  />
                </DataTable>
              </div>
            </AccordionTab>
          </Accordion>
        </>
      )}
    </section>
  );
}

/** Signed % with a mini bar; the number itself always carries the value. */
function PctImprovementCell({ value, max }: { value: number | null; max: number }) {
  if (value === null) return <span className="text-muted">—</span>;
  const barTone = value >= 0 ? POSITIVE : NEGATIVE;
  const width = `${Math.min(100, (Math.abs(value) / max) * 100)}%`;
  return (
    <div className="flex min-w-32 items-center gap-2">
      <span
        className="w-14 shrink-0 text-right text-sm font-semibold tabular-nums"
        style={{ color: value >= 0 ? POSITIVE_TEXT : NEGATIVE_TEXT }}
      >
        {value > 0 ? "+" : ""}
        {value.toFixed(1)}%
      </span>
      <div
        className="h-1.5 flex-1 overflow-hidden rounded-full"
        style={{ background: "color-mix(in srgb, currentColor 8%, transparent)" }}
      >
        <div
          className="h-full rounded-full"
          style={{ width, background: barTone }}
        />
      </div>
    </div>
  );
}

function SignedPct({ value }: { value: number | null }) {
  if (value === null) return <span className="text-muted">—</span>;
  return (
    <span
      className="tabular-nums font-medium"
      style={{ color: value >= 0 ? POSITIVE_TEXT : NEGATIVE_TEXT }}
    >
      {value > 0 ? "+" : ""}
      {value.toFixed(1)}%
    </span>
  );
}
