import { useQuery } from "@tanstack/react-query";
import { Dialog } from "primereact/dialog";
import { ProgressSpinner } from "primereact/progressspinner";
import { api } from "@/lib/api";
import type { Role } from "@/lib/types";

const ROLE_TONE: Record<Role, string> = {
  batter: "var(--color-accent)",
  pitcher: "#0ea5e9",
};

interface Props {
  playerId: number | null;
  role: Role;
  onClose: () => void;
}

export function SoftProfileModal({ playerId, role, onClose }: Props) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["softProfile", role, playerId],
    queryFn: () => api.playerSoftProfile(role, playerId!),
    enabled: playerId !== null,
  });

  const tone = ROLE_TONE[role];
  const maxProb = data ? Math.max(...data.probs.map((p) => p.prob)) : 1;

  return (
    <Dialog
      visible={playerId !== null}
      onHide={onClose}
      header={data ? `${data.player_name} — Archetype Fit` : "Archetype Fit"}
      style={{ width: "min(480px, 95vw)" }}
      draggable={false}
      resizable={false}
    >
      {isLoading && (
        <div className="flex items-center gap-3 py-4 text-sm text-muted">
          <ProgressSpinner style={{ width: 18, height: 18 }} strokeWidth="6" />
          Loading profile…
        </div>
      )}

      {error && (
        <p className="py-4 text-sm text-muted">
          Soft probabilities not available for this player/snapshot.
        </p>
      )}

      {data && (
        <div className="flex flex-col gap-1">
          <p className="mb-3 text-xs text-muted">
            Primary archetype:{" "}
            <span className="font-semibold text-foreground">{data.cluster_label}</span>
          </p>
          <ul className="m-0 list-none p-0">
            {data.probs.map((p) => {
              const pct = Math.round(p.prob * 100);
              const barWidth = maxProb > 0 ? `${(p.prob / maxProb) * 100}%` : "0%";
              const isPrimary = p.cluster_id === data.cluster_id;
              return (
                <li key={p.cluster_id} className="mb-3">
                  <div className="mb-1 flex items-center justify-between gap-2 text-sm">
                    <span className={isPrimary ? "font-semibold" : "text-muted"}>
                      {p.label}
                    </span>
                    <span
                      className="shrink-0 tabular-nums text-xs font-semibold"
                      style={{ color: isPrimary ? tone : undefined }}
                    >
                      {pct}%
                    </span>
                  </div>
                  <div
                    className="h-1.5 w-full overflow-hidden rounded-full"
                    style={{ background: `color-mix(in srgb, ${tone} 12%, transparent)` }}
                  >
                    <div
                      className="h-full rounded-full transition-all"
                      style={{
                        width: barWidth,
                        background: isPrimary
                          ? tone
                          : `color-mix(in srgb, ${tone} 40%, transparent)`,
                      }}
                    />
                  </div>
                </li>
              );
            })}
          </ul>
        </div>
      )}
    </Dialog>
  );
}
