import type { SoftProb } from "@/lib/types";

interface Props {
  probs: SoftProb[];
  primaryClusterId: number;
  tone: string;
}

/** Horizontal soft-membership bars — every bar carries its % as text. */
export function ArchetypeProbs({ probs, primaryClusterId, tone }: Props) {
  const maxProb = probs.length > 0 ? Math.max(...probs.map((p) => p.prob)) : 1;

  return (
    <ul className="m-0 list-none p-0">
      {probs.map((p) => {
        const pct = Math.round(p.prob * 100);
        const barWidth = maxProb > 0 ? `${(p.prob / maxProb) * 100}%` : "0%";
        const isPrimary = p.cluster_id === primaryClusterId;
        return (
          <li key={p.cluster_id} className="mb-3 last:mb-0">
            <div className="mb-1 flex items-center justify-between gap-2 text-sm">
              <span className={isPrimary ? "font-semibold" : "text-muted"}>{p.label}</span>
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
  );
}
