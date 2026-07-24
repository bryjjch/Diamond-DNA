import { useQuery } from "@tanstack/react-query";
import { api } from "./api";
import { useMeta } from "./useMeta";

// Optional build-time pin, e.g. VITE_SEASON_YEAR=2026 to freeze the site on a season.
const OVERRIDE = Number(import.meta.env.VITE_SEASON_YEAR) || undefined;

/**
 * Season a fresh projection set is "for" today. From November on, the offseason
 * is already projecting next year.
 */
export function currentSeasonYear(date = new Date()): number {
  const year = date.getUTCFullYear();
  return date.getUTCMonth() >= 10 ? year + 1 : year;
}

function probeKey(year: number) {
  return ["season-probe", year] as const;
}

/**
 * Season the site displays.
 *
 * /api/meta's `default_year` is derived from the calendar on the Lambda side
 * (current year − 1), so it lags the live season even when the newer artifacts
 * are already in S3. We probe the newest plausible season instead, step back a
 * year if it has no data, and fall back to `default_year` if neither answers.
 *
 * `year` is undefined while resolving — gate queries on it the way pages
 * previously gated on `meta.default_year`.
 */
export function useSeasonYear(): { year?: number; isLoading: boolean } {
  const meta = useMeta();
  const candidate = OVERRIDE ?? currentSeasonYear();

  // limit=1 keeps the probe cheap; a missing season answers 500, not an empty list.
  const primary = useQuery({
    queryKey: probeKey(candidate),
    queryFn: () =>
      api.projections({ role: "batter", model: "xgboost", year: candidate, limit: 1 }),
    retry: false,
    staleTime: Infinity,
  });

  const fallbackYear = candidate - 1;
  const fallback = useQuery({
    queryKey: probeKey(fallbackYear),
    queryFn: () =>
      api.projections({ role: "batter", model: "xgboost", year: fallbackYear, limit: 1 }),
    enabled: primary.isError,
    retry: false,
    staleTime: Infinity,
  });

  let year: number | undefined;
  if (primary.data && primary.data.total > 0) year = candidate;
  else if (fallback.data && fallback.data.total > 0) year = fallbackYear;
  else if (primary.isError && (fallback.isError || fallback.data)) year = meta.data?.default_year;

  return {
    year,
    isLoading: year === undefined && (meta.isLoading || primary.isLoading || fallback.isLoading),
  };
}
