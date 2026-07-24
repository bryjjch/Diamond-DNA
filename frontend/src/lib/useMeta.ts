import { useQuery } from "@tanstack/react-query";
import { api } from "./api";

/** Shared /api/meta query — Topbar and pages all key off the same cache entry. */
export function useMeta() {
  return useQuery({ queryKey: ["meta"], queryFn: api.meta });
}
