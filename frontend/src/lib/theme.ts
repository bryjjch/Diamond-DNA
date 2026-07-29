import type { ArchetypeRole, Role } from "./types";

/** Accent tone per role — club royal for batters, the lighter blue for pitchers. */
export const ROLE_TONE: Record<Role, string> = {
  batter: "var(--color-accent)",
  pitcher: "var(--color-sky)",
};

export const ROLE_TAG_SEVERITY: Record<ArchetypeRole, "success" | "info" | "warning"> = {
  batter: "success",
  pitcher: "info",
  catcher: "warning",
};
