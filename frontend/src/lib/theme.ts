import type { ArchetypeRole, Role } from "./types";

/** Accent tone per role — emerald for batters, sky for pitchers. */
export const ROLE_TONE: Record<Role, string> = {
  batter: "var(--color-accent)",
  pitcher: "#0ea5e9",
};

export const ROLE_TAG_SEVERITY: Record<ArchetypeRole, "success" | "info" | "warning"> = {
  batter: "success",
  pitcher: "info",
  catcher: "warning",
};
