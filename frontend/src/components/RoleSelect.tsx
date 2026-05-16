import type { Role } from "@/lib/types";

interface RoleSelectProps {
  value: Role;
  onChange: (next: Role) => void;
  label?: string;
}

export function RoleSelect({ value, onChange, label = "Role" }: RoleSelectProps) {
  return (
    <label className="flex items-center gap-2 text-sm text-muted">
      {label}
      <select
        aria-label={label}
        value={value}
        onChange={(e) => onChange(e.target.value as Role)}
        className="rounded-md border border-line bg-surface px-3 py-2 text-fg focus:outline-2 focus:outline-accent-dim"
      >
        <option value="batter">Batter</option>
        <option value="pitcher">Pitcher</option>
      </select>
    </label>
  );
}
