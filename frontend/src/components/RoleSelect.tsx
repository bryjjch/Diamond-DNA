import { SelectButton } from "primereact/selectbutton";
import type { Role } from "@/lib/types";

interface RoleSelectProps {
  value: Role;
  onChange: (next: Role) => void;
  label?: string;
}

const OPTIONS = [
  { label: "Batters", value: "batter", icon: "pi pi-bullseye" },
  { label: "Pitchers", value: "pitcher", icon: "pi pi-send" },
] as const;

export function RoleSelect({ value, onChange, label = "Role" }: RoleSelectProps) {
  return (
    <div className="flex flex-col gap-1">
      <span className="text-[11px] font-semibold uppercase tracking-wider text-muted">
        {label}
      </span>
      <SelectButton
        value={value}
        onChange={(e) => {
          if (e.value) onChange(e.value as Role);
        }}
        options={OPTIONS as unknown as { label: string; value: string }[]}
        optionLabel="label"
        itemTemplate={(opt: { label: string; icon: string }) => (
          <span className="flex items-center gap-2">
            <i className={opt.icon} />
            {opt.label}
          </span>
        )}
        allowEmpty={false}
      />
    </div>
  );
}
