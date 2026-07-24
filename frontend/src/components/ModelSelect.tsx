import { SelectButton } from "primereact/selectbutton";
import type { Model } from "@/lib/types";

interface ModelSelectProps {
  value: Model;
  onChange: (next: Model) => void;
  label?: string;
}

const OPTIONS = [
  { label: "XGBoost", value: "xgboost", icon: "pi pi-chart-line" },
  { label: "Marcel", value: "marcel", icon: "pi pi-calculator" },
] as const;

export function ModelSelect({ value, onChange, label = "Model" }: ModelSelectProps) {
  return (
    <div className="flex flex-col gap-1">
      <span className="text-[11px] font-semibold uppercase tracking-wider text-muted">
        {label}
      </span>
      <SelectButton
        value={value}
        onChange={(e) => {
          if (e.value) onChange(e.value as Model);
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
