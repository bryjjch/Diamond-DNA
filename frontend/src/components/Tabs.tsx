import { NavLink } from "react-router-dom";
import { cn } from "@/lib/utils";

const tabs = [
  { to: "/", label: "Clusters", end: true },
  { to: "/similar", label: "Similar players (KNN)", end: false },
] as const;

export function Tabs() {
  return (
    <nav className="border-b border-line bg-surface px-4" aria-label="Sections">
      <div className="mx-auto flex max-w-6xl gap-0">
        {tabs.map((tab) => (
          <NavLink
            key={tab.to}
            to={tab.to}
            end={tab.end}
            className={({ isActive }) =>
              cn(
                "border-b-2 px-5 py-3 text-sm transition-colors",
                isActive
                  ? "border-accent text-accent"
                  : "border-transparent text-muted hover:text-fg",
              )
            }
          >
            {tab.label}
          </NavLink>
        ))}
      </div>
    </nav>
  );
}
