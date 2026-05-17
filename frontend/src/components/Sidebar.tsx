import { NavLink } from "react-router-dom";
import { cn } from "@/lib/utils";

interface NavItem {
  to: string;
  label: string;
  icon: string;
  end: boolean;
}

const NAV: NavItem[] = [
  { to: "/", label: "Archetype Clusters", icon: "pi pi-th-large", end: true },
  { to: "/similar", label: "Similar Players", icon: "pi pi-users", end: false },
];

export function Sidebar() {
  return (
    <aside className="sticky top-0 hidden h-screen w-64 shrink-0 flex-col border-r border-line bg-surface md:flex">
      <div className="flex items-center gap-2 px-5 py-5">
        <span className="grid h-9 w-9 place-items-center rounded-lg bg-accent text-white shadow-sm">
          <i className="pi pi-bolt text-base" />
        </span>
        <div className="flex flex-col leading-tight">
          <span className="text-sm font-semibold tracking-tight">Diamond DNA</span>
          <span className="text-[11px] uppercase tracking-wider text-muted">Analytics</span>
        </div>
      </div>

      <nav className="flex flex-1 flex-col gap-1 px-3 py-2" aria-label="Sections">
        <p className="px-3 pb-1 pt-3 text-[11px] font-semibold uppercase tracking-wider text-subtle">
          Explore
        </p>
        {NAV.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.end}
            className={({ isActive }) =>
              cn(
                "flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors",
                isActive
                  ? "bg-accent-soft text-[color:var(--color-accent-hover)]"
                  : "text-muted hover:bg-[color:var(--color-surface-alt)] hover:text-fg",
              )
            }
          >
            <i className={cn(item.icon, "text-base")} />
            <span>{item.label}</span>
          </NavLink>
        ))}
      </nav>

      <div className="border-t border-line px-5 py-4 text-[11px] leading-relaxed text-muted">
        <p className="m-0 font-medium text-fg">PCA · KNN · K-Means</p>
        <p className="m-0">Statcast pipeline → daily refresh</p>
      </div>
    </aside>
  );
}
