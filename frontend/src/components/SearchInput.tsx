import { Search } from "lucide-react";
import type { InputHTMLAttributes } from "react";
import { cn } from "@/lib/utils";

interface SearchInputProps extends InputHTMLAttributes<HTMLInputElement> {
  label?: string;
}

export function SearchInput({ label, className, ...props }: SearchInputProps) {
  return (
    <div className={cn("relative min-w-[220px]", className)}>
      {label && <span className="sr-only">{label}</span>}
      <Search
        className="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-muted"
        size={16}
      />
      <input
        type="search"
        autoComplete="off"
        {...props}
        className="w-full rounded-md border border-line bg-surface py-2 pl-8 pr-3 text-sm text-fg placeholder:text-muted focus:outline-2 focus:outline-accent-dim"
      />
    </div>
  );
}
