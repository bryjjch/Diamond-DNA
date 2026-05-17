import { InputText } from "primereact/inputtext";
import { IconField } from "primereact/iconfield";
import { InputIcon } from "primereact/inputicon";
import { cn } from "@/lib/utils";

interface SearchInputProps {
  value: string;
  onChange: (next: string) => void;
  placeholder?: string;
  label?: string;
  className?: string;
}

export function SearchInput({
  value,
  onChange,
  placeholder,
  label,
  className,
}: SearchInputProps) {
  return (
    <IconField iconPosition="left" className={cn("w-full md:w-72", className)}>
      <InputIcon className="pi pi-search" />
      {label && <span className="sr-only">{label}</span>}
      <InputText
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        type="search"
        autoComplete="off"
        aria-label={label}
        className="w-full"
      />
    </IconField>
  );
}
