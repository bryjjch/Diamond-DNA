import { AlertCircle } from "lucide-react";

export function ErrorBanner({ title, message }: { title: string; message: string }) {
  return (
    <div
      role="alert"
      className="mx-auto my-4 flex max-w-6xl items-start gap-3 rounded-lg border border-[color:var(--color-danger-line)] bg-[color:var(--color-danger-bg)] px-5 py-4"
    >
      <AlertCircle className="mt-0.5 shrink-0" size={18} />
      <div>
        <p className="m-0 font-semibold">{title}</p>
        <p className="mt-1 text-sm">{message}</p>
      </div>
    </div>
  );
}
