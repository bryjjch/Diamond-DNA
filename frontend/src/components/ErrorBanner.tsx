import { Message } from "primereact/message";

export function ErrorBanner({ title, message }: { title: string; message: string }) {
  return (
    <Message
      severity="error"
      className="mb-4 w-full justify-start"
      content={
        <div className="flex items-start gap-3">
          <i className="pi pi-exclamation-circle mt-0.5 text-lg text-[color:var(--color-danger-line)]" />
          <div>
            <p className="m-0 font-semibold text-[color:var(--color-danger-line)]">
              {title}
            </p>
            <p className="m-0 mt-0.5 text-sm text-[color:var(--color-danger-line)]">
              {message}
            </p>
          </div>
        </div>
      }
    />
  );
}
