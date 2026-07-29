/**
 * Baseball glyph used as the app mark. The ball takes `currentColor` so it
 * inherits from whatever it sits on; the seams stay Blue Jays red.
 */
export function BaseballMark({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 32 32" className={className} aria-hidden="true" focusable="false">
      <circle cx="16" cy="16" r="14.5" fill="currentColor" />
      <g
        fill="none"
        stroke="var(--color-brand-red)"
        strokeWidth="1.6"
        strokeLinecap="round"
      >
        <path d="M8.5 3.6C12.5 8 12.5 24 8.5 28.4" />
        <path d="M23.5 3.6C19.5 8 19.5 24 23.5 28.4" />
        <g strokeWidth="1.2">
          <path d="M9.2 8.8 12.6 7.9M10.9 12.8 14.3 12.4M10.9 19.2 14.3 19.6M9.2 23.2 12.6 24.1" />
          <path d="M22.8 8.8 19.4 7.9M21.1 12.8 17.7 12.4M21.1 19.2 17.7 19.6M22.8 23.2 19.4 24.1" />
        </g>
      </g>
    </svg>
  );
}
