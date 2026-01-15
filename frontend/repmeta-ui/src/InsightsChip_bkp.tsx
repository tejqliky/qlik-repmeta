import type { MouseEventHandler } from "react";

export type InsightsChipProps = {
  /** Job status as returned by backend (created|pending|running|succeeded|failed). */
  status?: string | null;
  /** Optional click handler (e.g., open Insights modal). */
  onClick?: MouseEventHandler<HTMLButtonElement>;
};

function normalizeStatus(status?: string | null) {
  const s = (status ?? "created").toLowerCase().trim();
  return s || "created";
}

function labelFor(status?: string | null) {
  const s = normalizeStatus(status);
  if (s === "succeeded") return "Insights Ready";
  if (s === "failed") return "Insights Failed";
  if (s === "running" || s === "pending") return "Insights Running";
  // created or unknown
  return "Insights Not Started";
}

function classesFor(status?: string | null, clickable?: boolean) {
  const s = normalizeStatus(status);

  const base =
    "inline-flex items-center gap-2 rounded-full border px-3 h-7 text-[11px] font-semibold transition-colors whitespace-nowrap";
  const interactivity = clickable ? "cursor-pointer hover:opacity-95" : "cursor-default";

  if (s === "succeeded") return `${base} ${interactivity} bg-green-50 border-green-200 text-green-800`;
  if (s === "failed") return `${base} ${interactivity} bg-red-50 border-red-200 text-red-800`;
  if (s === "running" || s === "pending")
    return `${base} ${interactivity} bg-amber-50 border-amber-200 text-amber-800`;
  return `${base} ${interactivity} bg-gray-50 border-gray-200 text-gray-700`;
}

/**
 * Named export (so `import { InsightsChip } from "./InsightsChip"` works)
 */
export function InsightsChip({ status, onClick }: InsightsChipProps) {
  const clickable = typeof onClick === "function";
  const s = normalizeStatus(status);

  return (
    <button
      type="button"
      onClick={onClick}
      disabled={!clickable}
      className={classesFor(s, clickable)}
      title={clickable ? "View AI Insights" : undefined}
    >
      {s === "running" || s === "pending" ? (
        <span className="inline-block h-2 w-2 rounded-full bg-amber-500 animate-pulse" />
      ) : s === "succeeded" ? (
        <span className="inline-block h-2 w-2 rounded-full bg-green-600" />
      ) : s === "failed" ? (
        <span className="inline-block h-2 w-2 rounded-full bg-red-600" />
      ) : (
        <span className="inline-block h-2 w-2 rounded-full bg-gray-400" />
      )}
      {labelFor(s)}
    </button>
  );
}

/**
 * Default export (so `import InsightsChip from "./InsightsChip"` also works)
 */
export default InsightsChip;
