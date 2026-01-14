/**
 * Centralized queue status styling utilities.
 * Extracted from JobBoard and QueuedUrlRow.
 */

export type QueueStatus =
  | "pending"
  | "processing"
  | "completed"
  | "failed"
  | "invalid";

/**
 * Tailwind classes for each queue status.
 */
export const STATUS_STYLES: Record<QueueStatus, string> = {
  pending: "bg-amber-500/10 text-amber-300 border-amber-500/20",
  processing: "bg-blue-500/10 text-blue-300 border-blue-500/20",
  completed: "bg-emerald-500/10 text-emerald-300 border-emerald-500/20",
  failed: "bg-red-500/10 text-red-300 border-red-500/20",
  invalid: "bg-slate-700/50 text-slate-300 border-slate-600/50",
};

/**
 * Resolve queue status to Tailwind classes.
 */
export const resolveQueueStatusClass = (status?: string): string => {
  return STATUS_STYLES[status as QueueStatus] ?? STATUS_STYLES.pending;
};
