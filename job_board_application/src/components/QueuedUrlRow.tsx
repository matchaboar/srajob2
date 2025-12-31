import { motion } from "framer-motion";
import { LiveTimer } from "./LiveTimer";

export type QueueStatus = "pending" | "processing" | "completed" | "failed" | "invalid";

export interface QueuedUrlRowItem {
  _id: string;
  url: string;
  sourceUrl: string;
  provider?: string;
  status: QueueStatus;
  createdAt: number;
  scheduledAt?: number | null;
  attempts?: number | null;
}

interface QueuedUrlRowProps {
  item: QueuedUrlRowItem;
  index: number;
  isSelected: boolean;
  onSelect: () => void;
  keyboardBlur?: boolean;
}

const STATUS_STYLES: Record<QueueStatus, string> = {
  pending: "bg-amber-500/10 text-amber-300 border-amber-500/20",
  processing: "bg-blue-500/10 text-blue-300 border-blue-500/20",
  completed: "bg-emerald-500/10 text-emerald-300 border-emerald-500/20",
  failed: "bg-red-500/10 text-red-300 border-red-500/20",
  invalid: "bg-slate-700/50 text-slate-300 border-slate-600/50",
};

export function QueuedUrlRow({ item, index, isSelected, onSelect, keyboardBlur }: QueuedUrlRowProps) {
  const statusStyle = STATUS_STYLES[item.status] ?? STATUS_STYLES.pending;

  return (
    <motion.div
      layout
      initial={false}
      animate={{
        opacity: 1,
        x: 0,
        backgroundColor: isSelected ? "rgba(30, 41, 59, 1)" : "rgba(15, 23, 42, 0)",
        transition: keyboardBlur ? { duration: 0.12 } : { duration: 0.2 },
      }}
      exit={{
        x: 0,
        opacity: 0,
        transition: { duration: 0.16 },
      }}
      onClick={onSelect}
      data-job-id={item._id}
      className={
        `relative group flex items-center gap-3 px-3 sm:px-4 py-2 border-b border-slate-800 cursor-pointer transition-colors ` +
        `${isSelected ? "bg-slate-800" : "hover:bg-slate-900"} ` +
        `${keyboardBlur ? "blur-[1px] opacity-70" : ""}`
      }
    >
      <div className={`w-1 h-8 rounded-full transition-colors ${isSelected ? "bg-amber-400" : "bg-transparent"}`} />

      <div className="flex-1 min-w-0 grid grid-cols-[auto_minmax(0,1fr)_auto] sm:grid-cols-[auto_minmax(0,5fr)_minmax(0,2fr)_minmax(0,2fr)_minmax(0,1fr)_minmax(0,1fr)_minmax(0,2fr)_minmax(0,2fr)] gap-3 items-center">
        <div className="text-right text-xs text-slate-500 font-mono">{index + 1}</div>
        <div className="min-w-0">
          <a
            href={item.url}
            target="_blank"
            rel="noreferrer"
            className="text-sm font-semibold text-slate-200 hover:text-white truncate block"
            title={item.url}
            onClick={(event) => event.stopPropagation()}
          >
            {item.url}
          </a>
          <div className="text-[10px] text-slate-500 truncate" title={item.sourceUrl}>
            {item.sourceUrl}
          </div>
        </div>

        <div className="hidden sm:block text-xs text-slate-400 truncate" title={item.provider ?? ""}>
          {item.provider ?? "—"}
        </div>

        <div className="hidden sm:block text-xs text-slate-400 truncate">
          {typeof item.scheduledAt === "number"
            ? new Date(item.scheduledAt).toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })
            : "—"}
        </div>

        <div className="hidden sm:block text-right text-xs text-slate-400">
          {item.attempts ?? 0}
        </div>

        <div className="flex justify-end">
          <span className={`px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide rounded border ${statusStyle}`}>
            {item.status}
          </span>
        </div>

        <div className="hidden sm:flex justify-end">
          <LiveTimer
            startTime={item.createdAt}
            colorize={isSelected}
            warnAfterMs={6 * 60 * 60 * 1000}
            dangerAfterMs={24 * 60 * 60 * 1000}
            showAgo
            showSeconds={isSelected}
            className="text-[10px] font-mono text-slate-400 truncate"
          />
        </div>
      </div>
    </motion.div>
  );
}
