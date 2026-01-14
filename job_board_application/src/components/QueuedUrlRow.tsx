import { motion } from "framer-motion";
import { LiveTimer } from "./LiveTimer";
import { CompanyIcon } from "./CompanyIcon";
import { extractCompanyLabel, toDisplayName } from "../lib/domainUtils";
import { STATUS_STYLES } from "../lib/statusStyles";
import type { QueueStatus } from "../lib/statusStyles";

export type { QueueStatus };

export interface QueuedUrlRowItem {
  _id: string;
  url: string;
  sourceUrl: string;
  provider?: string;
  urlType?: "listing" | "detail" | null;
  bucket?: number | null;
  siteId?: string | null;
  status: QueueStatus;
  createdAt: number;
  scheduledAt?: number | null;
  attempts?: number | null;
  completedAt?: number | null;
  lastError?: string | null;
}

interface QueuedUrlRowProps {
  item: QueuedUrlRowItem;
  index: number;
  isSelected: boolean;
  onSelect: () => void;
  keyboardBlur?: boolean;
  onResetRetries?: () => void;
  showCompletedAt?: boolean;
  showLastError?: boolean;
}

export function QueuedUrlRow({
  item,
  index,
  isSelected,
  onSelect,
  keyboardBlur,
  onResetRetries,
  showCompletedAt,
  showLastError,
}: QueuedUrlRowProps) {
  const statusStyle = STATUS_STYLES[item.status] ?? STATUS_STYLES.pending;
  const label =
    extractCompanyLabel(item.sourceUrl) ?? extractCompanyLabel(item.url) ?? "Company";
  const displayLabel = toDisplayName(label);
  const logoUrl = item.sourceUrl || item.url;
  const showExtras = Boolean(showCompletedAt || showLastError);
  const metaParts = [
    item.urlType ? `type: ${item.urlType}` : null,
    typeof item.bucket === "number" ? `bucket: ${item.bucket}` : null,
  ].filter(Boolean) as string[];
  const gridClassName = showExtras
    ? "grid-cols-[auto_auto_minmax(0,1fr)_auto] sm:grid-cols-[auto_auto_minmax(0,3.5fr)_minmax(0,1.5fr)_minmax(0,1.5fr)_minmax(0,1fr)_minmax(0,1fr)_minmax(0,1.5fr)_minmax(0,1.5fr)_minmax(0,1.8fr)_minmax(0,2.2fr)]"
    : "grid-cols-[auto_auto_minmax(0,1fr)_auto] sm:grid-cols-[auto_auto_minmax(0,4.5fr)_minmax(0,2fr)_minmax(0,2fr)_minmax(0,1fr)_minmax(0,1fr)_minmax(0,2fr)_minmax(0,2fr)]";

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

      <div className={`flex-1 min-w-0 grid ${gridClassName} gap-3 items-center`}>
        <div className="text-right text-xs text-slate-500 font-mono">{index + 1}</div>
        <CompanyIcon company={displayLabel} size={26} url={logoUrl} />
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
          <div>{item.provider ?? "—"}</div>
          <div className="text-[10px] text-slate-500">
            {metaParts.length ? metaParts.join(" • ") : "—"}
          </div>
        </div>

        <div className="hidden sm:block text-xs text-slate-400 truncate">
          {typeof item.scheduledAt === "number"
            ? new Date(item.scheduledAt).toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })
            : "—"}
        </div>

        <div className="hidden sm:block text-right text-xs text-slate-400">
          {item.attempts ?? 0}
        </div>

        <div className="flex flex-col items-end gap-1">
          <span className={`px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide rounded border ${statusStyle}`}>
            {item.status}
          </span>
          {item.status === "failed" && onResetRetries && (
            <button
              type="button"
              onClick={(event) => {
                event.stopPropagation();
                onResetRetries();
              }}
              className="px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide rounded border border-rose-400/40 text-rose-200 hover:text-white hover:border-rose-300 hover:bg-rose-500/20 transition-colors"
            >
              Reset retries
            </button>
          )}
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
        {showExtras && (
          <div className="hidden sm:block text-xs text-slate-400 truncate">
            {typeof item.completedAt === "number"
              ? new Date(item.completedAt).toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })
              : "—"}
          </div>
        )}
        {showExtras && (
          <div className="hidden sm:block text-xs text-slate-400 truncate" title={item.lastError ?? ""}>
            {item.lastError ?? "—"}
          </div>
        )}
      </div>
    </motion.div>
  );
}
