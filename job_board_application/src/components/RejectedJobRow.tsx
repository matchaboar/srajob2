import { motion } from "framer-motion";
import { CompanyIcon } from "./CompanyIcon";
import { buildCompensationMeta } from "../lib/compensation";

interface RejectedJobRowProps {
  job: any;
  isSelected: boolean;
  onSelect: () => void;
}

export function RejectedJobRow({ job, isSelected, onSelect }: RejectedJobRowProps) {
  const compensationMeta = buildCompensationMeta(job);
  const levelLabel = typeof job.level === "string" ? job.level.charAt(0).toUpperCase() + job.level.slice(1) : "N/A";

  const formatDate = (date: number) =>
    new Date(date).toLocaleDateString(undefined, {
      month: "short",
      day: "numeric",
    });

  const formatDaysAgo = (timestamp: number) => {
    const days = Math.max(0, Math.floor((Date.now() - timestamp) / (1000 * 60 * 60 * 24)));
    return `${days}d ago`;
  };

  const rejectedDate = job.rejectedAt ?? job.appliedAt;

  return (
    <motion.div
      layout
      initial={{ opacity: 0 }}
      animate={{
        opacity: 1,
        backgroundColor: isSelected ? "rgba(30, 41, 59, 1)" : "rgba(15, 23, 42, 0)",
      }}
      onClick={onSelect}
      className={`
        group flex items-center gap-3 px-3 sm:px-4 py-3 sm:py-2 border-b border-slate-800 cursor-pointer transition-colors
        ${isSelected ? "bg-slate-800" : "hover:bg-slate-900"}
      `}
    >
      <div className={`w-1 h-8 rounded-full transition-colors ${isSelected ? "bg-red-500" : "bg-transparent"}`} />

      <div className="flex-1 min-w-0 grid grid-cols-[auto_6fr_3fr] sm:grid-cols-[auto_8fr_3fr_2fr_2fr_2fr] gap-3 items-center">
        <CompanyIcon company={job.company ?? ""} size={32} url={job.url} />

        {/* Title & Pills */}
        <div className="min-w-0 flex items-center gap-2 overflow-hidden">
          <h3 className={`text-sm font-semibold ${isSelected ? "text-white" : "text-slate-200"} truncate shrink-0 max-w-[50%]`}>
            {job.title}
          </h3>
          <div className="flex items-center gap-1.5 shrink-0 overflow-hidden">
            <span className="px-2 py-0.5 text-[10px] font-medium rounded-md border border-slate-700 bg-slate-800/50 text-slate-300 truncate max-w-[12rem]">
              {job.company}
            </span>
            <span className="px-1.5 py-0.5 text-[10px] font-bold uppercase tracking-wide rounded-md border border-slate-800 bg-slate-900/70 text-slate-400 shrink-0">
              {levelLabel}
            </span>
          </div>
        </div>

        {/* Location */}
        <div className="hidden sm:flex items-center gap-2 min-w-0">
          <span className="text-xs text-slate-400 truncate max-w-[160px]">
            {job.location || "—"}
          </span>
          {job.remote && (
            <span className="shrink-0 px-1.5 py-0.5 bg-red-500/10 text-red-300 text-[10px] font-medium rounded border border-red-500/20">
              Rejected
            </span>
          )}
        </div>

        {/* Salary */}
        <div className="text-right min-w-0 hidden sm:block">
          <span
            className={`text-xs font-bold ${compensationMeta.isUnknown ? "text-slate-500" : "text-emerald-400"} truncate block`}
            title={compensationMeta.reason}
          >
            {compensationMeta.display}
          </span>
        </div>

        {/* Rejected Date */}
        <div className="hidden sm:block text-right min-w-0">
          <div className="flex flex-col items-end gap-0.5">
            <span className="text-[10px] text-slate-500 font-medium truncate">
              {formatDate(rejectedDate)} • {formatDaysAgo(rejectedDate)}
            </span>
          </div>
        </div>

        {/* Empty Column for Alignment */}
        <div className="hidden sm:block" />
      </div>
    </motion.div>
  );
}
