import { motion } from "framer-motion";
import { LiveTimer } from "./LiveTimer";
import { CompanyIcon } from "./CompanyIcon";
import { buildCompensationMeta } from "../lib/compensation";

interface JobRowProps {
    job: any;
    isSelected: boolean;
    onSelect: () => void;
    onApply: (type: "ai" | "manual") => void;
    onReject: () => void;
    isExiting?: "apply" | "reject";
    keyboardBlur?: boolean;
}

export function JobRow({ job, isSelected, onSelect, onApply, onReject, isExiting, keyboardBlur }: JobRowProps) {
    const compensationMeta = buildCompensationMeta(job);
    const levelLabel = typeof job.level === "string" ? job.level.charAt(0).toUpperCase() + job.level.slice(1) : "N/A";
    const scrapedAt = typeof job.scrapedAt === "number" ? job.scrapedAt : null;
    const postedAt = typeof job.postedAt === "number" ? job.postedAt : null;
    const timerClass = "text-xs font-medium text-slate-400 font-mono";

    return (
        <motion.div
            layout
            initial={false}
            animate={{
                opacity: 1,
                x: 0,
                backgroundColor: isSelected ? "rgba(30, 41, 59, 1)" : "rgba(15, 23, 42, 0)", // slate-800 vs transparent
                transition: keyboardBlur ? { duration: 0.12 } : { duration: 0.2 },
            }}
            exit={{
                x: isExiting === "apply" ? 100 : isExiting === "reject" ? -100 : 0,
                opacity: 0,
                transition: { duration: 0.16 }
            }}
            onClick={onSelect}
            data-job-id={job._id}
            className={`
        relative group flex items-center gap-3 px-4 pr-36 py-2 border-b border-slate-800 cursor-pointer transition-colors
        ${isSelected ? "bg-slate-800" : "hover:bg-slate-900"}
        ${keyboardBlur ? "blur-[1px] opacity-70" : ""}
      `}
        >
            {/* Selection Indicator */}
            <div className={`w-1 h-8 rounded-full transition-colors ${isSelected ? "bg-blue-500" : "bg-transparent"}`} />

            <div className="flex-1 min-w-0 grid grid-cols-[auto_4fr_3fr_2fr_3fr_3fr_3fr] gap-3 items-center">
                <CompanyIcon company={job.company ?? ""} size={32} />
                {/* Title & Company */}
                <div className="min-w-0">
                    <h3 className={`text-sm font-semibold truncate ${isSelected ? "text-white" : "text-slate-200"}`}>
                        {job.title}
                    </h3>
                    <p className="text-xs text-slate-500 truncate">{job.company}</p>
                </div>

                {/* Location & Badges */}
                <div className="flex items-center gap-2 min-w-0">
                    <span className="text-xs text-slate-400 truncate max-w-[110px]">{job.location}</span>
                    {job.remote && (
                        <span className="px-1.5 py-0.5 bg-emerald-500/10 text-emerald-400 text-[10px] font-medium rounded border border-emerald-500/20">
                            Remote
                        </span>
                    )}
                </div>

                {/* Level */}
                <div className="text-center">
                    <span className="px-2 py-0.5 text-[11px] font-semibold rounded-md border border-slate-800 bg-slate-900/70 text-slate-200">
                        {levelLabel}
                    </span>
                </div>

                {/* Salary */}
                <div className="text-right">
                    <span
                        className={`text-xs font-medium ${compensationMeta.isUnknown ? "text-slate-400" : "text-emerald-400"}`}
                        title={compensationMeta.reason}
                    >
                        {compensationMeta.display}
                    </span>
                </div>

                {/* Posted Date */}
                <div className="text-right">
                    <div className="flex flex-col items-end gap-0.5">
                        {postedAt ? (
                            <>
                                <span className="text-[10px] text-slate-500 font-medium">
                                    {new Date(postedAt).toLocaleString(undefined, {
                                        month: "short",
                                        day: "numeric",
                                        hour: "numeric",
                                        minute: "2-digit"
                                    })}
                                </span>
                                {(Date.now() - postedAt) < 5 * 24 * 60 * 60 * 1000 ? (
                                    <LiveTimer
                                        className={timerClass}
                                        startTime={postedAt}
                                        colorize={isSelected}
                                        warnAfterMs={24 * 60 * 60 * 1000}
                                        dangerAfterMs={3 * 24 * 60 * 60 * 1000}
                                        showAgo
                                        showSeconds={isSelected}
                                        dataTestId="posted-timer"
                                    />
                                ) : (
                                    <div className="h-4" />
                                )}
                            </>
                        ) : (
                            <span className="text-[11px] text-slate-600">Unknown</span>
                        )}
                    </div>
                </div>

                {/* Scraped */}
                <div className="text-right">
                    {scrapedAt ? (
                        <div className="flex flex-col items-end gap-0.5">
                            <span className="text-[10px] text-slate-500 font-medium">
                                {new Date(scrapedAt).toLocaleString(undefined, {
                                    month: "short",
                                    day: "numeric",
                                    hour: "numeric",
                                    minute: "2-digit"
                                })}
                            </span>
                            <LiveTimer
                                className={timerClass}
                                startTime={scrapedAt}
                                colorize={isSelected}
                                warnAfterMs={12 * 60 * 60 * 1000}
                                dangerAfterMs={48 * 60 * 60 * 1000}
                                showAgo
                                showSeconds={isSelected}
                                dataTestId="scraped-timer"
                            />
                        </div>
                    ) : (
                        <div className="flex flex-col items-end gap-0.5 text-right">
                            <span className="text-[11px] text-slate-600">Not scraped</span>
                        </div>
                    )}
                </div>
            </div>

            {/* Actions - Anchored on right, full-height for apply/reject when selected */}
            <div
                className={`absolute right-0 top-0 bottom-0 flex items-center gap-0 w-36 pl-2 transition-opacity ${isSelected ? "opacity-100 pointer-events-auto" : "opacity-0 pointer-events-none"
                    }`}
            >
                <button
                    onClick={(e) => { e.stopPropagation(); onApply("ai"); }}
                    disabled
                    className="w-14 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-slate-500 line-through border border-slate-700 bg-slate-900/60 rounded-l-sm shadow-sm shadow-slate-900/40 cursor-not-allowed"
                    title="AI Apply (a)"
                >
                    Apply
                </button>
                <button
                    onClick={(e) => { e.stopPropagation(); onReject(); }}
                    className="w-14 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-slate-200 border border-red-500/70 hover:border-red-400 hover:bg-red-500/10 rounded-r-sm shadow-sm shadow-red-900/40 transition-colors"
                    title="Reject (r)"
                >
                    Reject
                </button>
            </div>
        </motion.div>
    );
}
