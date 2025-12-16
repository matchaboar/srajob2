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
        relative group flex items-start sm:items-center gap-3 px-3 sm:px-4 pr-4 sm:pr-36 py-3 sm:py-2 border-b border-slate-800 cursor-pointer transition-colors
        ${isSelected ? "bg-slate-800" : "hover:bg-slate-900"}
        ${keyboardBlur ? "blur-[1px] opacity-70" : ""}
      `}
        >
            {/* Selection Indicator */}
            <div className={`w-1 h-8 rounded-full transition-colors ${isSelected ? "bg-blue-500" : "bg-transparent"}`} />
            <div className="flex-1 min-w-0 grid grid-cols-[auto_6fr_3fr] sm:grid-cols-[auto_4fr_3fr_2fr_3fr_3fr_3fr] gap-3 items-start sm:items-center">
                <CompanyIcon company={job.company ?? ""} size={32} />
                {/* Title & meta */}
                <div className="min-w-0 space-y-1">
                    <h3 className={`text-sm font-semibold leading-snug ${isSelected ? "text-white" : "text-slate-200"} line-clamp-2`}>
                        {job.title}
                    </h3>
                    <div className="flex flex-wrap items-center gap-2 text-[11px] text-slate-500">
                        <span className="truncate max-w-[12rem]">{job.company}</span>
                        {job.remote && (
                        <span className="px-1.5 py-0.5 bg-emerald-500/10 text-emerald-400 text-[10px] font-semibold rounded border border-emerald-500/20">
                            Remote
                        </span>
                    )}
                        <span className="px-2 py-0.5 text-[10px] font-semibold rounded-md border border-slate-800 bg-slate-900/70 text-slate-200">
                            {levelLabel}
                        </span>
                    </div>
                </div>

                {/* Salary (kept on mobile) */}
                <div className="text-right">
                    <span
                        className={`text-sm sm:text-xs font-semibold ${compensationMeta.isUnknown ? "text-slate-400" : "text-emerald-400"}`}
                        title={compensationMeta.reason}
                    >
                        {compensationMeta.display}
                    </span>
                    <div className="flex sm:hidden flex-col items-end gap-0.5 mt-1 text-[10px] text-slate-500">
                        {scrapedAt ? (
                            <>
                                <span className="font-medium">
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
                                    dataTestId="scraped-timer-mobile"
                                />
                            </>
                        ) : postedAt ? (
                            <>
                                <span className="font-medium">
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
                                        dataTestId="posted-timer-mobile"
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

                {/* Level column (desktop) */}
                <div className="hidden sm:flex justify-center">
                    <span className="px-2 py-0.5 text-[11px] font-semibold rounded-md border border-slate-800 bg-slate-900/70 text-slate-200">
                        {levelLabel}
                    </span>
                </div>

                {/* Location (desktop only) */}
                <div className="hidden sm:flex items-center gap-2 min-w-0">
                    <span className="text-xs text-slate-400 truncate max-w-[120px]">{job.location || "—"}</span>
                </div>

                {/* Posted (desktop) */}
                <div className="hidden sm:block text-right">
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

                {/* Scraped (desktop) */}
                <div className="hidden sm:block text-right">
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

            {/* Mobile inline actions */}
            {isSelected && (
                <div className="sm:hidden mt-3 ml-10 flex gap-2">
                    <button
                        onClick={(e) => { e.stopPropagation(); onApply("ai"); }}
                        disabled
                        className="px-3 py-1 text-[11px] font-semibold uppercase tracking-wide text-slate-500 line-through border border-slate-700 bg-slate-900/70 rounded shadow-sm cursor-not-allowed"
                        title="AI Apply (a)"
                    >
                        Apply
                    </button>
                    <button
                        onClick={(e) => { e.stopPropagation(); onReject(); }}
                        className="px-3 py-1 text-[11px] font-semibold uppercase tracking-wide text-slate-200 border border-red-500/70 hover:border-red-400 hover:bg-red-500/10 rounded shadow-sm transition-colors"
                        title="Reject (r)"
                    >
                        Reject
                    </button>
                </div>
            )}

            {/* Desktop action rail */}
            <div
                className={`hidden sm:flex absolute right-0 top-0 bottom-0 items-center gap-0 w-36 pl-2 transition-opacity ${isSelected ? "opacity-100 pointer-events-auto" : "opacity-0 pointer-events-none"}`}
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
