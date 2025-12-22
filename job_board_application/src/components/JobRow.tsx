import { motion } from "framer-motion";
import type { MouseEvent } from "react";
import { LiveTimer } from "./LiveTimer";
import { CompanyIcon } from "./CompanyIcon";
import { buildCompensationMeta } from "../lib/compensation";
import { Keycap } from "./Keycap";
import { StatusTracker } from "./StatusTracker";
import { JobRowCompanyPill } from "./jobRow/JobRowCompanyPill";
import { JobRowLevelPill } from "./jobRow/JobRowLevelPill";
import { JobRowSalary } from "./jobRow/JobRowSalary";

export type JobRowVariant = 'default' | 'applied' | 'rejected';

interface JobRowProps {
    job: any;
    groupedLabel?: string;
    isSelected: boolean;
    onSelect: () => void;
    onApply: (type: "ai" | "manual") => void;
    onReject: () => void;
    isExiting?: "apply" | "reject";
    keyboardBlur?: boolean;
    showHotkeys?: boolean;
    variant?: JobRowVariant;
    getCompanyJobsUrl?: (companyName: string) => string;
}

export function JobRow({
    job,
    groupedLabel,
    isSelected,
    onSelect,
    onApply,
    onReject,
    isExiting,
    keyboardBlur,
    showHotkeys,
    variant = 'default',
    getCompanyJobsUrl
}: JobRowProps) {
    const compensationMeta = buildCompensationMeta(job);
    const levelLabel = typeof job.level === "string" ? job.level.charAt(0).toUpperCase() + job.level.slice(1) : "N/A";
    const scrapedAt = typeof job.scrapedAt === "number" ? job.scrapedAt : null;
    const postedAt = typeof job.postedAt === "number" ? job.postedAt : null;
    const displayLocation = groupedLabel ?? job.location;
    const companyName = typeof job.company === "string" ? job.company : "";
    const companyUrl = getCompanyJobsUrl && companyName ? getCompanyJobsUrl(companyName) : "";
    const handleCompanyClick = (event: MouseEvent) => {
        event.stopPropagation();
    };

    // Applied/Rejected specific dates
    const appliedAt = typeof job.appliedAt === "number" ? job.appliedAt : null;
    const rejectedAt = typeof job.rejectedAt === "number" ? job.rejectedAt : appliedAt; // Fallback to appliedAt if rejectedAt missing

    const formatDate = (date: number) => {
        return new Date(date).toLocaleDateString(undefined, {
            month: 'short',
            day: 'numeric',
        });
    };

    const formatDaysAgo = (timestamp: number) => {
        const days = Math.max(0, Math.floor((Date.now() - timestamp) / (1000 * 60 * 60 * 24)));
        return `${days}d ago`;
    };

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
        relative group flex items-start sm:items-center gap-3 px-3 sm:px-4 py-2 sm:py-1 border-b border-slate-800 cursor-pointer transition-colors
        ${isSelected ? "bg-slate-800" : "hover:bg-slate-900"}
        ${keyboardBlur ? "blur-[1px] opacity-70" : ""}
      `}
        >
            {/* Selection Indicator */}
            <div className={`w-1 h-8 rounded-full transition-colors ${variant === 'rejected' ? (isSelected ? "bg-red-500" : "bg-transparent") :
                (isSelected ? "bg-blue-500" : "bg-transparent")
                }`} />

            <div className={`flex-1 min-w-0 grid gap-3 items-center ${variant === 'applied' ? 'grid-cols-[auto_5fr_3fr] sm:grid-cols-[auto_5fr_5fr_3fr_2fr_2fr]' : 'grid-cols-[auto_6fr_3fr] sm:grid-cols-[auto_8fr_3fr_2fr_2fr_2fr]'}`}>
                <div className="order-1">
                    {companyUrl ? (
                        <a
                            href={companyUrl}
                            target="_blank"
                            rel="noreferrer"
                            onClick={handleCompanyClick}
                            className="inline-flex"
                            aria-label={`View jobs for ${companyName}`}
                        >
                            <CompanyIcon company={companyName} size={28} url={job.url} />
                        </a>
                    ) : (
                        <CompanyIcon company={companyName} size={28} url={job.url} />
                    )}
                </div>
                {/* Title & Pills */}
                <div className="min-w-0 flex items-center gap-2 overflow-hidden order-2">
                    <h3 className={`text-sm font-semibold ${isSelected ? "text-white" : "text-slate-200"} truncate shrink-0 max-w-[50%]`}>
                        {job.title}
                    </h3>
                    <div className="flex items-center gap-1.5 shrink-0 overflow-hidden">
                        {showHotkeys && variant === 'default' && (
                            <div className="flex items-center gap-1 mr-1">
                                <Keycap label="A" className="text-[9px] h-4 min-w-[16px] px-1 bg-slate-700 border-slate-600 text-slate-300 shadow-sm" />
                                <Keycap label="R" className="text-[9px] h-4 min-w-[16px] px-1 bg-slate-700 border-slate-600 text-slate-300 shadow-sm" />
                            </div>
                        )}
                        <JobRowCompanyPill
                            company={companyName}
                            href={companyUrl || undefined}
                            onClick={handleCompanyClick}
                            title={companyUrl ? `View jobs for ${companyName}` : undefined}
                        />
                        <JobRowLevelPill label={levelLabel} />
                        {variant === 'rejected' && (
                            <span className="shrink-0 px-1.5 py-0.5 bg-red-500/10 text-red-300 text-[10px] font-medium rounded border border-red-500/20">
                                Rejected
                            </span>
                        )}
                    </div>
                </div>

                {/* Location (desktop only) */}
                <div className={`hidden sm:flex items-center gap-2 min-w-0 ${variant === 'applied' ? 'order-4' : 'order-3'}`}>
                    <span className="text-xs text-slate-400 truncate max-w-[160px]" title={job.location || displayLocation}>
                        {displayLocation || "—"}
                    </span>
                    {job.remote && (
                        <span className="shrink-0 px-1.5 py-0.5 bg-emerald-500/10 text-emerald-400 text-[9px] font-bold uppercase tracking-wide rounded border border-emerald-500/20">
                            Remote
                        </span>
                    )}
                </div>

                {/* Salary */}
                <div className={`text-right min-w-0 ${variant === 'applied' ? 'order-5' : 'order-4'}`}>
                    <JobRowSalary meta={compensationMeta} className="text-sm sm:text-xs" />
                </div>

                {/* Col 5: Posted (Default) / Applied (Applied) / Rejected (Rejected) */}
                <div className={`hidden sm:block text-right min-w-0 ${variant === 'applied' ? 'order-6' : 'order-5'}`}>
                    <div className="flex flex-col items-end gap-0.5">
                        {variant === 'default' && (
                            postedAt ? (
                                <span className="text-[10px] text-slate-500 font-medium truncate">
                                    {new Date(postedAt).toLocaleDateString(undefined, {
                                        month: "short",
                                        day: "numeric",
                                    })}
                                </span>
                            ) : (
                                <span className="text-[11px] text-slate-600">Unknown</span>
                            )
                        )}
                        {variant === 'applied' && appliedAt && (
                            <span className="text-[10px] text-slate-500 font-medium truncate">
                                {formatDate(appliedAt)} • {formatDaysAgo(appliedAt)}
                            </span>
                        )}
                        {variant === 'rejected' && rejectedAt && (
                            <span className="text-[10px] text-slate-500 font-medium truncate">
                                {formatDate(rejectedAt)} • {formatDaysAgo(rejectedAt)}
                            </span>
                        )}
                    </div>
                </div>

                {/* Col 6: Scraped (Default) / Status (Applied) / Empty (Rejected) */}
                <div className={`hidden sm:block min-w-0 ${variant === 'applied' ? 'order-3' : 'order-6 text-right'}`}>
                    {variant === 'default' && (
                        scrapedAt ? (
                            <div className="flex flex-col items-end gap-0.5">
                                <LiveTimer
                                    startTime={scrapedAt}
                                    colorize={isSelected}
                                    warnAfterMs={12 * 60 * 60 * 1000}
                                    dangerAfterMs={48 * 60 * 60 * 1000}
                                    showAgo
                                    showSeconds={isSelected}
                                    className="text-[10px] font-mono text-slate-400 truncate"
                                />
                            </div>
                        ) : (
                            <div className="flex flex-col items-end gap-0.5 text-right">
                                <span className="text-[11px] text-slate-600">Not scraped</span>
                            </div>
                        )
                    )}
                    {variant === 'applied' && (
                        <div className="flex min-w-0">
                            <StatusTracker
                                status={job.workerStatus || (job.userStatus === 'applied' ? 'Applied' : null)}
                                updatedAt={job.workerUpdatedAt || job.appliedAt}
                                compact
                            />
                        </div>
                    )}
                    {variant === 'rejected' && (
                        <div />
                    )}
                </div>
            </div>

            {/* Mobile inline actions */}
            {isSelected && variant === 'default' && (
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
        </motion.div>
    );
}
