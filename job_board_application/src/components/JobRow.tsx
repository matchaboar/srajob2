import { motion } from "framer-motion";
import type { MouseEvent } from "react";
import { LiveTimer } from "./LiveTimer";
import { CompanyIcon } from "./CompanyIcon";
import { buildCompensationMeta } from "../lib/compensation";
import { formatShortDate, formatDaysAgo, formatLevelLabel } from "../lib/dateFormatting";
import { StatusTracker } from "./StatusTracker";
import { JobRowCompanyPill } from "./jobRow/JobRowCompanyPill";
import { JobRowLevelPill } from "./jobRow/JobRowLevelPill";
import { JobRowSalary } from "./jobRow/JobRowSalary";

export type JobRowVariant = 'default' | 'applied' | 'rejected';

interface JobRowProps {
    job: any;
    isSelected: boolean;
    onSelect: () => void;
    isExiting?: "apply" | "reject";
    keyboardBlur?: boolean;
    variant?: JobRowVariant;
    getCompanyJobsUrl?: (companyName: string) => string;
    queuedAt?: number | null;
    showQueuedSince?: boolean;
}

export function JobRow({
    job,
    isSelected,
    onSelect,
    isExiting,
    keyboardBlur,
    variant = 'default',
    getCompanyJobsUrl,
    queuedAt,
    showQueuedSince
}: JobRowProps) {
    const compensationMeta = buildCompensationMeta(job);
    const levelLabel = formatLevelLabel(job.level);
    const scrapedAt = typeof job.scrapedAt === "number" ? job.scrapedAt : null;
    const postedAt = typeof job.postedAt === "number" ? job.postedAt : null;
    const companyName = typeof job.company === "string" ? job.company : "";
    const companyUrl = getCompanyJobsUrl && companyName ? getCompanyJobsUrl(companyName) : "";
    const handleCompanyClick = (event: MouseEvent) => {
        event.stopPropagation();
    };

    // Applied/Rejected specific dates
    const appliedAt = typeof job.appliedAt === "number" ? job.appliedAt : null;
    const rejectedAt = typeof job.rejectedAt === "number" ? job.rejectedAt : appliedAt; // Fallback to appliedAt if rejectedAt missing

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
                    <span className="text-xs text-slate-400 truncate max-w-[160px]" title={job.location}>
                        {job.location || "—"}
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
                            showQueuedSince ? (
                                queuedAt ? (
                                    <LiveTimer
                                        startTime={queuedAt}
                                        colorize={isSelected}
                                        warnAfterMs={6 * 60 * 60 * 1000}
                                        dangerAfterMs={24 * 60 * 60 * 1000}
                                        showAgo
                                        showSeconds={isSelected}
                                        className="text-[10px] font-mono text-slate-400 truncate"
                                    />
                                ) : (
                                    <span className="text-[11px] text-slate-600">Unknown</span>
                                )
                            ) : (
                                postedAt ? (
                                    <LiveTimer
                                        startTime={postedAt}
                                        colorize={isSelected}
                                        warnAfterMs={24 * 60 * 60 * 1000} // e.g. warn after 1 day
                                        dangerAfterMs={3 * 24 * 60 * 60 * 1000} // e.g. danger after 3 days
                                        showSeconds={isSelected}
                                        showAgo
                                        className="text-[10px] font-mono text-slate-500 truncate"
                                    />
                                ) : (
                                    <span className="text-[11px] text-slate-600">Unknown</span>
                                )
                            )
                        )}
                        {variant === 'applied' && appliedAt && (
                            <span className="text-[10px] text-slate-500 font-medium truncate">
                                {formatShortDate(appliedAt)} • {formatDaysAgo(appliedAt)}
                            </span>
                        )}
                        {variant === 'rejected' && rejectedAt && (
                            <span className="text-[10px] text-slate-500 font-medium truncate">
                                {formatShortDate(rejectedAt)} • {formatDaysAgo(rejectedAt)}
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

        </motion.div>
    );
}
