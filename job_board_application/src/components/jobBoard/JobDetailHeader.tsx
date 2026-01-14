import { LiveTimer } from "../LiveTimer";
import { formatLevelLabel } from "../../lib/dateFormatting";

interface CompensationMeta {
  display: string;
  isEstimated: boolean;
  isUnknown: boolean;
  reason?: string;
}

interface JobDetailHeaderProps {
  title: string;
  company?: string | null;
  location?: string | null;
  remote?: boolean;
  level?: string | null;
  postedAt?: number | null;
  postedAtUnknown?: boolean;
  postingFirstPublishedAt?: number | null;
  compensationMeta?: CompensationMeta | null;
  buildCompanyJobsUrl: (company: string) => string;
  onClose: () => void;
}

export function JobDetailHeader({
  title,
  company,
  location,
  remote,
  level,
  postedAt,
  postedAtUnknown,
  postingFirstPublishedAt,
  compensationMeta,
  buildCompanyJobsUrl,
  onClose,
}: JobDetailHeaderProps) {
  const compColorClass = compensationMeta?.isEstimated ? "text-slate-300" : "text-emerald-200";

  return (
    <div className="flex items-start justify-between px-4 py-3 border-b border-slate-800/50 bg-slate-900/20">
      <div className="min-w-0 pr-4">
        <h2 className="text-lg font-bold text-white leading-tight mb-1.5">{title}</h2>
        <div className="flex flex-wrap items-center gap-2 text-[11px] text-slate-300">
          {company && (
            <a
              href={buildCompanyJobsUrl(company)}
              target="_blank"
              rel="noreferrer"
              onClick={(event) => event.stopPropagation()}
              className="text-sm font-medium text-blue-400 mr-1 hover:text-blue-300 underline-offset-2 hover:underline"
            >
              {company}
            </a>
          )}
          {location && location !== "Unknown" && (
            <span
              className="px-2 py-0.5 rounded-md border border-slate-800 bg-slate-900/70"
              title={location}
            >
              {location}
            </span>
          )}
          {remote && (
            <span className="px-2 py-0.5 rounded-md border border-emerald-600/60 bg-emerald-500/10 text-emerald-300 font-semibold">
              Remote
            </span>
          )}
          {level && (
            <span className="px-2 py-0.5 rounded-md border border-slate-800 bg-slate-900/70">
              {formatLevelLabel(level)}
            </span>
          )}
          {compensationMeta && !compensationMeta.isUnknown && (
            <span className="px-2 py-0.5 rounded-md border border-slate-800 bg-slate-900/70">
              <span className={compColorClass} title={compensationMeta.reason}>
                {compensationMeta.display}
              </span>
            </span>
          )}
          {typeof postedAt === "number" && (
            <span className="flex items-center gap-1 text-xs text-slate-500">
              Posted{" "}
              <LiveTimer
                startTime={postedAt}
                showAgo
                showSeconds={false}
                className={postedAtUnknown ? "text-slate-300" : "text-emerald-300"}
              />
            </span>
          )}
          {typeof postingFirstPublishedAt === "number" &&
            postingFirstPublishedAt !== postedAt && (
              <span
                className="px-1.5 py-0.5 rounded text-[10px] bg-slate-800/50 text-slate-400 border border-slate-700"
                title={`Originally posted: ${new Date(postingFirstPublishedAt).toLocaleDateString()}`}
              >
                First posted{" "}
                {new Date(postingFirstPublishedAt).toLocaleDateString(undefined, {
                  month: "short",
                  day: "numeric",
                  year: "numeric",
                })}
              </span>
            )}
        </div>
      </div>
      <button
        onClick={onClose}
        className="shrink-0 p-2 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition-colors"
        aria-label="Close job details"
      >
        <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
        </svg>
      </button>
    </div>
  );
}
