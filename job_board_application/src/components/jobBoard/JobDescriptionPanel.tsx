import { JobDetailHeader } from "./JobDetailHeader";
import { DescriptionSection, type DescriptionState } from "./DescriptionSection";
import { MetadataSection } from "./MetadataSection";
import { ScrapeInfoSection, type QueueDurationInfo } from "./ScrapeInfoSection";
import { ParsingWorkflowsSection, type ParsingStep } from "./ParsingWorkflowsSection";
import { StatusTracker } from "../StatusTracker";

type PanelVariant = "jobs" | "applied" | "rejected";

interface CompensationMeta {
  display: string;
  isEstimated: boolean;
  isUnknown: boolean;
  reason?: string;
}

interface JobDescriptionPanelProps {
  variant: PanelVariant;
  title: string;
  company?: string | null;
  location?: string | null;
  remote?: boolean;
  level?: string | null;
  postedAt?: number | null;
  postedAtUnknown?: boolean;
  postingFirstPublishedAt?: number | null;
  compensationMeta?: CompensationMeta | null;
  url?: string | null;
  jobId: string;
  description: string;
  descriptionWordCount?: number | null;
  descriptionState?: DescriptionState;
  metadata?: string | null;
  // Jobs tab specific
  scrapedAt?: number | null;
  scrapedWith?: string | null;
  workflowName?: string | null;
  scrapedCostMilliCents?: number | null;
  sourceUrl?: string | null;
  scrapeUrl?: string | null;
  queueDurationInfos?: QueueDurationInfo[];
  applicationCount?: number | null;
  parsingSteps?: ParsingStep[];
  parseNotes?: string;
  // Applied tab specific
  workerStatus?: string | null;
  userStatus?: string | null;
  workerUpdatedAt?: number | null;
  appliedAt?: number | null;
  // Callbacks
  buildCompanyJobsUrl: (company: string) => string;
  onClose: () => void;
  onApply?: () => void;
  onReadMore?: () => void;
  onCopyLink?: () => void;
}

export function JobDescriptionPanel({
  variant,
  title,
  company,
  location,
  remote,
  level,
  postedAt,
  postedAtUnknown,
  postingFirstPublishedAt,
  compensationMeta,
  url,
  jobId,
  description,
  descriptionWordCount,
  descriptionState,
  metadata,
  // Jobs tab specific
  scrapedAt,
  scrapedWith,
  workflowName,
  scrapedCostMilliCents,
  sourceUrl,
  scrapeUrl,
  queueDurationInfos,
  applicationCount,
  parsingSteps,
  parseNotes,
  // Applied tab specific
  workerStatus,
  userStatus,
  workerUpdatedAt,
  appliedAt,
  // Callbacks
  buildCompanyJobsUrl,
  onClose,
  onApply,
  onReadMore,
  onCopyLink,
}: JobDescriptionPanelProps) {
  const buttonColors = {
    jobs: "bg-emerald-400 hover:bg-emerald-300 border-emerald-500 shadow-emerald-900/30",
    applied: "bg-blue-400 hover:bg-blue-300 border-blue-500 shadow-blue-900/30",
    rejected: "bg-red-400 hover:bg-red-300 border-red-500 shadow-red-900/30",
  };

  const buttonLabel = variant === "jobs" ? "Direct Apply" : "View Job Posting";

  return (
    <div className="w-full sm:w-[50rem] border-l border-slate-800 bg-slate-950 flex flex-col shadow-2xl max-h-[85vh] sm:max-h-none sm:h-auto fixed sm:static inset-x-0 bottom-0 sm:bottom-auto sm:inset-auto z-40 sm:z-auto rounded-t-2xl sm:rounded-none">
      <JobDetailHeader
        title={title}
        company={company}
        location={location}
        remote={remote}
        level={level}
        postedAt={postedAt}
        postedAtUnknown={postedAtUnknown}
        postingFirstPublishedAt={variant === "jobs" ? postingFirstPublishedAt : undefined}
        compensationMeta={compensationMeta}
        buildCompanyJobsUrl={buildCompanyJobsUrl}
        onClose={onClose}
      />

      <div className="flex-1 overflow-y-auto custom-scrollbar">
        <div className="p-3 space-y-2">
          {/* Status Tracker - Applied tab only */}
          {variant === "applied" && (
            <div className="flex justify-center w-full pb-2">
              <StatusTracker
                status={workerStatus || (userStatus === "applied" ? "Applied" : null)}
                updatedAt={workerUpdatedAt || appliedAt}
              />
            </div>
          )}

          {/* Action buttons */}
          <div className="flex gap-2">
            {url && (
              <button
                onClick={() => {
                  if (variant === "jobs" && onApply) {
                    onApply();
                  } else {
                    window.open(url, "_blank");
                  }
                }}
                className={`flex-1 px-4 py-2.5 text-sm font-semibold uppercase tracking-wide text-slate-900 ${buttonColors[variant]} border shadow-lg transition-transform active:scale-[0.99]`}
              >
                {buttonLabel}
              </button>
            )}
            {variant === "jobs" && (
              <button
                onClick={() => {}}
                disabled
                className="px-4 py-2.5 text-sm font-semibold uppercase tracking-wide text-slate-500 line-through border border-slate-700 bg-slate-900/70 cursor-not-allowed"
              >
                Apply with AI
              </button>
            )}
          </div>

          {/* Job URL link - Jobs tab only */}
          {variant === "jobs" && url && (
            <div className="rounded-lg border border-slate-800/70 bg-slate-900/50 px-3 py-1.5 flex items-center gap-2">
              <a
                href={url}
                target="_blank"
                rel="noreferrer"
                className="text-xs text-blue-300 hover:text-blue-200 underline-offset-2 break-all truncate"
              >
                {url}
              </a>
            </div>
          )}

          {/* Description section */}
          <DescriptionSection
            description={description}
            wordCount={descriptionWordCount}
            descriptionState={variant === "rejected" ? undefined : descriptionState}
            onReadMore={variant === "rejected" ? undefined : onReadMore}
            onCopyLink={onCopyLink}
            jobId={jobId}
            maxHeight={variant === "jobs" ? "max-h-72" : "max-h-[60vh]"}
          />

          {/* Metadata section - Jobs and Applied tabs */}
          {(variant === "jobs" || variant === "applied") && metadata && (
            <MetadataSection metadata={metadata} />
          )}

          {/* Scrape Info - Jobs tab only */}
          {variant === "jobs" && (
          <ScrapeInfoSection
            scrapedAt={scrapedAt}
            scrapedWith={scrapedWith}
            workflowName={workflowName}
            scrapedCostMilliCents={scrapedCostMilliCents}
            sourceUrl={sourceUrl}
            scrapeUrl={scrapeUrl}
            queueDurationInfos={queueDurationInfos}
          />
          )}

          {/* Applications count - Jobs tab only */}
          {variant === "jobs" && (
            <div className="rounded-lg border border-slate-800/70 bg-slate-900/40 p-2 space-y-2">
              <div className="text-[10px] uppercase tracking-wider font-semibold text-slate-500">
                Applications
              </div>
              <div className="flex items-start gap-2 text-sm text-slate-200">
                <span className="font-semibold text-slate-100 break-words">
                  {applicationCount ?? 0}
                </span>
              </div>
            </div>
          )}

          {/* Parsing Workflows - Jobs tab only */}
          {variant === "jobs" && parsingSteps && parseNotes !== undefined && (
            <ParsingWorkflowsSection parsingSteps={parsingSteps} parseNotes={parseNotes} />
          )}

          {/* Links section - Rejected tab only */}
          {variant === "rejected" && (
            <div className="rounded-lg border border-slate-800/70 bg-slate-900/40 p-2 space-y-2">
              <div className="text-[11px] uppercase tracking-wider font-semibold text-slate-500">
                Links
              </div>
              {url ? (
                <a
                  href={url}
                  target="_blank"
                  rel="noreferrer"
                  className="text-sm text-blue-300 hover:text-blue-200 underline break-all"
                >
                  {url}
                </a>
              ) : (
                <div className="text-sm text-slate-400">No link available</div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export type { PanelVariant, CompensationMeta };
