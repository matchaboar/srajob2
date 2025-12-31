import { useMemo } from "react";
import { useQuery } from "convex/react";
import type { Id } from "../convex/_generated/dataModel";
import { api } from "../convex/_generated/api";
import { buildCompensationMeta } from "./lib/compensation";

const formatDateTime = (value?: number) => {
  if (typeof value !== "number") return "Unknown";
  return new Date(value).toLocaleString();
};

const formatDuration = (start?: number | null, end?: number | null) => {
  if (typeof start !== "number" || typeof end !== "number") return "—";
  const diff = Math.max(0, end - start);
  const totalSeconds = Math.floor(diff / 1000);
  if (totalSeconds <= 0) return "0s";
  const minutes = Math.floor(totalSeconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);
  if (days > 0) return `${days}d ${hours % 24}h`;
  if (hours > 0) return `${hours}h ${minutes % 60}m`;
  if (minutes > 0) return `${minutes}m ${totalSeconds % 60}s`;
  return `${totalSeconds}s`;
};

export function JobDetailsPage({ jobId, onBack }: { jobId: Id<"jobs">; onBack?: () => void }) {
  const job = useQuery(api.jobs.getJobById, { id: jobId });
  const compensationMeta = useMemo(() => buildCompensationMeta(job), [job]);
  const toOptionalNumber = (value: unknown) => (typeof value === "number" ? value : undefined);
  const toOptionalString = (value: unknown) => {
    if (typeof value === "string") return value;
    if (typeof value === "number" || typeof value === "boolean") return String(value);
    return undefined;
  };
  const formatRelativeTime = useMemo(
    () =>
      (timestamp?: number | null) => {
        if (typeof timestamp !== "number") return null;
        const delta = Math.max(0, Date.now() - timestamp);
        const minutes = Math.round(delta / (1000 * 60));
        const hours = Math.floor(minutes / 60);
        const days = Math.floor(hours / 24);
        let relative: string;
        if (days > 0) {
          relative = `${days}d ago`;
        } else if (hours > 0) {
          relative = `${hours}h ago`;
        } else if (minutes > 0) {
          relative = `${minutes}m ago`;
        } else {
          relative = "just now";
        }
        const absolute = new Date(timestamp).toLocaleString();
        return `${relative} • ${absolute}`;
      },
    []
  );
  const scrapedAt = toOptionalNumber(job?.scrapedAt);
  const postedAtUnknown = (job as { postedAtUnknown?: boolean } | null)?.postedAtUnknown ?? false;
  const postedAtLabel = postedAtUnknown ? "Unknown" : formatDateTime(job?.postedAt);
  const scrapeQueueCreatedAt = toOptionalNumber((job as { scrapeQueueCreatedAt?: number } | null)?.scrapeQueueCreatedAt);
  const scrapeQueueCompletedAt = toOptionalNumber(
    (job as { scrapeQueueCompletedAt?: number } | null)?.scrapeQueueCompletedAt
  );
  const scrapeQueueWait =
    typeof scrapeQueueCreatedAt === "number"
      ? typeof (scrapedAt ?? scrapeQueueCompletedAt) === "number"
        ? formatDuration(scrapeQueueCreatedAt, scrapedAt ?? scrapeQueueCompletedAt)
        : "Pending"
      : "—";

  const description = useMemo(() => {
    if (!job?.description) return "No description provided.";
    return job.description;
  }, [job]);
  const metadata = useMemo(() => {
    const raw = (job as { metadata?: string } | null)?.metadata ?? "";
    const trimmed = raw.trim();
    return trimmed;
  }, [job]);

  const parsingSteps = useMemo(() => {
    const workflowName = toOptionalString(job?.workflowName);
    const scrapedWith = toOptionalString(job?.scrapedWith) || workflowName;
    const heuristicAttempts = toOptionalNumber(job?.heuristicAttempts) ?? 0;
    const heuristicLastTried = toOptionalNumber(job?.heuristicLastTried);
    const heuristicVersion = toOptionalNumber(job?.heuristicVersion);
    const compensationReason = toOptionalString(job?.compensationReason);
    const heuristicRan =
      (workflowName || "").toLowerCase() === "heuristicjobdetails" ||
      (compensationReason || "").toLowerCase().includes("heuristic") ||
      heuristicAttempts > 0 ||
      typeof heuristicLastTried === "number";

    const heuristicParts: string[] = [];
    if (heuristicVersion !== undefined) {
      heuristicParts.push(`v${heuristicVersion}`);
    }
    if (heuristicAttempts > 0) {
      heuristicParts.push(`${heuristicAttempts} attempt${heuristicAttempts === 1 ? "" : "s"}`);
    }
    if (heuristicLastTried) {
      heuristicParts.push(`last ${formatRelativeTime(heuristicLastTried) || new Date(heuristicLastTried).toLocaleString()}`);
    }
    return [
      {
        label: "Initial scrape",
        checked: Boolean(scrapedWith),
        status: scrapedAt ? "Completed" : "Pending",
        note: scrapedAt
          ? `${new Date(scrapedAt).toLocaleString()}${scrapedWith ? ` • ${scrapedWith}` : ""}`
          : "Not scraped yet",
      },
      {
        label: "Heuristic parsing",
        checked: heuristicRan,
        status: heuristicRan ? "Completed" : "Pending",
        note: heuristicRan
          ? heuristicParts.join(" • ") || workflowName || "HeuristicJobDetails"
          : `Not run${compensationReason ? ` (reason: ${compensationReason})` : ""}`,
        subtext: heuristicRan && workflowName ? `Workflow: ${workflowName}` : undefined,
      },
      {
        label: "LLM parsing",
        checked: false,
        status: "Pending",
        note: "Optional enrichment (not requested)",
      },
    ];
  }, [formatRelativeTime, job, scrapedAt, toOptionalNumber, toOptionalString]);

  const handleBack = () => {
    if (onBack) {
      onBack();
    } else {
      window.history.back();
    }
  };

  return (
    <div className="flex flex-col flex-1 bg-slate-950 text-slate-100 overflow-hidden">
      <div className="border-b border-slate-900 px-4 py-3 sm:px-5 sm:py-4 flex items-center justify-between bg-slate-950/90">
        <div className="min-w-0">
          <div className="text-[11px] uppercase tracking-wide text-slate-500 font-semibold">Job details</div>
          <h1 className="text-2xl font-bold text-white truncate">{job?.title ?? "Job not found"}</h1>
          <div className="mt-1 flex flex-wrap items-center gap-2 text-sm text-slate-400">
            {job?.company && <span className="font-medium text-slate-300">{job.company}</span>}
            <span className="px-2 py-0.5 rounded-full border border-slate-800 bg-slate-900/60 text-[11px] font-semibold text-slate-200">
              {job?.location || "Location: Unknown"}
            </span>
            <span className="px-2 py-0.5 rounded-full border border-slate-800 bg-slate-900/60 text-[11px] font-semibold text-slate-200">
              {job?.level ? `Level: ${job.level}` : "Level: Not specified"}
            </span>
            <span className="px-2 py-0.5 rounded-full border border-slate-800 bg-slate-900/60 text-[11px] font-semibold text-slate-200">
              {job?.remote === true ? "Remote" : job?.remote === false ? "On-site" : "Remote: Unknown"}
            </span>
          </div>
          {job?.url && (
            <div className="mt-1 flex items-center gap-2 text-xs text-slate-500">
              <span className="uppercase tracking-wide">Source</span>
              <a
                href={job.url}
                target="_blank"
                rel="noreferrer"
                title={job.url}
                className="inline-block text-blue-300 hover:text-blue-200 underline-offset-2 truncate max-w-[60vw] sm:max-w-[420px] lg:max-w-[560px]"
              >
                {job.url}
              </a>
            </div>
          )}
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={handleBack}
            className="px-3 py-2 text-sm rounded border border-slate-800 bg-slate-900 text-slate-200 hover:text-white hover:border-slate-700 transition-colors"
          >
            Back to Admin
          </button>
          {job?.url && (
            <a
              href={job.url}
              target="_blank"
              rel="noreferrer"
              className="px-3 py-2 text-sm rounded bg-emerald-500 text-slate-950 font-semibold border border-emerald-600 hover:bg-emerald-400 transition-colors"
            >
              Open Job URL
            </a>
          )}
        </div>
      </div>

      {job === undefined ? (
        <div className="flex-1 flex items-center justify-center text-slate-400">Loading job...</div>
      ) : job === null ? (
        <div className="flex-1 flex items-center justify-center text-slate-400">Job not found.</div>
      ) : (
        <div className="flex-1 overflow-auto">
          <div className="max-w-6xl lg:max-w-7xl mx-auto px-4 py-5 space-y-4">
            <div className="rounded-lg border border-slate-800 bg-slate-900/60 p-4">
              <div className="text-xs uppercase tracking-wide font-semibold text-slate-500 mb-2">Description</div>
              <div className="text-sm leading-relaxed text-slate-200 whitespace-pre-wrap">{description}</div>
            </div>
            {metadata && (
              <div className="rounded-lg border border-slate-800 bg-slate-900/50 p-4">
                <div className="text-xs uppercase tracking-wide font-semibold text-slate-500 mb-2">Metadata</div>
                <div className="text-sm leading-relaxed text-slate-200 whitespace-pre-wrap">{metadata}</div>
              </div>
            )}

            <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-400">
              <span>
                <span className="text-slate-500">Total comp:</span> {compensationMeta.display}
              </span>
              {compensationMeta.currencyCode && compensationMeta.currencyCode !== "USD" && (
                <span>
                  <span className="text-slate-500">Currency:</span> {compensationMeta.currencyCode}
                </span>
              )}
              <span>
                <span className="text-slate-500">Posted:</span>{" "}
                <span className={postedAtUnknown ? "text-slate-600" : "text-emerald-300"}>
                  {postedAtLabel}
                </span>
              </span>
              <span>
                <span className="text-slate-500">Scraped:</span> {formatDateTime(job.scrapedAt)}
              </span>
              <span>
                <span className="text-slate-500">Queued:</span>{" "}
                {formatRelativeTime(scrapeQueueCreatedAt) ?? formatDateTime(scrapeQueueCreatedAt)}
              </span>
              <span>
                <span className="text-slate-500">Queue wait:</span> {scrapeQueueWait}
              </span>
              <span>
                <span className="text-slate-500">Workflow:</span> {job.workflowName || "—"}
              </span>
            </div>

            <div className="rounded-lg border border-slate-800 bg-slate-900/70 p-4 space-y-3">
              <div className="text-xs uppercase tracking-wide font-semibold text-slate-500">Parsing Workflows</div>
              <div className="flex flex-col gap-2">
                {parsingSteps.map((step) => (
                  <label key={step.label} className="flex items-start gap-3 text-sm text-slate-100">
                    <input type="checkbox" checked={step.checked} readOnly className="mt-0.5 h-4 w-4 accent-emerald-400" />
                    <span className="flex-1 flex flex-col gap-1 leading-tight">
                      <span className="flex items-center gap-2">
                        <span className="font-semibold">{step.label}</span>
                        <span
                          className={`px-2 py-0.5 rounded-full text-[10px] font-semibold uppercase tracking-wide border ${
                            step.checked
                              ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-200"
                              : "border-amber-500/40 bg-amber-500/10 text-amber-100"
                          }`}
                        >
                          {step.status || (step.checked ? "Completed" : "Pending")}
                        </span>
                      </span>
                      <span className="text-xs text-slate-400">{step.note}</span>
                      {step.subtext && <span className="text-[11px] text-slate-500">{step.subtext}</span>}
                    </span>
                  </label>
                ))}
              </div>
              <div className="text-[11px] uppercase tracking-wide font-semibold text-slate-500 pt-1">Parse Notes</div>
              <div className="rounded border border-slate-800 bg-slate-950/70 text-sm text-slate-200 px-3 py-2 whitespace-pre-wrap">
                {compensationMeta.reason || "No additional notes."}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
