import { useMemo } from "react";
import { useQuery } from "convex/react";
import type { Id } from "../convex/_generated/dataModel";
import { api } from "../convex/_generated/api";
import { buildCompensationMeta } from "./lib/compensation";

const formatDateTime = (value?: number) => {
  if (typeof value !== "number") return "Unknown";
  return new Date(value).toLocaleString();
};

export function JobDetailsPage({ jobId, onBack }: { jobId: Id<"jobs">; onBack?: () => void }) {
  const job = useQuery(api.jobs.getJobById, { id: jobId });
  const compensationMeta = useMemo(() => buildCompensationMeta(job), [job]);
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

  const description = useMemo(() => {
    if (!job?.description) return "No description provided.";
    return job.description;
  }, [job]);

  const parsingSteps = useMemo(() => {
    const scrapedWith = job?.scrapedWith || job?.workflowName;
    const scrapedAt = job?.scrapedAt;
    const heuristicAttempts = job?.heuristicAttempts ?? 0;
    const heuristicLastTried = job?.heuristicLastTried;
    const heuristicVersion = job?.heuristicVersion;
    const heuristicRan =
      (job?.workflowName || "").toLowerCase() === "heuristicjobdetails" ||
      (job?.compensationReason || "").toLowerCase().includes("heuristic") ||
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
          ? heuristicParts.join(" • ") || job?.workflowName || "HeuristicJobDetails"
          : `Not run${job?.compensationReason ? ` (reason: ${job.compensationReason})` : ""}`,
        subtext: heuristicRan && job?.workflowName ? `Workflow: ${job.workflowName}` : undefined,
      },
      {
        label: "LLM parsing",
        checked: false,
        status: "Pending",
        note: "Optional enrichment (not requested)",
      },
    ];
  }, [formatRelativeTime, job]);

  const handleBack = () => {
    if (onBack) {
      onBack();
    } else {
      window.history.back();
    }
  };

  return (
    <div className="flex flex-col flex-1 bg-slate-950 text-slate-100 overflow-hidden">
      <div className="border-b border-slate-900 px-6 py-4 flex items-center justify-between bg-slate-950/90">
        <div className="min-w-0">
          <div className="text-[11px] uppercase tracking-wide text-slate-500 font-semibold">Job details</div>
          <h1 className="text-2xl font-bold text-white truncate">
            {job?.title ?? "Job not found"}
          </h1>
          {job?.company && <div className="text-sm text-slate-400 mt-1">{job.company}</div>}
        </div>
        <div className="flex items-center gap-3">
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
          <div className="max-w-5xl mx-auto px-6 py-8 space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <DetailCard label="Location" value={job.location || "Unknown"} />
              <DetailCard label="Level" value={job.level ?? "Not specified"} />
              <DetailCard label="Remote" value={job.remote ? "Yes" : "No"} />
              <DetailCard label="Total Compensation" value={compensationMeta.display} />
              {compensationMeta.currencyCode && compensationMeta.currencyCode !== "USD" && (
                <DetailCard label="Currency" value={compensationMeta.currencyCode} />
              )}
              <DetailCard label="Posted" value={formatDateTime(job.postedAt)} />
              <DetailCard label="Scraped At" value={formatDateTime(job.scrapedAt)} />
              <DetailCard label="Workflow" value={job.workflowName || "—"} />
              <DetailCard label="Source URL" value={job.url ?? "—"} isLink />
            </div>

            <div className="rounded-lg border border-slate-800 bg-slate-900/70 p-4 shadow-inner space-y-3">
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

            <div className="rounded-lg border border-slate-800 bg-slate-900/60 p-4 shadow-inner">
              <div className="text-xs uppercase tracking-wide font-semibold text-slate-500 mb-2">Description</div>
              <div className="text-sm leading-relaxed text-slate-200 whitespace-pre-wrap">
                {description}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function DetailCard({ label, value, isLink = false }: { label: string; value: string; isLink?: boolean }) {
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/50 p-3 flex flex-col gap-1">
      <div className="text-[10px] uppercase tracking-wider font-semibold text-slate-500">{label}</div>
      {isLink && value && value !== "—" ? (
        <a
          href={value}
          target="_blank"
          rel="noreferrer"
          className="text-sm font-medium text-blue-300 hover:text-blue-200 break-all underline-offset-2"
        >
          {value}
        </a>
      ) : (
        <div className="text-sm font-medium text-slate-100 break-words">{value}</div>
      )}
    </div>
  );
}
