import { useMutation, useQuery } from "convex/react";
import { api } from "../convex/_generated/api";
import { toast } from "sonner";
import { useState, useEffect, useMemo, useRef } from "react";
import type { FormEvent, MouseEvent } from "react";
import clsx from "clsx";
import { WorkflowRunsSection } from "./components/WorkflowRunsSection";
import { LiveTimer } from "./components/LiveTimer";
import { PROCESS_WEBHOOK_WORKFLOW, SITE_LEASE_WORKFLOW, formatInterval, type WorkflowScheduleMeta } from "./constants/schedules";

type AdminSection = "scraper" | "activity" | "activityRuns" | "worker" | "database" | "temporal" | "scrapeHistory" | "urlScrapes";
type AdminSectionExtended = AdminSection | "pending";
type ScheduleDay = "mon" | "tue" | "wed" | "thu" | "fri" | "sat" | "sun";
type ScrapeProvider = "fetchfox" | "firecrawl" | "spidercloud" | "fetchfox_spidercloud";
const SCHEDULE_DAY_LABELS: Record<ScheduleDay, string> = {
  mon: "Mon",
  tue: "Tue",
  wed: "Wed",
  thu: "Thu",
  fri: "Fri",
  sat: "Sat",
  sun: "Sun",
};
const ALL_SCHEDULE_DAYS: ScheduleDay[] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
const COMMON_SUBDOMAIN_PREFIXES = ["www", "jobs", "careers", "boards", "app", "apply"];
const DEFAULT_SCHEDULE_STORAGE_KEY = "admin-default-schedule-id";

const toTitleCaseSlug = (slug: string): string => {
  return slug
    .replace(/[_-]+/g, " ")
    .split(/[\s.]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
};

const baseDomainFromHost = (host: string): string => {
  const parts = host.split(".").filter(Boolean);
  if (parts.length <= 1) return host;
  const last = parts[parts.length - 1];
  const secondLast = parts[parts.length - 2];
  const shouldUseThree = secondLast.length === 2 || last.length === 2;
  if (shouldUseThree && parts.length >= 3) {
    return parts.slice(-3).join(".");
  }
  return parts.slice(-2).join(".");
};

const isGreenhouseUrlString = (rawUrl: string): boolean => {
  if (!rawUrl) return false;
  if (/greenhouse/i.test(rawUrl)) return true;
  try {
    const parsed = new URL(rawUrl);
    return /greenhouse/i.test(parsed.hostname);
  } catch {
    return /greenhouse/i.test(rawUrl);
  }
};

const deriveSiteName = (rawUrl: string): string => {
  if (!rawUrl) return "Site";
  try {
    const parsed = new URL(rawUrl);
    const host = parsed.hostname.toLowerCase();
    const pathSegments = parsed.pathname.split("/").filter(Boolean);

    // Greenhouse boards: company slug is usually the first path segment
    if (/greenhouse/.test(host) && pathSegments.length > 0) {
      const boardsIdx = pathSegments.findIndex((p) => p.toLowerCase() === "boards");
      if (boardsIdx >= 0 && boardsIdx + 1 < pathSegments.length) {
        const candidate = toTitleCaseSlug(pathSegments[boardsIdx + 1]);
        if (candidate) return candidate;
      }
      const candidate = toTitleCaseSlug(pathSegments[0]);
      if (candidate && candidate !== "V1") return candidate;
    }

    const hostParts = host.split(".");
    while (hostParts.length > 2 && COMMON_SUBDOMAIN_PREFIXES.includes(hostParts[0])) {
      hostParts.shift();
    }

    const basePart = hostParts.length >= 2 ? hostParts[hostParts.length - 2] : hostParts[0];
    if (basePart && !COMMON_SUBDOMAIN_PREFIXES.includes(basePart)) {
      const candidate = toTitleCaseSlug(basePart);
      if (candidate) return candidate;
    }

    if (pathSegments.length > 0) {
      const candidate = toTitleCaseSlug(pathSegments[0]);
      if (candidate) return candidate;
    }

    const baseDomain = baseDomainFromHost(host);
    if (baseDomain) return baseDomain;
  } catch {
    // fall back to raw input if parsing fails
  }
  return "Site";
};

const cleanCompanyName = (name: string, url: string): string => {
  const base = (name || "").trim() || deriveSiteName(url);
  const withoutSuffix = base
    .replace(/\binc(?:orporated)?\.?$/i, "")
    .replace(/\b(llc|ltd|corp|co|company|inc)\.?$/gi, "")
    .trim();
  const normalized = withoutSuffix || base;
  const slugged = normalized
    .split(/[\s._-]+/)
    .map((part) => part.trim())
    .filter(Boolean)
    .join(" ");
  const titleCased = toTitleCaseSlug(slugged || normalized);
  return titleCased || normalized || "Site";
};

const resolvePipeline = (provider: ScrapeProvider, siteType?: string) => {
  const normalized = provider || (siteType === "greenhouse" ? "spidercloud" : "fetchfox");
  if (normalized === "fetchfox_spidercloud") {
    return { crawler: "FetchFox", scraper: "SpiderCloud", extractor: "Regex/Heuristic parser" };
  }
  if (normalized === "firecrawl") {
    return { crawler: "Firecrawl", scraper: "Firecrawl", extractor: "Firecrawl" };
  }
  if (normalized === "spidercloud") {
    return { crawler: "SpiderCloud", scraper: "SpiderCloud", extractor: "SpiderCloud" };
  }
  return { crawler: "FetchFox", scraper: "FetchFox", extractor: "FetchFox" };
};

function TemporalStatusSection() {
  const [activeTab, setActiveTab] = useState<"active" | "stale">("active");
  const activeWorkers = useQuery(api.temporal?.getActiveWorkers);
  const staleWorkers = useQuery(api.temporal?.getStaleWorkers);

  const workers = activeTab === "active" ? activeWorkers : staleWorkers;
  const mergedWorkers = workers
    ? (() => {
      const byHost = new Map<string, any>();
      for (const w of workers as any[]) {
        const existing = byHost.get(w.hostname);
        if (!existing) {
          byHost.set(w.hostname, {
            ...w,
            workerIds: [w.workerId],
            latestWorkerId: w.workerId,
            workflows: w.workflows || [],
          });
          continue;
        }

        // Merge workflows and keep latest heartbeat info
        const merged: any = {
          ...existing,
          lastHeartbeat:
            (w.lastHeartbeat ?? 0) > (existing.lastHeartbeat ?? 0) ? w.lastHeartbeat : existing.lastHeartbeat,
          latestWorkerId:
            (w.lastHeartbeat ?? 0) > (existing.lastHeartbeat ?? 0) ? w.workerId : existing.latestWorkerId,
          temporalAddress: w.temporalAddress ?? existing.temporalAddress,
          temporalNamespace: w.temporalNamespace ?? existing.temporalNamespace,
          taskQueue: w.taskQueue ?? existing.taskQueue,
          workflows: [
            ...existing.workflows,
            ...((w.workflows || []).filter((wf: any) => !(existing.workflows || []).some((e: any) => e.id === wf.id))),
          ],
          workerIds: Array.from(new Set([...(existing.workerIds || []), w.workerId])),
        };
        byHost.set(w.hostname, merged);
      }
      return Array.from(byHost.values());
    })()
    : workers;

  if (workers === undefined) {
    return <div className="text-slate-400 p-4">Loading workers...</div>;
  }

  return (
    <div className="bg-slate-900 p-4 rounded border border-slate-800 shadow-sm">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-white">Temporal Workers</h2>
        <div className="flex bg-slate-950 rounded p-0.5 border border-slate-800">
          <button
            onClick={() => setActiveTab("active")}
            className={clsx(
              "px-3 py-1 text-xs font-medium rounded transition-colors",
              activeTab === "active" ? "bg-slate-800 text-white shadow-sm" : "text-slate-400 hover:text-slate-200"
            )}
          >
            Active ({activeWorkers?.length || 0})
          </button>
          <button
            onClick={() => setActiveTab("stale")}
            className={clsx(
              "px-3 py-1 text-xs font-medium rounded transition-colors",
              activeTab === "stale" ? "bg-slate-800 text-white shadow-sm" : "text-slate-400 hover:text-slate-200"
            )}
          >
            Stale ({staleWorkers?.length || 0})
          </button>
        </div>
      </div>

      {mergedWorkers.length === 0 ? (
        <div className="text-slate-400 text-sm p-4 text-center border border-slate-800 rounded bg-slate-950/30">
          {activeTab === "active" ? (
            <>
              No active workers detected.
              <br />
              <span className="text-xs text-slate-500 mt-1 block">
                Start a worker with <code className="bg-slate-900 px-1 rounded">.\start_worker.ps1</code>
              </span>
            </>
          ) : (
            <>
              No stale workers.
              <br />
              <span className="text-xs text-slate-500 mt-1 block">
                Workers that haven't sent a heartbeat in 90+ seconds appear here.
              </span>
            </>
          )}
        </div>
      ) : (
        <div className="space-y-3">
          {mergedWorkers.map((worker: any) => {
            const workflowCount = worker.workflows?.length || 0;
            const isStale = activeTab === "stale";

            return (
              <div
                key={worker._id}
                className={clsx(
                  "bg-slate-950/50 border rounded p-4",
                  isStale ? "border-amber-900/30" : "border-slate-800"
                )}
              >
                {/* Worker Header */}
                <div className="flex items-start justify-between mb-3">
                  <div>
                    <div className="flex items-center gap-2 mb-1">
                      <div className={clsx("w-2 h-2 rounded-full", isStale ? "bg-amber-500" : "bg-green-500")} />
                      <h3 className="text-sm font-semibold text-white">{worker.hostname}</h3>
                      <span className="text-xs text-slate-500 font-mono">{worker.latestWorkerId || worker.workerId}</span>
                    </div>
                    <div className="text-xs text-slate-400 space-y-0.5">
                      <div>Queue: <span className="text-slate-300">{worker.taskQueue}</span></div>
                      <div>Temporal: <span className="text-slate-300">{worker.temporalAddress}</span> / {worker.temporalNamespace}</div>
                    </div>
                  </div>
                  <div className="text-right">
                    <div className="text-xs text-slate-500 mb-1">Last heartbeat</div>
                    <div className="text-sm font-medium font-mono text-slate-200">
                      <LiveTimer
                        startTime={worker.lastHeartbeat}
                        colorize
                        warnAfterMs={90_000}
                        dangerAfterMs={5 * 60 * 1000}
                        showAgo
                      />
                    </div>
                    <div className="text-[10px] text-slate-600 mt-0.5">
                      {new Date(worker.lastHeartbeat).toLocaleTimeString()}
                    </div>
                  </div>
                </div>

                {/* Workflows Section */}
                <div className="border-t border-slate-800 pt-3">
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-xs font-medium text-slate-400">
                      Workflows ({workflowCount})
                    </span>
                    {worker.noWorkflowsReason && (
                      <span className="text-xs text-slate-500 italic">
                        {worker.noWorkflowsReason}
                      </span>
                    )}
                  </div>

                  {workflowCount > 0 ? (
                    <div className="overflow-x-auto border border-slate-800 rounded">
                      <table className="w-full text-left text-xs text-slate-400">
                        <thead className="text-[10px] uppercase bg-slate-950 text-slate-300">
                          <tr>
                            <th className="px-3 py-1.5 border-b border-slate-800">ID</th>
                            <th className="px-3 py-1.5 border-b border-slate-800">Type</th>
                            <th className="px-3 py-1.5 border-b border-slate-800">Status</th>
                            <th className="px-3 py-1.5 border-b border-slate-800">Start Time</th>
                          </tr>
                        </thead>
                        <tbody>
                          {worker.workflows.map((wf: any) => (
                            <tr key={wf.id} className="border-b border-slate-800 hover:bg-slate-800/50 last:border-0">
                              <td className="px-3 py-1.5 font-mono text-[10px] text-slate-300">{wf.id}</td>
                              <td className="px-3 py-1.5">{wf.type}</td>
                              <td className="px-3 py-1.5">
                                <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-green-900/30 text-green-400 border border-green-900">
                                  {wf.status}
                                </span>
                              </td>
                              <td className="px-3 py-1.5 text-[10px]">{new Date(wf.startTime).toLocaleString()}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="text-xs text-slate-500 text-center py-2 bg-slate-900/50 rounded border border-slate-800">
                      No workflows running
                    </div>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function ScrapeHistorySection() {
  const scrapes = useQuery(api.router.listScrapes, { limit: 50 });

  if (scrapes === undefined) {
    return <div className="text-slate-400 p-4">Loading scrape history...</div>;
  }

  if (!scrapes?.length) {
    return (
      <div className="text-slate-400 text-sm p-4 text-center border border-slate-800 rounded bg-slate-950/30">
        No scrapes recorded yet.
      </div>
    );
  }

  return (
    <div className="bg-slate-900 p-4 rounded border border-slate-800 shadow-sm overflow-x-auto h-full w-full flex flex-col">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-lg font-semibold text-white">Scrape History</h2>
        <span className="text-xs text-slate-400">Latest {scrapes.length}</span>
      </div>
      <table className="min-w-full w-full text-left text-sm text-slate-200 flex-1">
        <thead className="bg-slate-800 text-slate-300 text-xs uppercase tracking-wide">
          <tr>
            <th className="px-2 py-2">URL</th>
            <th className="px-2 py-2">Type</th>
            <th className="px-2 py-2">Provider</th>
            <th className="px-2 py-2">Job</th>
            <th className="px-2 py-2">Batch</th>
            <th className="px-2 py-2">Workflow</th>
            <th className="px-2 py-2">Sync Response</th>
            <th className="px-2 py-2">Async State</th>
            <th className="px-2 py-2">Async Response</th>
            <th className="px-2 py-2">Sub URLs</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800">
          {scrapes.map((row: any) => {
            const jobLink = row.jobBoardJobId ? `/jobs/${row.jobBoardJobId}` : null;
            return (
              <tr key={row._id} className="hover:bg-slate-800/70 bg-slate-900/70">
                <td className="px-2 py-2 max-w-[200px] truncate" title={row.sourceUrl}>
                  {row.sourceUrl}
                </td>
                <td className="px-2 py-2">{row.type || "n/a"}</td>
                <td className="px-2 py-2">{row.provider || "n/a"}</td>
                <td className="px-2 py-2">
                  {jobLink ? (
                    <a href={jobLink} className="text-blue-300 hover:text-blue-100 underline">
                      {row.jobBoardJobId}
                    </a>
                  ) : (
                    "—"
                  )}
                </td>
                <td className="px-2 py-2">{row.batchId || "—"}</td>
                <td className="px-2 py-2">
                  <div className="flex flex-col leading-tight">
                    <span className="text-xs text-slate-300">{row.workflowName || row.workflowType || "—"}</span>
                    <span className="text-[11px] text-slate-500">{row.workflowId || "—"}</span>
                  </div>
                </td>
                <td className="px-2 py-2 max-w-[200px] truncate" title={JSON.stringify(row.response)?.slice(0, 500)}>
                  {row.response ? JSON.stringify(row.response).slice(0, 80) : "—"}
                </td>
                <td className="px-2 py-2">{row.asyncState || "—"}</td>
                <td className="px-2 py-2 max-w-[200px] truncate" title={JSON.stringify(row.asyncResponse)?.slice(0, 500)}>
                  {row.asyncResponse ? JSON.stringify(row.asyncResponse).slice(0, 80) : "—"}
                </td>
                <td className="px-2 py-2 max-w-[160px] truncate" title={(row.subUrls || []).join(", ")}>
                  {(row.subUrls || []).slice(0, 3).join(", ") || "—"}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function UrlScrapeListSection() {
  const logs = useQuery(api.router.listUrlScrapeLogs, { limit: 200 });

  const formatJson = (value: any) => {
    if (value === undefined) return "—";
    if (typeof value === "string") return value;
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  };

  if (logs === undefined) {
    return <div className="text-slate-400 p-4">Loading URL scrapes...</div>;
  }

  if (!logs?.length) {
    return (
      <div className="text-slate-400 text-sm p-4 text-center border border-slate-800 rounded bg-slate-950/30">
        No URL scrapes recorded yet.
      </div>
    );
  }

  const ExpandableJsonCell = ({ value }: { value: any }) => {
    const [hovered, setHovered] = useState(false);
    const [popoverStyle, setPopoverStyle] = useState<{ top: number; left: number; maxWidth: number; maxHeight: number }>(() => ({
      top: 0,
      left: 0,
      maxWidth: 520,
      maxHeight: 520,
    }));
    const [copied, setCopied] = useState(false);
    const copyResetRef = useRef<ReturnType<typeof setTimeout> | null>(null);

    useEffect(() => {
      return () => {
        if (copyResetRef.current) {
          clearTimeout(copyResetRef.current);
        }
      };
    }, []);

    const handleMove = (event: MouseEvent<HTMLDivElement>) => {
      const vw = window.innerWidth || 1200;
      const vh = window.innerHeight || 800;
      const maxWidth = Math.min(520, vw - 24);
      const maxHeight = Math.min(520, vh - 24);
      const preferredLeft = event.clientX - maxWidth * 0.2;
      const clampedLeft = Math.min(Math.max(12, preferredLeft), vw - maxWidth - 12);
      const preferredTop = event.clientY + 12;
      const clampedTop = Math.min(preferredTop, vh - maxHeight - 12);
      setPopoverStyle({ top: clampedTop, left: clampedLeft, maxWidth, maxHeight });
    };

    const formatted = formatJson(value);
    const handleCopy = async () => {
      if (!formatted || formatted === "—") return;
      if (copyResetRef.current) {
        clearTimeout(copyResetRef.current);
      }
      try {
        if (typeof navigator === "undefined" || !navigator.clipboard) {
          toast.error("Clipboard not available in this browser");
          return;
        }
        await navigator.clipboard.writeText(formatted);
        setCopied(true);
        copyResetRef.current = setTimeout(() => setCopied(false), 1200);
      } catch (err) {
        console.error("Failed to copy JSON", err);
        toast.error("Failed to copy");
      }
    };

    return (
      <div
        className="relative flex items-start gap-2 group"
        onMouseEnter={() => setHovered(true)}
        onMouseLeave={() => setHovered(false)}
        onMouseMove={handleMove}
      >
        <pre
          className="bg-slate-950/60 border border-slate-800 rounded p-1 max-h-7 min-h-[14px] leading-tight overflow-hidden whitespace-pre-wrap break-words font-mono text-[11px] cursor-pointer transition-colors hover:border-slate-600 focus:outline-none focus:ring-1 focus:ring-emerald-500"
          onClick={handleCopy}
          role="button"
          tabIndex={0}
          onKeyDown={(event) => {
            if (event.key === "Enter" || event.key === " ") {
              event.preventDefault();
              handleCopy();
            }
          }}
          title={copied ? "Copied" : "Click to copy"}
        >
          {formatted}
        </pre>
        <div className="flex flex-col items-start gap-1 pt-0.5">
          <button
            type="button"
            onClick={(event) => {
              event.stopPropagation();
              handleCopy();
            }}
            className="inline-flex h-7 w-7 items-center justify-center rounded border border-slate-800 bg-slate-950 text-slate-300 hover:text-white hover:border-slate-600 hover:bg-slate-800 transition-colors focus:outline-none focus:ring-1 focus:ring-emerald-500"
            title={copied ? "Copied" : "Copy JSON"}
            aria-label="Copy JSON to clipboard"
          >
            <svg
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="h-4 w-4"
            >
              <rect x="9" y="9" width="11" height="11" rx="2" ry="2" />
              <path d="M5 15V5a2 2 0 0 1 2-2h10" />
            </svg>
          </button>
          {copied && <span className="text-[10px] text-emerald-300 font-semibold">Copied</span>}
        </div>
        {hovered && (
          <div
            className="fixed z-50 pointer-events-none"
            style={{
              top: popoverStyle.top,
              left: popoverStyle.left,
              width: popoverStyle.maxWidth,
              maxWidth: popoverStyle.maxWidth,
              maxHeight: popoverStyle.maxHeight,
            }}
          >
            <div className="bg-slate-950 border border-slate-600 rounded shadow-2xl p-3 max-h-[32rem] overflow-auto">
              <pre className="whitespace-pre-wrap break-words font-mono text-[11px]">{formatted}</pre>
            </div>
          </div>
        )}
      </div>
    );
  };

  const TimestampCell = ({ timestamp }: { timestamp?: number | string }) => {
    const parsed = typeof timestamp === "string" ? Date.parse(timestamp) : timestamp;
    if (!parsed || Number.isNaN(parsed)) return <span className="text-slate-600">—</span>;

    const formatted = new Date(parsed).toLocaleString();

    return (
      <div className="flex flex-col gap-1">
        <span className="text-[11px] text-slate-200 font-mono">{formatted}</span>
        <span className="inline-flex items-center gap-1 text-[10px] px-2 py-1 rounded-full border border-slate-800 bg-slate-900/70 text-slate-200 w-fit">
          <LiveTimer
            startTime={parsed}
            colorize
            warnAfterMs={10 * 60 * 1000}
            dangerAfterMs={60 * 60 * 1000}
            showAgo
            suffixClassName="text-slate-400"
          />
        </span>
      </div>
    );
  };

  return (
    <div className="flex flex-col w-full h-full min-h-screen bg-slate-950">
      <div className="flex items-center justify-end px-4 py-3 border-b border-slate-900 bg-slate-950">
        <span className="text-xs text-slate-400">Showing {logs.length}</span>
      </div>
      <div className="flex-1 overflow-hidden">
        <div className="h-full overflow-auto">
          <table className="min-w-full w-full text-left text-[11px] text-slate-200 table-fixed">
            <thead className="bg-slate-800 text-slate-50 uppercase tracking-wide border-b border-slate-700 shadow-inner sticky top-0 z-10">
              <tr>
                <th className="px-3 py-2 w-56 font-bold">URL</th>
                <th className="px-3 py-2 w-40 font-bold">Timestamp</th>
                <th className="px-3 py-2 w-48 font-bold">JobBoard</th>
                <th className="px-3 py-2 w-28 font-bold">Reason</th>
                <th className="px-3 py-2 w-20 font-bold">Action</th>
                <th className="px-3 py-2 w-24 font-bold">Provider</th>
                <th className="px-3 py-2 w-32 font-bold">Workflow</th>
                <th className="px-3 py-2 w-28 font-bold">Batch</th>
                <th className="px-3 py-2 w-64 font-bold">Request Data</th>
                <th className="px-3 py-2 w-64 font-bold">Response</th>
                <th className="px-3 py-2 w-64 font-bold">Async Response</th>
              </tr>
            </thead>
            <tbody className="bg-slate-950 divide-y divide-slate-800">
              {logs.map((row: any, idx: number) => (
                <tr key={`${row.url}-${idx}`} className="hover:bg-slate-900 transition-colors">
                  <td className="px-3 py-2 align-top">
                    {row.url ? (
                      <a href={row.url} className="text-blue-300 hover:text-blue-100 underline break-all">
                        {row.url}
                      </a>
                    ) : (
                      "—"
                    )}
                  </td>
                  <td className="px-3 py-2 align-top">
                    <TimestampCell timestamp={row.timestamp} />
                  </td>
                  <td className="px-3 py-2 align-top">
                    {row.jobId ? (
                      <div className="flex flex-col gap-1">
                        <a
                          href={`/#job-details-${row.jobId}`}
                          className="text-emerald-300 hover:text-emerald-200 font-semibold underline underline-offset-2 break-all"
                          title="Open job details in JobBoard"
                        >
                          {row.jobTitle || "Open in JobBoard"}
                        </a>
                        {row.jobCompany && (
                          <span className="text-[11px] text-slate-400 truncate">{row.jobCompany}</span>
                        )}
                      </div>
                    ) : (
                      <span className="text-slate-600">—</span>
                    )}
                  </td>
                  <td className="px-3 py-2 align-top">
                    {row.reason ? (
                      <span className="px-2 py-1 rounded bg-slate-800 text-slate-100 border border-slate-700">{row.reason}</span>
                    ) : (
                      "—"
                    )}
                  </td>
                  <td className="px-3 py-2 align-top">
                    <span
                      className={clsx(
                        "px-2 py-1 rounded text-[10px] font-semibold uppercase",
                        row.action === "skipped"
                          ? "bg-amber-900/40 text-amber-200 border border-amber-800"
                          : "bg-green-900/40 text-green-200 border border-green-800"
                      )}
                    >
                      {row.action || "n/a"}
                    </span>
                  </td>
                  <td className="px-3 py-2 align-top">{row.provider || "—"}</td>
                  <td className="px-3 py-2 align-top break-words">{row.workflow || "—"}</td>
                  <td className="px-3 py-2 align-top break-all">{row.batchId || "—"}</td>
                  <td className="px-3 py-2 align-top">
                    <ExpandableJsonCell value={row.requestData} />
                  </td>
                  <td className="px-3 py-2 align-top">
                    <ExpandableJsonCell value={row.response} />
                  </td>
                  <td className="px-3 py-2 align-top">
                    <ExpandableJsonCell value={row.asyncResponse} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

export function AdminPage() {
  // Use URL hash to persist active section across refreshes
  const parseHash = () => {
    const raw = window.location.hash.replace("#admin-", "");
    const [section, query] = raw.split("?");
    const urlParam = new URLSearchParams(query || "").get("url");
    const allowed = ["scraper", "activity", "activityRuns", "worker", "database", "temporal", "pending", "scrapeHistory", "urlScrapes"] as const;
    const sec = allowed.includes(section as any) ? (section as AdminSectionExtended) : "scraper";
    return { section: sec, urlParam };
  };

  const [{ section, runsUrl }, setNavState] = useState<{ section: AdminSectionExtended; runsUrl: string | null }>(() => {
    const { section, urlParam } = parseHash();
    return { section, runsUrl: urlParam || null };
  });

  // Update URL hash when active section changes
  useEffect(() => {
    const current = window.location.hash;
    const target =
      section === "activityRuns" && runsUrl
        ? `#admin-${section}?url=${encodeURIComponent(runsUrl)}`
        : `#admin-${section}`;
    if (current !== target) {
      window.location.hash = target;
    }
  }, [section, runsUrl]);

  // Listen for hash changes (back/forward navigation)
  useEffect(() => {
    const handleHashChange = () => {
      const { section: sec, urlParam } = parseHash();
      setNavState({ section: sec, runsUrl: urlParam || null });
    };
    window.addEventListener("hashchange", handleHashChange);
    return () => window.removeEventListener("hashchange", handleHashChange);
  }, []);

  return (
    <div className="flex min-h-screen bg-slate-950 text-slate-200 font-sans">
      {/* Sidebar */}
      <aside className="w-60 bg-slate-950 border-r border-slate-900 flex-shrink-0 fixed h-full overflow-y-auto">
        <div className="p-4 border-b border-slate-900">
          <h1 className="text-lg font-bold text-white tracking-tight">Admin Panel</h1>
        </div>
        <nav className="p-3 space-y-1">
          <SidebarItem
            label="Scraper Config"
            active={section === "scraper"}
            onClick={() => setNavState({ section: "scraper", runsUrl: null })}
          />
          <SidebarItem
            label="Scrape Activity"
            active={section === "activity"}
            onClick={() => setNavState({ section: "activity", runsUrl: null })}
          />
          <SidebarItem
            label="Worker Status"
            active={section === "worker"}
            onClick={() => setNavState({ section: "worker", runsUrl: null })}
          />
          <SidebarItem
            label="Pending Requests"
            active={section === "pending"}
            onClick={() => setNavState({ section: "pending", runsUrl: null })}
          />
          <SidebarItem
            label="Database"
            active={section === "database"}
            onClick={() => setNavState({ section: "database", runsUrl: null })}
          />
          <SidebarItem
            label="Scrape History"
            active={section === "scrapeHistory"}
            onClick={() => setNavState({ section: "scrapeHistory", runsUrl: null })}
          />
          <SidebarItem
            label="URL scrape list"
            active={section === "urlScrapes"}
            onClick={() => setNavState({ section: "urlScrapes", runsUrl: null })}
          />
          <SidebarItem
            label="Temporal Status"
            active={section === "temporal"}
            onClick={() => setNavState({ section: "temporal", runsUrl: null })}
          />
        </nav>
      </aside>

      {/* Main Content */}
      <main
        className={clsx(
          "flex-1 ml-60 overflow-y-auto",
          section === "activity" || section === "urlScrapes" ? "p-0" : "p-8"
        )}
      >
        <div
          className={clsx(
            "w-full",
            section === "activity" || section === "urlScrapes" ? "max-w-none" : "max-w-5xl mx-auto"
          )}
        >
          {section === "scraper" && <ScraperConfigSection />}
          {section === "activity" && <ScrapeActivitySection onOpenRuns={(url) => setNavState({ section: "activityRuns", runsUrl: url })} />}
          {section === "activityRuns" && <WorkflowRunsSection url={runsUrl} onBack={() => setNavState({ section: "activity", runsUrl: null })} />}
          {section === "worker" && <WorkerStatusSection />}
          {section === "pending" && <PendingRequestsSection />}
          {section === "database" && <DatabaseSection />}
          {section === "scrapeHistory" && <ScrapeHistorySection />}
          {section === "urlScrapes" && <UrlScrapeListSection />}
          {section === "temporal" && <TemporalStatusSection />}
        </div>
      </main>
    </div>
  );
}

function SidebarItem({ label, active, onClick }: { label: string; active: boolean; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className={clsx(
        "w-full text-left px-3 py-2 rounded text-sm font-medium transition-colors",
        active
          ? "bg-slate-800 text-white shadow-inner"
          : "text-slate-400 hover:bg-slate-900 hover:text-slate-200"
      )}
    >
      {label}
    </button>
  );
}

function ScraperConfigSection() {
  const [showDisabled, setShowDisabled] = useState(false);
  const sites = useQuery(api.router.listSites, { enabledOnly: !showDisabled });
  const allSites = useQuery(api.router.listSites, { enabledOnly: false });
  const disabledCount = allSites ? allSites.filter((s: any) => !s.enabled).length : 0;
  const schedules = useQuery(api.router.listSchedules);
  const upsertSite = useMutation(api.router.upsertSite);
  const bulkUpsertSites = useMutation(api.router.bulkUpsertSites);
  const runSiteNow = useMutation(api.router.runSiteNow);
  const updateSiteEnabled = useMutation(api.router.updateSiteEnabled);
  const updateSiteName = useMutation(api.router.updateSiteName);
  const updateSiteSchedule = useMutation(api.router.updateSiteSchedule);
  const upsertSchedule = useMutation(api.router.upsertSchedule);
  const deleteSchedule = useMutation(api.router.deleteSchedule);

  const [mode, setMode] = useState<"single" | "bulk">("single");
  const [selectedScheduleId, setSelectedScheduleId] = useState<string>("");
  const [bulkScheduleId, setBulkScheduleId] = useState<string>("");
  const [defaultScheduleId, setDefaultScheduleId] = useState<string>(() => {
    if (typeof window === "undefined") return "";
    try {
      return window.localStorage.getItem(DEFAULT_SCHEDULE_STORAGE_KEY) || "";
    } catch {
      return "";
    }
  });
  const [scheduleName, setScheduleName] = useState("");
  const defaultTimezone = useMemo(() => {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || "America/Denver";
  }, []);
  const [scheduleDays, setScheduleDays] = useState<Set<ScheduleDay>>(new Set(ALL_SCHEDULE_DAYS));
  const [scheduleStartTime, setScheduleStartTime] = useState("08:00");
  const [scheduleIntervalHours, setScheduleIntervalHours] = useState(24);
  const [scheduleIntervalMinutes, setScheduleIntervalMinutes] = useState(0);
  const [scheduleTimezone, setScheduleTimezone] = useState(defaultTimezone);
  const [editingScheduleId, setEditingScheduleId] = useState<string | null>(null);
  const [savingSchedule, setSavingSchedule] = useState(false);
  const [deletingScheduleId, setDeletingScheduleId] = useState<string | null>(null);
  const [updatingSiteScheduleId, setUpdatingSiteScheduleId] = useState<string | null>(null);
  const [nameEdits, setNameEdits] = useState<Record<string, string>>({});
  const [savingSiteNameId, setSavingSiteNameId] = useState<string | null>(null);
  const [expandedSites, setExpandedSites] = useState<Set<string>>(new Set());
  const siteRowColumns = "grid grid-cols-[minmax(0,1.7fr)_minmax(0,1.05fr)_minmax(0,0.8fr)_minmax(0,0.8fr)_minmax(0,0.7fr)]";

  // Single add state
  const [url, setUrl] = useState("");
  const [siteType, setSiteType] = useState<"general" | "greenhouse">("general");
  const [scrapeProvider, setScrapeProvider] = useState<ScrapeProvider>("fetchfox_spidercloud");
  const [pattern, setPattern] = useState("");
  const [enabled, setEnabled] = useState(true);

  // Bulk add state
  const [bulkText, setBulkText] = useState("");
  const [bulkSiteType, setBulkSiteType] = useState<"general" | "greenhouse">("general");
  const [bulkScrapeProvider, setBulkScrapeProvider] = useState<ScrapeProvider>("fetchfox_spidercloud");

  const isGreenhouseUrl = useMemo(() => isGreenhouseUrlString(url), [url]);
  const generatedName = useMemo(() => deriveSiteName(url), [url]);

  const setDefaultSchedule = (id: string) => {
    setDefaultScheduleId(id);
    if (typeof window !== "undefined") {
      try {
        window.localStorage.setItem(DEFAULT_SCHEDULE_STORAGE_KEY, id);
      } catch {
        // ignore storage errors
      }
    }
    setSelectedScheduleId(id);
    setBulkScheduleId(id);
  };

  useEffect(() => {
    if (!isGreenhouseUrl) return;
    if (siteType !== "greenhouse") setSiteType("greenhouse");
    if (scrapeProvider !== "spidercloud") setScrapeProvider("spidercloud");
    if (pattern) setPattern("");
    if (!enabled) setEnabled(true);
  }, [isGreenhouseUrl, siteType, pattern, enabled, selectedScheduleId, scrapeProvider]);

  useEffect(() => {
    if (!schedules || schedules.length === 0) return;
    const first = schedules[0]._id as unknown as string;
    if (!defaultScheduleId && first) {
      setDefaultSchedule(first);
      return;
    }
    const hasDefault = defaultScheduleId && (schedules as any[]).some((s) => (s._id as unknown as string) === defaultScheduleId);
    const target = hasDefault ? defaultScheduleId : first;

    if (!hasDefault && defaultScheduleId && typeof window !== "undefined") {
      try {
        window.localStorage.setItem(DEFAULT_SCHEDULE_STORAGE_KEY, target);
      } catch {
        // ignore
      }
      setDefaultScheduleId(target);
    }

    if (!selectedScheduleId) {
      setSelectedScheduleId(target);
    }
    if (!bulkScheduleId) {
      setBulkScheduleId(target);
    }
  }, [schedules, selectedScheduleId, bulkScheduleId, defaultScheduleId]);

  const scheduleMap = useMemo(() => {
    const map = new Map<string, any>();
    (schedules ?? []).forEach((s: any) => {
      map.set(s._id as unknown as string, s);
    });
    return map;
  }, [schedules]);

  const resetScheduleForm = () => {
    setScheduleName("");
    setScheduleDays(new Set(ALL_SCHEDULE_DAYS));
    setScheduleStartTime("08:00");
    setScheduleIntervalHours(24);
    setScheduleIntervalMinutes(0);
    setScheduleTimezone(defaultTimezone);
    setEditingScheduleId(null);
  };

  const formatIntervalLabel = (minutes: number) => {
    const hrs = Math.floor(minutes / 60);
    const mins = minutes % 60;
    if (hrs > 0 && mins > 0) return `${hrs}h ${mins}m`;
    if (hrs > 0) return `${hrs}h`;
    return `${mins}m`;
  };

  const formatScheduleSummary = (schedule: any) => {
    const days = (schedule?.days ?? []) as ScheduleDay[];
    const ordered = ALL_SCHEDULE_DAYS.filter((d) => days.includes(d));
    const dayLabel =
      ordered.length === 7
        ? "Every day"
        : ordered.length === 5 && !ordered.includes("sat") && !ordered.includes("sun")
          ? "Weekdays"
          : ordered.map((d) => SCHEDULE_DAY_LABELS[d]).join(", ") || "Custom days";
    return `${dayLabel} • ${schedule?.startTime ?? "??:??"} ${schedule?.timezone ?? "UTC"} • every ${formatIntervalLabel(schedule?.intervalMinutes ?? 0)}`;
  };

  const handleEditSchedule = (schedule: any) => {
    setEditingScheduleId(schedule._id as string);
    setScheduleName(schedule.name ?? "");
    setScheduleDays(new Set((schedule.days ?? []) as ScheduleDay[]));
    setScheduleStartTime(schedule.startTime ?? "08:00");
    setScheduleTimezone(schedule.timezone ?? defaultTimezone);
    const minutes = Math.max(0, schedule.intervalMinutes ?? 0);
    setScheduleIntervalHours(Math.floor(minutes / 60));
    setScheduleIntervalMinutes(minutes % 60);
  };

  const handleSaveSchedule = async () => {
    const totalMinutes = Math.max(0, scheduleIntervalHours) * 60 + Math.max(0, scheduleIntervalMinutes);

    if (!scheduleDays.size) {
      toast.error("Pick at least one day");
      return;
    }
    if (!/^\d{2}:\d{2}$/.test(scheduleStartTime)) {
      toast.error("Start time must be in HH:MM format");
      return;
    }
    if (totalMinutes <= 0) {
      toast.error("Repeat interval must be greater than 0 minutes");
      return;
    }

    try {
      setSavingSchedule(true);
      const savedId = await upsertSchedule({
        id: editingScheduleId ?? undefined,
        name: scheduleName.trim() || "Untitled schedule",
        days: Array.from(scheduleDays) as ScheduleDay[],
        startTime: scheduleStartTime,
        intervalMinutes: totalMinutes,
        timezone: scheduleTimezone || defaultTimezone,
      });
      const savedIdStr = savedId as unknown as string;
      toast.success(editingScheduleId ? "Schedule updated" : "Schedule created");
      setSelectedScheduleId((prev) => prev || savedIdStr);
      setBulkScheduleId((prev) => prev || savedIdStr);
      resetScheduleForm();
    } catch {
      toast.error("Failed to save schedule");
    } finally {
      setSavingSchedule(false);
    }
  };

  const handleDeleteSchedule = async (id: string) => {
    try {
      setDeletingScheduleId(id);
      await deleteSchedule({ id: id as any });
      toast.success("Schedule deleted");
      if (selectedScheduleId === id) setSelectedScheduleId("");
      if (bulkScheduleId === id) setBulkScheduleId("");
      if (editingScheduleId === id) resetScheduleForm();
    } catch {
      toast.error("Cannot delete a schedule that is still in use");
    } finally {
      setDeletingScheduleId(null);
    }
  };

  const handleSiteScheduleChange = async (siteId: string, scheduleId: string) => {
    try {
      setUpdatingSiteScheduleId(siteId);
      await updateSiteSchedule({
        id: siteId as any,
        scheduleId: scheduleId ? (scheduleId as any) : undefined,
      });
      toast.success("Site schedule updated");
    } catch {
      toast.error("Failed to update site schedule");
    } finally {
      setUpdatingSiteScheduleId(null);
    }
  };

  const toggleScheduleDay = (day: ScheduleDay) => {
    setScheduleDays((prev) => {
      const next = new Set(prev);
      if (next.has(day)) {
        next.delete(day);
      } else {
        next.add(day);
      }
      return next;
    });
  };

  const handleSaveSiteName = async (siteId: string, nextName: string) => {
    const trimmed = nextName.trim();
    if (!trimmed) {
      toast.error("Name cannot be empty");
      return;
    }
    try {
      setSavingSiteNameId(siteId);
      const res = await updateSiteName({ id: siteId as any, name: trimmed });
      setNameEdits((prev) => ({ ...prev, [siteId]: trimmed }));
      const updatedJobs = (res as any)?.updatedJobs ?? 0;
      if (updatedJobs > 0) {
        toast.success(`Name updated; ${updatedJobs} job${updatedJobs === 1 ? "" : "s"} retagged`);
      } else {
        toast.success("Name updated");
      }
    } catch {
      toast.error("Failed to update name");
    } finally {
      setSavingSiteNameId((prev) => (prev === siteId ? null : prev));
    }
  };

  const handleAutoFixSiteName = async (siteId: string, url: string, currentName: string | undefined) => {
    const suggestion = cleanCompanyName(currentName ?? "", url);
    setNameEdits((prev) => ({ ...prev, [siteId]: suggestion }));
    await handleSaveSiteName(siteId, suggestion);
  };

  const toggleSiteExpanded = (siteId: string) => {
    setExpandedSites((prev) => {
      const next = new Set(prev);
      if (next.has(siteId)) {
        next.delete(siteId);
      } else {
        next.add(siteId);
      }
      return next;
    });
  };

  const handleAddSite = async (e: FormEvent) => {
    e.preventDefault();
    const trimmedUrl = url.trim();
    if (!trimmedUrl) {
      toast.error("URL is required");
      return;
    }
    try {
      const greenhouseSubmission = isGreenhouseUrlString(trimmedUrl);
      const normalizedType = greenhouseSubmission ? "greenhouse" : siteType ?? "general";
      const normalizedPattern = normalizedType === "greenhouse" ? undefined : (pattern.trim() || undefined);
      const generatedName = deriveSiteName(trimmedUrl);
      const normalizedProvider: ScrapeProvider = greenhouseSubmission ? "spidercloud" : scrapeProvider;

      await upsertSite({
        name: generatedName,
        url: trimmedUrl,
        type: normalizedType,
        scrapeProvider: normalizedProvider,
        pattern: normalizedPattern,
        scheduleId: selectedScheduleId || undefined,
        enabled,
      });
      toast.success("Site added");
      setUrl("");
      setPattern("");
      setSiteType("general");
      setScrapeProvider("fetchfox_spidercloud");
      setEnabled(true);
    } catch {
      toast.error("Failed to add site");
    }
  };

  const handleBulkImport = async () => {
    if (!bulkText.trim()) return;

    const lines = bulkText.split("\n").filter(l => l.trim());
    const sitesToInsert: any[] = [];

    for (const line of lines) {
      // Format: url, pattern (optional), type (optional)
      const parts = line.split(",").map(p => p.trim()).filter(Boolean);
      if (parts.length === 0 || !parts[0]) continue;

      const [u, ...rest] = parts;
      let parsedType: "general" | "greenhouse" | undefined;
      let parsedProvider: ScrapeProvider | undefined;
      let parsedPattern: string | undefined;

      for (const segment of rest) {
        const lowered = segment.toLowerCase();
        if (!parsedType && (lowered === "general" || lowered === "greenhouse")) {
          parsedType = lowered as "general" | "greenhouse";
          continue;
        }
        if (!parsedProvider && (lowered === "fetchfox" || lowered === "fetchfox_spidercloud" || lowered === "firecrawl" || lowered === "spidercloud")) {
          parsedProvider = lowered as ScrapeProvider;
          continue;
        }
        if (!parsedPattern) {
          parsedPattern = segment;
        }
      }

      const greenhouseSubmission = isGreenhouseUrlString(u);
      const normalizedType = greenhouseSubmission
        ? "greenhouse"
        : parsedType ?? bulkSiteType ?? "general";
      const normalizedProvider: ScrapeProvider = greenhouseSubmission
        ? "spidercloud"
        : parsedProvider ?? bulkScrapeProvider ?? "fetchfox";
      const patternValue = normalizedType === "greenhouse" ? undefined : parsedPattern;
      const generatedName = deriveSiteName(u);

      sitesToInsert.push({
        url: u,
        name: generatedName,
        pattern: patternValue,
        type: normalizedType,
        scrapeProvider: normalizedProvider,
        scheduleId: bulkScheduleId || selectedScheduleId || undefined,
        enabled: true,
      });
    }

    if (sitesToInsert.length === 0) {
      toast.error("No valid sites found");
      return;
    }

    try {
      await bulkUpsertSites({ sites: sitesToInsert });
      toast.success(`Imported ${sitesToInsert.length} sites`);
      setBulkText("");
    } catch {
      toast.error("Failed to import sites");
    }
  };

  const toggleEnabled = async (id: string, next: boolean) => {
    try {
      await updateSiteEnabled({ id: id as any, enabled: next });
    } catch {
      toast.error("Failed to update site");
    }
  };

  return (
    <div className="bg-slate-900 p-4 rounded border border-slate-800 shadow-sm">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-white">Sites to Scrape</h2>
        <div className="flex bg-slate-950 rounded p-0.5 border border-slate-800">
          <button
            onClick={() => setMode("single")}
            className={clsx(
              "px-3 py-1 text-xs font-medium rounded transition-colors",
              mode === "single" ? "bg-slate-800 text-white shadow-sm" : "text-slate-400 hover:text-slate-200"
            )}
          >
            Single
          </button>
          <button
            onClick={() => setMode("bulk")}
            className={clsx(
              "px-3 py-1 text-xs font-medium rounded transition-colors",
              mode === "bulk" ? "bg-slate-800 text-white shadow-sm" : "text-slate-400 hover:text-slate-200"
            )}
          >
            Bulk Import
          </button>
        </div>
      </div>

      <div
        className="mb-6 rounded border border-slate-800 p-4 space-y-4"
        style={{
          backgroundImage: "linear-gradient(135deg, rgba(15,23,42,0.9), rgba(30,41,59,0.95) 40%, rgba(56,189,248,0.08))",
          backgroundColor: "#0f172a",
        }}
      >
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-sm font-semibold text-white">Schedules</h3>
            <p className="text-xs text-slate-400">Define reusable cadences and assign them to scrape jobs.</p>
          </div>
          {editingScheduleId && (
            <button
              onClick={resetScheduleForm}
              className="text-xs px-3 py-1 rounded border border-slate-700 text-slate-300 hover:bg-slate-800 transition-colors"
            >
              Cancel edit
            </button>
          )}
        </div>

        <div className="grid gap-4 lg:grid-cols-3">
          <div className="space-y-3 lg:col-span-1">
            <div>
              <label className="text-xs text-slate-400 block mb-1">Schedule name</label>
              <input
                type="text"
                placeholder="Weekday mornings"
                className="w-full bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:border-blue-500"
                value={scheduleName}
                onChange={(e) => setScheduleName(e.target.value)}
              />
            </div>
            <div>
              <label className="text-xs text-slate-400 block mb-1">Days</label>
              <div className="inline-flex flex-nowrap divide-x divide-slate-800 rounded overflow-hidden border border-slate-800 bg-slate-900">
                {ALL_SCHEDULE_DAYS.map((day) => (
                  <button
                    key={day}
                    type="button"
                    onClick={() => toggleScheduleDay(day)}
                    className={clsx(
                      "w-10 text-center py-1 text-[10px] font-semibold transition-colors shrink-0 leading-4",
                      scheduleDays.has(day)
                        ? "bg-amber-300 text-slate-900"
                        : "bg-slate-900 text-slate-300 hover:bg-slate-800"
                    )}
                  >
                    {SCHEDULE_DAY_LABELS[day]}
                  </button>
                ))}
              </div>
            </div>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
              <div>
                <label className="text-xs text-slate-400 block mb-1">Start time</label>
                <input
                  type="time"
                  value={scheduleStartTime}
                  onChange={(e) => setScheduleStartTime(e.target.value)}
                  className="w-full bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
                />
              </div>
              <div>
                <label className="text-xs text-slate-400 block mb-1">Timezone</label>
                <input
                  type="text"
                  value={scheduleTimezone}
                  onChange={(e) => setScheduleTimezone(e.target.value || "UTC")}
                  placeholder={defaultTimezone}
                  className="w-full bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
                />
                <p className="text-[11px] text-slate-500 mt-1">IANA name, e.g. America/Denver</p>
              </div>
              <div className="col-span-1 sm:col-span-3">
                <label className="text-xs text-slate-400 block mb-1">Repeat every (HH:MM)</label>
                <div className="flex flex-wrap sm:flex-nowrap items-center gap-2">
                  <input
                    type="number"
                    min={0}
                    value={scheduleIntervalHours}
                    onChange={(e) => setScheduleIntervalHours(parseInt(e.target.value || "0", 10))}
                    className="w-24 sm:w-full bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
                    placeholder="Hours"
                  />
                  <span className="text-slate-500 text-xs">:</span>
                  <input
                    type="number"
                    min={0}
                    max={59}
                    value={scheduleIntervalMinutes}
                    onChange={(e) => setScheduleIntervalMinutes(parseInt(e.target.value || "0", 10))}
                    className="w-24 sm:w-full bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
                    placeholder="Minutes"
                  />
                </div>
              </div>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[11px] text-slate-500">
                {editingScheduleId ? "Editing existing schedule" : "New schedule"}
              </span>
              <button
                onClick={() => { void handleSaveSchedule(); }}
                disabled={savingSchedule}
                className="px-3 py-1.5 bg-emerald-600 text-white text-xs font-medium rounded hover:bg-emerald-500 transition-colors disabled:opacity-60 disabled:cursor-not-allowed"
              >
                {savingSchedule ? "Saving..." : editingScheduleId ? "Update schedule" : "Create schedule"}
              </button>
            </div>
          </div>

          <div className="lg:col-span-2 space-y-2">
            {!schedules && (
              <div className="text-xs text-slate-500 border border-slate-800 rounded bg-slate-950/50 p-3">
                Loading schedules...
              </div>
            )}
            {schedules && schedules.length === 0 && (
              <div className="text-xs text-slate-500 border border-dashed border-slate-800 rounded bg-slate-950/40 p-3">
                No schedules yet. Create one to start assigning scrape jobs.
              </div>
            )}
            {schedules && schedules.length > 0 && (
              <div className="space-y-2">
                {schedules.map((sched: any) => (
                  <div
                    key={sched._id}
                    className="flex items-start justify-between gap-3 p-3 bg-slate-950 border border-slate-800 rounded"
                  >
                    <div className="min-w-0">
                      <div className="flex items-center gap-2 mb-1">
                        <p className="text-sm font-semibold text-white truncate">{sched.name}</p>
                        <span className="text-[10px] text-slate-500">
                          {sched.siteCount === 1 ? "1 site" : `${sched.siteCount} sites`}
                        </span>
                        {(defaultScheduleId && (sched._id as unknown as string) === defaultScheduleId) && (
                          <span className="text-[10px] px-1.5 py-0.5 rounded border border-blue-800 bg-blue-900/30 text-blue-100">
                            Default
                          </span>
                        )}
                      </div>
                      <div className="text-xs text-slate-400 truncate">
                        {formatScheduleSummary(sched)}
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <button
                        onClick={() => setDefaultSchedule(sched._id as unknown as string)}
                        disabled={(sched._id as unknown as string) === defaultScheduleId}
                        className={clsx(
                          "text-[11px] px-2 py-1 rounded border transition-colors",
                          (sched._id as unknown as string) === defaultScheduleId
                            ? "border-blue-900/60 text-blue-200 bg-blue-900/20 cursor-not-allowed"
                            : "border-blue-800 text-blue-100 hover:bg-blue-900/30"
                        )}
                      >
                        {(sched._id as unknown as string) === defaultScheduleId ? "Default" : "Set default"}
                      </button>
                      <button
                        onClick={() => handleEditSchedule(sched)}
                        className="text-[11px] px-2 py-1 rounded border border-slate-700 bg-slate-800 text-slate-200 hover:bg-slate-700 transition-colors"
                      >
                        Edit
                      </button>
                      <button
                        onClick={() => { void handleDeleteSchedule(sched._id as unknown as string); }}
                        disabled={sched.siteCount > 0 || deletingScheduleId === (sched._id as unknown as string)}
                        className={clsx(
                          "text-[11px] px-2 py-1 rounded border transition-colors",
                          sched.siteCount > 0
                            ? "border-slate-800 text-slate-600 cursor-not-allowed"
                            : "border-red-900/50 text-red-300 hover:bg-red-900/20"
                        )}
                      >
                        {deletingScheduleId === (sched._id as unknown as string) ? "Deleting..." : "Delete"}
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>

      {mode === "single" ? (
        <form onSubmit={(e) => { void handleAddSite(e); }} className="space-y-3 mb-6 bg-slate-950/50 p-3 rounded border border-slate-800">
          <div className="grid grid-cols-1 md:grid-cols-12 gap-3 items-start">
            <div className="md:col-span-6">
              <label className="text-xs text-slate-400 block mb-1">Start URL</label>
              <input
                type="url"
                placeholder="Start URL (required)"
                className="w-full bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:border-blue-500"
                value={url}
                onChange={(e) => setUrl(e.target.value)}
                required
              />
              <div className="space-y-1 mt-1 min-h-[32px]">
                {isGreenhouseUrl && (
                  <p className="text-[11px] text-amber-300 leading-snug truncate">
                    Greenhouse board detected. Other fields are locked; name is auto-generated.
                  </p>
                )}
                {!!url.trim() && (
                  <p className="text-[11px] text-slate-500 leading-snug truncate">
                    Will save as <span className="text-slate-200">{generatedName}</span>
                  </p>
                )}
              </div>
            </div>
            <div className="md:col-span-2">
              <label className="text-xs text-slate-400 block mb-1">Site type</label>
              <select
                value={siteType}
                onChange={(e) => {
                  const next = e.target.value as "general" | "greenhouse";
                  setSiteType(next);
                  if (next === "greenhouse") {
                    setPattern("");
                    setScrapeProvider("spidercloud");
                  } else if (!isGreenhouseUrl && scrapeProvider === "spidercloud") {
                    setScrapeProvider("fetchfox_spidercloud");
                  }
                }}
                disabled={isGreenhouseUrl}
                className="w-full bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500 disabled:opacity-60"
              >
                <option value="general">General</option>
                <option value="greenhouse">Greenhouse board</option>
              </select>
            </div>
            <div className="md:col-span-2">
              <label className="text-xs text-slate-400 block mb-1">Scraper</label>
              <select
                value={scrapeProvider}
                onChange={(e) => setScrapeProvider(e.target.value as ScrapeProvider)}
                disabled={isGreenhouseUrl || siteType === "greenhouse"}
                className="w-full bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500 disabled:opacity-60"
              >
                <option value="fetchfox">FetchFox (structured JSON)</option>
                <option value="fetchfox_spidercloud">FetchFox crawl + SpiderCloud detail</option>
                <option value="firecrawl">Firecrawl (webhook)</option>
                <option value="spidercloud">SpiderCloud (streaming markdown)</option>
              </select>
              <p className="text-[11px] text-slate-500 mt-1">SpiderCloud defaults for Greenhouse sites; FetchFox defaults for general sites.</p>
            </div>
            <div className="md:col-span-2">
              <label className="text-xs text-slate-400 block mb-1">Pattern (optional)</label>
              <input
                type="text"
                placeholder="Pattern (optional)"
                className="w-full bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:border-blue-500 disabled:opacity-60"
                value={pattern}
                onChange={(e) => setPattern(e.target.value)}
                disabled={isGreenhouseUrl || siteType === "greenhouse"}
                title={isGreenhouseUrl || siteType === "greenhouse" ? "Greenhouse sites don't need a pattern" : "Optional pattern for detail pages"}
              />
            </div>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-12 gap-3 items-end">
            <div className="md:col-span-6">
              <label className="text-xs text-slate-400 block mb-1">Schedule</label>
              <select
                value={selectedScheduleId}
                onChange={(e) => setSelectedScheduleId(e.target.value)}
                disabled={!schedules}
                className="w-full bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500 disabled:opacity-60"
              >
                {!schedules && <option value="">Loading schedules...</option>}
                <option value="">{schedules ? "No schedule (manual)" : " "}</option>
                {schedules?.map((sched: any) => (
                  <option key={sched._id as unknown as string} value={sched._id as unknown as string}>
                    {sched.name} • {formatScheduleSummary(sched)}
                  </option>
                ))}
              </select>
            </div>
            <div className="md:col-span-3">
              <label className="text-xs text-slate-400 block mb-1">Status</label>
              <div className="flex items-center gap-2">
                <input
                  type="checkbox"
                  className="h-3.5 w-3.5 bg-slate-900 border-slate-700 rounded"
                  checked={enabled}
                  onChange={(e) => setEnabled(e.target.checked)}
                  disabled={isGreenhouseUrl}
                />
                <span className="text-xs text-slate-400">Enabled by default</span>
              </div>
            </div>
            <div className="md:col-span-3 flex md:justify-end">
              <button
                type="submit"
                className="w-full md:w-auto px-3 py-1.5 bg-blue-600 text-white text-xs font-medium rounded hover:bg-blue-500 transition-colors"
              >
                Add Site
              </button>
            </div>
          </div>
        </form>
      ) : (
        <div className="space-y-3 mb-6 bg-slate-950/50 p-3 rounded border border-slate-800">
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <div className="text-xs text-slate-400 sm:col-span-2 lg:col-span-2">
              Paste sites (one per line): <code className="bg-slate-900 px-1 rounded text-slate-300">url, pattern (optional), type/provider (optional)</code>
              <div className="text-[11px] text-slate-500 mt-1">
                Names are auto-generated from the URL. Type can be <code className="bg-slate-900 px-1 rounded text-slate-300">general</code> or <code className="bg-slate-900 px-1 rounded text-slate-300">greenhouse</code>; providers accept <code className="bg-slate-900 px-1 rounded text-slate-300">fetchfox</code>, <code className="bg-slate-900 px-1 rounded text-slate-300">fetchfox_spidercloud</code> (crawl + SpiderCloud detail), <code className="bg-slate-900 px-1 rounded text-slate-300">firecrawl</code>, or <code className="bg-slate-900 px-1 rounded text-slate-300">spidercloud</code>. Greenhouse entries default to SpiderCloud.
              </div>
            </div>
            <div>
              <label className="text-xs text-slate-400 block mb-1">Schedule</label>
              <select
                value={bulkScheduleId || selectedScheduleId}
                onChange={(e) => setBulkScheduleId(e.target.value)}
                className="w-full bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
              >
                {!schedules && <option value="">Loading schedules...</option>}
                <option value="">{schedules ? "No schedule (manual)" : " "}</option>
                {schedules?.map((sched: any) => (
                  <option key={sched._id as unknown as string} value={sched._id as unknown as string}>
                    {sched.name} • {formatScheduleSummary(sched)}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="text-xs text-slate-400 block mb-1">Site type (batch default)</label>
              <select
                value={bulkSiteType}
                onChange={(e) => setBulkSiteType(e.target.value as "general" | "greenhouse")}
                className="w-full bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
              >
                <option value="general">General</option>
                <option value="greenhouse">Greenhouse board</option>
              </select>
            </div>
            <div>
              <label className="text-xs text-slate-400 block mb-1">Scraper (batch default)</label>
              <select
                value={bulkScrapeProvider}
                onChange={(e) => setBulkScrapeProvider(e.target.value as ScrapeProvider)}
                className="w-full bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
              >
                <option value="fetchfox">FetchFox (structured JSON)</option>
                <option value="fetchfox_spidercloud">FetchFox crawl + SpiderCloud detail</option>
                <option value="firecrawl">Firecrawl (webhook)</option>
                <option value="spidercloud">SpiderCloud (streaming markdown)</option>
              </select>
            </div>
          </div>
          <textarea
            value={bulkText}
            onChange={(e) => setBulkText(e.target.value)}
            placeholder="https://example.com/jobs, https://example.com/jobs/**, general"
            className="w-full h-32 bg-slate-900 border border-slate-700 rounded px-2 py-2 text-sm text-slate-200 placeholder-slate-600 focus:outline-none focus:border-blue-500 font-mono"
          />
          <div className="flex justify-end">
            <button
              onClick={() => { void handleBulkImport(); }}
              disabled={!bulkText.trim()}
              className="px-3 py-1.5 bg-blue-600 text-white text-xs font-medium rounded hover:bg-blue-500 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Import Sites
            </button>
          </div>
        </div>
      )}

      <div className="flex items-center justify-between mb-2">
        <p className="text-xs text-slate-500">
          {sites ? `${sites.length} site${sites.length === 1 ? "" : "s"}` : "Loading..."}
        </p>
        <label className="flex items-center gap-2 text-xs text-slate-400 cursor-pointer">
          <input
            type="checkbox"
            className="h-3.5 w-3.5 bg-slate-900 border-slate-700 rounded"
            checked={showDisabled}
            onChange={(e) => setShowDisabled(e.target.checked)}
          />
          <span className="flex items-center gap-1">
            Show disabled
            {disabledCount > 0 && (
              <span className="px-1.5 py-0.5 rounded-full text-[10px] bg-slate-800 text-slate-300 border border-slate-700">
                {disabledCount}
              </span>
            )}
          </span>
        </label>
      </div>

      <div className="border border-slate-800 rounded bg-slate-950/30">
        <div className={`${siteRowColumns} px-3 py-2 bg-slate-900 text-[11px] uppercase tracking-wide text-slate-500 border-b border-slate-800`}>
          <span>Site</span>
          <span>Days</span>
          <span>Time</span>
          <span>Interval</span>
          <span className="text-right">Actions</span>
        </div>
        <div className="divide-y divide-slate-800">
          {sites === undefined && <div className="p-3 text-xs text-slate-500">Loading...</div>}
          {sites && sites.length === 0 && <div className="p-3 text-xs text-slate-500">No sites found.</div>}
          {sites && sites.map((s) => {
            const siteId = s._id as unknown as string;
            const scheduleId = s.scheduleId ? (s.scheduleId as unknown as string) : "";
            const schedule = scheduleId ? scheduleMap.get(scheduleId) : null;
            const scheduleLabel = schedule ? formatScheduleSummary(schedule) : "No schedule";
            const siteType = (s as any).type ?? "general";
            const siteTypeLabel = siteType === "greenhouse" ? "Greenhouse" : "General";
            const scrapeProvider: ScrapeProvider = (s as any).scrapeProvider ?? (siteType === "greenhouse" ? "spidercloud" : "fetchfox_spidercloud");
            const scrapeProviderLabel =
              scrapeProvider === "firecrawl"
                ? "Firecrawl"
                : scrapeProvider === "fetchfox_spidercloud"
                  ? "FetchFox + SpiderCloud"
                  : scrapeProvider === "spidercloud"
                    ? "SpiderCloud"
                    : "FetchFox";
            const scheduleDaysSet = new Set((schedule?.days ?? []) as ScheduleDay[]);
            const timeLabel = schedule ? `${schedule.startTime} ${schedule.timezone}` : "No time";
            const intervalLabel = formatIntervalLabel(schedule?.intervalMinutes ?? 0);
            const isExpanded = expandedSites.has(siteId);
            const pipeline = resolvePipeline(scrapeProvider, siteType);

            return (
              <div key={siteId} className={clsx("p-3 bg-slate-950/20", !s.enabled && "opacity-50")}>
                <div className={`${siteRowColumns} items-center gap-3`}>
                  <div className="flex items-start gap-2 min-w-0">
                    <button
                      onClick={() => toggleSiteExpanded(siteId)}
                      className="mt-0.5 h-6 w-6 rounded bg-slate-900 border border-slate-800 text-slate-200 text-sm hover:bg-slate-800"
                      aria-label={isExpanded ? "Collapse site" : "Expand site"}
                    >
                      {isExpanded ? "−" : "+"}
                    </button>
                    <div className="min-w-0 space-y-1">
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="text-sm font-semibold text-white truncate max-w-[200px]">{s.name || "Untitled"}</span>
                        <span className={clsx("text-[10px] px-1.5 py-0.5 rounded border", s.enabled ? "bg-green-900/20 text-green-400 border-green-900/30" : "bg-slate-800 text-slate-400 border-slate-700")}>
                          {s.enabled ? "Active" : "Disabled"}
                        </span>
                        <span className={clsx(
                          "text-[10px] px-1.5 py-0.5 rounded border",
                          siteType === "greenhouse"
                            ? "bg-amber-900/30 text-amber-200 border-amber-800"
                            : "bg-slate-800 text-slate-300 border-slate-700"
                        )}>
                          {siteTypeLabel}
                        </span>
                        <span className={clsx(
                          "text-[10px] px-1.5 py-0.5 rounded border",
                          scrapeProvider === "firecrawl"
                            ? "bg-blue-900/30 text-blue-200 border-blue-800"
                            : scrapeProvider === "fetchfox_spidercloud"
                              ? "bg-sky-900/40 text-sky-100 border-sky-800"
                              : scrapeProvider === "spidercloud"
                                ? "bg-indigo-900/30 text-indigo-200 border-indigo-800"
                                : "bg-emerald-900/30 text-emerald-200 border-emerald-800"
                        )}>
                          {scrapeProviderLabel}
                        </span>
                      </div>
                      <div className="text-[11px] text-slate-500 truncate font-mono">{s.url}</div>
                    </div>
                  </div>
                  <div className="flex items-center">
                    <div className="inline-flex flex-nowrap divide-x divide-slate-800 rounded overflow-hidden border border-slate-800 bg-slate-900">
                      {ALL_SCHEDULE_DAYS.map((day) => (
                        <span
                          key={day}
                          className={clsx(
                            "w-7 text-center py-0.5 text-[10px] font-semibold transition-colors shrink-0 leading-4",
                            scheduleDaysSet.has(day) ? "bg-slate-800 text-slate-50" : "bg-slate-900 text-slate-500"
                          )}
                        >
                          {SCHEDULE_DAY_LABELS[day]}
                        </span>
                      ))}
                    </div>
                  </div>
                  <div className="text-[11px] text-slate-200">{timeLabel}</div>
                  <div className="text-[11px] text-slate-200">Every {intervalLabel}</div>
                  <div className="flex items-center gap-2 justify-end">
                    <button
                      onClick={() => { void toggleEnabled(siteId, !s.enabled); }}
                      className="px-2 py-1 text-[11px] font-medium rounded border border-slate-700 bg-slate-800 text-slate-300 hover:bg-slate-700 transition-colors whitespace-nowrap"
                    >
                      {s.enabled ? "Disable" : "Enable"}
                    </button>
                    <button
                      onClick={() => {
                        void (async () => {
                          try {
                            await runSiteNow({ id: siteId as any });
                            toast.success("Queued for next scrape run");
                          } catch {
                            toast.error("Failed to queue run");
                          }
                        })();
                      }}
                      className="px-2 py-1 text-[11px] font-medium rounded border border-blue-700 bg-blue-900/40 text-blue-200 hover:bg-blue-800/60 transition-colors whitespace-nowrap"
                      disabled={!s.enabled}
                      title={s.enabled ? "Trigger on next workflow cycle" : "Enable site to run"}
                    >
                      Run now
                    </button>
                  </div>
                </div>

                {isExpanded && (
                  <div className="pt-3 border-t border-slate-800 space-y-3 mt-2">
                    <div className="flex flex-wrap items-center gap-2">
                      <input
                        value={nameEdits[siteId] ?? s.name ?? ""}
                        onChange={(e) => setNameEdits((prev) => ({ ...prev, [siteId]: e.target.value }))}
                        className="w-48 sm:w-64 bg-slate-900 border border-slate-700 rounded px-2 py-1 text-sm text-slate-100 placeholder-slate-500 focus:outline-none focus:border-blue-500"
                        placeholder="Site name"
                      />
                      <button
                        onClick={() => { void handleSaveSiteName(siteId, nameEdits[siteId] ?? s.name ?? ""); }}
                        disabled={savingSiteNameId === siteId || !(nameEdits[siteId] ?? s.name ?? "").trim() || (nameEdits[siteId] ?? s.name ?? "").trim() === (s.name ?? "").trim()}
                        className={clsx(
                          "text-[11px] px-2 py-1 rounded border transition-colors",
                          savingSiteNameId === siteId
                            ? "border-slate-800 text-slate-500 cursor-not-allowed"
                            : "border-emerald-800 text-emerald-200 hover:bg-emerald-900/20"
                        )}
                      >
                        {savingSiteNameId === siteId ? "Saving..." : "Save name"}
                      </button>
                      <button
                        onClick={() => { void handleAutoFixSiteName(siteId, s.url, nameEdits[siteId] ?? s.name ?? ""); }}
                        disabled={savingSiteNameId === siteId}
                        className="text-[11px] px-2 py-1 rounded border border-sky-800 text-sky-100 hover:bg-sky-900/30 transition-colors"
                      >
                        Auto-fix
                      </button>
                    </div>

                    <div className="text-[11px] text-slate-300 space-y-1">
                      <div className="flex flex-wrap gap-2">
                        <span className="px-2 py-1 rounded bg-slate-900 border border-slate-800 text-slate-200">
                          URL: <span className="font-mono text-slate-100">{s.url}</span>
                        </span>
                        {s.pattern && (
                          <span className="px-2 py-1 rounded bg-slate-900 border border-slate-800 text-slate-200">
                            Pattern: <span className="font-mono text-slate-100">{s.pattern}</span>
                          </span>
                        )}
                        <span className="px-2 py-1 rounded bg-slate-900 border border-slate-800 text-slate-200">
                          Schedule: <span className="font-semibold">{schedule?.name ?? "None"}</span>
                        </span>
                      </div>
                      <div className="flex flex-wrap gap-2 items-center">
                        <span className="text-slate-400">Change schedule</span>
                        <select
                          value={scheduleId}
                          onChange={(e) => { void handleSiteScheduleChange(siteId, e.target.value); }}
                          disabled={!schedules || updatingSiteScheduleId === siteId}
                          className="bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-xs text-slate-200 focus:outline-none focus:border-blue-500"
                        >
                          <option value="">No schedule</option>
                          {schedules?.map((sched: any) => (
                            <option key={sched._id as unknown as string} value={sched._id as unknown as string}>
                              {sched.name}
                            </option>
                          ))}
                        </select>
                        <span className="text-slate-500 truncate max-w-[240px]" title={scheduleLabel}>
                          {scheduleLabel}
                        </span>
                      </div>
                    </div>

                    <div className="flex flex-wrap gap-2 text-[11px]">
                      <span className="px-2 py-1 rounded bg-slate-900 border border-slate-800 text-slate-200">Crawler: {pipeline.crawler}</span>
                      <span className="px-2 py-1 rounded bg-slate-900 border border-slate-800 text-slate-200">Scraper: {pipeline.scraper}</span>
                      <span className="px-2 py-1 rounded bg-slate-900 border border-slate-800 text-slate-200">Extractor: {pipeline.extractor}</span>
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function ScrapeActivitySection({ onOpenRuns }: { onOpenRuns: (url: string) => void }) {
  const activity = useQuery(api.sites.listScrapeActivity);
  const [currentTime, setCurrentTime] = useState(Date.now());

  useEffect(() => {
    const id = setInterval(() => setCurrentTime(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  const formatDate = (value?: number | null) => {
    if (!value) return "-";
      return new Date(value).toLocaleString();
  };

  const _formatAge = (value?: number | null) => {
    if (!value) return {label: "—", tone: "text-slate-600" };
      const diff = Date.now() - value;
      const totalSeconds = Math.max(0, Math.floor(diff / 1000));
      const days = Math.floor(totalSeconds / 86400);
      const hours = Math.floor((totalSeconds % 86400) / 3600)
      .toString()
      .padStart(2, "0");
      const minutes = Math.floor((totalSeconds % 3600) / 60)
      .toString()
      .padStart(2, "0");
      const seconds = (totalSeconds % 60).toString().padStart(2, "0");
      const label = `${days}d ${hours}:${minutes}:${seconds}`;

      const tone =
      diff < 3 * 60 * 60 * 1000
      ? "text-green-400"
      : diff < 24 * 60 * 60 * 1000
      ? "text-amber-400"
      : "text-red-400";

      return {label, tone};
  };

  const formatElapsed = (value?: number | null) => {
    if (!value) return {label: "-", tone: "text-slate-600" };
      const diff = Math.max(0, currentTime - value);
      const totalSeconds = Math.floor(diff / 1000);
      const days = Math.floor(totalSeconds / 86400);
      const hours = Math.floor((totalSeconds % 86400) / 3600);
      const minutes = Math.floor((totalSeconds % 3600) / 60)
      .toString()
      .padStart(2, "0");
      const seconds = (totalSeconds % 60).toString().padStart(2, "0");
      const hDisplay = (days * 24 + hours).toString().padStart(2, "0");
      const label = `${hDisplay}:${minutes}:${seconds}`;
      const tone =
      diff < 3 * 60 * 60 * 1000
      ? "text-green-400"
      : diff < 24 * 60 * 60 * 1000
      ? "text-amber-400"
      : "text-red-400";
      return {label, tone};
  };

  const _formatDuration = (start?: number | null, end?: number | null) => {
    if (!start || !end) return "-";
      const diff = Math.max(0, end - start);
      const seconds = Math.floor(diff / 1000);
      const mins = Math.floor(seconds / 60);
      const secs = seconds % 60;
      return `${mins}m ${secs.toString().padStart(2, "0")}s`;
  };

      if (activity === undefined) {
    return (
      <div className="bg-slate-900 p-4 rounded border border-slate-800 shadow-sm text-xs text-slate-500">
        Loading scrape activity...
      </div>
      );
  }

      if (!activity || activity.length === 0) {
    return (
      <div className="bg-slate-900 p-4 rounded border border-slate-800 shadow-sm text-sm text-slate-500">
        No sites configured yet.
      </div>
      );
  }

      return (
      <div className="flex flex-col w-full min-h-[calc(100vh-4rem)]">
        <div className="flex-1 overflow-auto w-full">
          <table className="min-w-full text-left text-sm text-slate-200 font-medium border border-slate-800 rounded-lg shadow-sm overflow-hidden">
            <thead className="bg-slate-900/95 text-[11px] uppercase tracking-wide text-slate-100 sticky top-0 z-10">
              <tr>
                <th className="px-4 py-3 border-b border-slate-800 text-left">URL</th>
                <th className="px-3 py-3 border-b border-slate-800 whitespace-nowrap">Successful run</th>
                <th className="px-3 py-3 border-b border-slate-800 whitespace-nowrap">Last run</th>
                <th className="px-3 py-3 border-b border-slate-800 whitespace-nowrap">Created</th>
                <th className="px-3 py-3 border-b border-slate-800 text-center whitespace-nowrap">Jobs</th>
                <th className="px-3 py-3 border-b border-slate-800 whitespace-nowrap">Worker</th>
                <th className="px-3 py-3 border-b border-slate-800 whitespace-nowrap">Start</th>
                <th className="px-3 py-3 border-b border-slate-800 whitespace-nowrap">End</th>
                <th className="px-3 py-3 border-b border-slate-800 text-center whitespace-nowrap">Runs</th>
                <th className="px-3 py-3 border-b border-slate-800 text-center whitespace-nowrap">Jobs Sum</th>
              </tr>
            </thead>
            <tbody className="bg-slate-950 divide-y divide-slate-800">
              {[...activity]
                .sort((a: any, b: any) => {
                  const lastA = Math.max(a.lastRunAt ?? 0, a.lastFailureAt ?? 0);
                  const lastB = Math.max(b.lastRunAt ?? 0, b.lastFailureAt ?? 0);
                  return lastB - lastA;
                })
                .map((row: any, idx: number) => {
                  const lastAnyRun = Math.max(row.lastRunAt ?? 0, row.lastFailureAt ?? 0);
                  const lastRunFailed = (row.lastFailureAt ?? 0) >= (row.lastRunAt ?? 0);

                  return (
                    <tr
                      key={row.siteId}
                      onClick={() => onOpenRuns(row.url)}
                      className={clsx(
                        "transition-colors cursor-pointer border-b border-slate-800 last:border-b-0",
                        idx % 2 === 0 ? "bg-slate-950" : "bg-slate-900/40",
                        "hover:bg-slate-800/80"
                      )}
                    >
                      <td className="px-4 py-3 align-top">
                        <div className="relative group inline-block">
                          <div className="text-[11px] text-slate-300 font-mono break-words max-w-[320px] relative z-10">
                            {row.url}
                          </div>
                          <div className="absolute left-0 top-full mt-2 w-72 bg-slate-900 border border-slate-800 rounded shadow-lg p-3 opacity-0 pointer-events-none group-hover:opacity-100 group-hover:pointer-events-auto transition z-20">
                            <div className="flex items-center justify-between gap-2 mb-2">
                              <span className="text-sm font-semibold text-white truncate max-w-[200px]">
                                {row.name || "Untitled"}
                              </span>
                              <span
                                className={clsx(
                                  "text-[10px] px-1.5 py-0.5 rounded-full border whitespace-nowrap",
                                  row.enabled
                                    ? "bg-green-900/25 text-green-300 border-green-800"
                                    : "bg-slate-800 text-slate-400 border-slate-700"
                                )}
                              >
                                {row.enabled ? "Active" : "Disabled"}
                              </span>
                            </div>
                            {row.pattern && (
                              <div className="text-[11px] text-slate-400 font-mono break-words">
                                Pattern: {row.pattern}
                              </div>
                            )}
                          </div>
                        </div>
                      </td>
                      <td className="px-3 py-3 align-top">
                        <span className="text-xs text-slate-400 truncate max-w-[200px] inline-block">
                          {(() => {
                            const age = formatElapsed(row.lastRunAt);
                            return <span className={clsx("font-mono font-semibold", age.tone)}>{age.label}</span>;
                          })()}
                        </span>
                      </td>
                      <td className="px-3 py-3 align-top">
                        <div className="flex items-center gap-2 text-xs text-slate-400">
                          {lastRunFailed && <span className="text-red-400 font-bold">✕</span>}
                          {(() => {
                            if (!lastAnyRun) return <span className="text-slate-500">-</span>;
                            const age = formatElapsed(lastAnyRun);
                            return <span className={clsx("font-mono font-semibold", age.tone)}>{age.label}</span>;
                          })()}
                        </div>
                      </td>
                      <td className="px-3 py-3 align-top text-[11px] text-slate-300 whitespace-nowrap">
                        {(() => {
                          const age = formatElapsed(row.createdAt);
                          return <span className={clsx("font-mono font-semibold", age.tone)}>{age.label}</span>;
                        })()}
                      </td>
                      <td className="px-3 py-3 align-top text-center text-sm font-semibold text-slate-100">
                        {row.lastJobsScraped}
                      </td>
                      <td className="px-3 py-3 align-top text-[11px] text-slate-300 whitespace-nowrap font-mono">
                        {row.workerId || "-"}
                      </td>
                      <td className="px-3 py-3 align-top text-[11px] text-slate-300 whitespace-nowrap">
                        {formatDate(row.lastScrapeStart)}
                      </td>
                      <td className="px-3 py-3 align-top text-[11px] text-slate-300 whitespace-nowrap">
                        {formatDate(row.lastScrapeEnd)}
                      </td>
                      <td className="px-3 py-3 align-top text-center text-sm font-semibold text-slate-100">
                        {row.totalScrapes}
                      </td>
                      <td className="px-3 py-3 align-top text-center text-sm font-semibold text-slate-100">
                        {row.totalJobsScraped}
                      </td>
                    </tr>
                  );
                })}
            </tbody>
          </table>
        </div>
      </div>
      );
}

      function WorkerStatusSection() {
  const successfulSites = useQuery(api.sites.listSuccessfulSites, {limit: 100 });
      const failedSites = useQuery(api.sites.listFailedSites, {limit: 100 });
      const retrySite = useMutation(api.sites.retrySite);
      const retryProcessing = useMutation(api.sites.retryProcessing);
      const resetScrapeUrlProcessing = useMutation(api.router.resetScrapeUrlProcessing);
      const resetScrapeUrlsByStatus = useMutation(api.router.resetScrapeUrlsByStatus);
      const rateLimits = useQuery(api.router.listJobDetailRateLimits, { });
      const upsertRateLimit = useMutation(api.router.upsertJobDetailRateLimit);
      const deleteRateLimit = useMutation(api.router.deleteJobDetailRateLimit);
      const [rateDomain, setRateDomain] = useState("");
      const [rateValue, setRateValue] = useState("50");
      const scrapeErrors = useQuery(api.router.listScrapeErrors, {limit: 25 });

      const rows: any[] = [];
      if (successfulSites) {
    for (const s of successfulSites as any[]) rows.push({...s, status: "success" });
  }
      if (failedSites) {
    for (const s of failedSites as any[]) rows.push({...s, status: "failed" });
  }

  const sorted = rows.sort((a, b) => {
    const aTime = a.lastRunAt ?? a.lastFailureAt ?? 0;
      const bTime = b.lastRunAt ?? b.lastFailureAt ?? 0;
      return bTime - aTime;
  });

      return (
      <div className="space-y-4">
        <div className="bg-slate-900 border border-slate-800 rounded shadow-sm overflow-hidden">
          <div className="px-4 py-3 border-b border-slate-800 flex items-center justify-between">
            <div>
              <h2 className="text-sm font-semibold text-white">Worker Status</h2>
              <p className="text-xs text-slate-500">Recent successful/failed site scrapes.</p>
              <p className="text-[11px] text-slate-500 mt-1">
                Use <span className="text-amber-200 font-semibold">Clear failures</span> to reset a stuck site:
                it clears the failed flag and immediately requeues the site for the next scrape cycle.
              </p>
              <p className="text-[11px] text-slate-500">
                <span className="text-blue-200 font-semibold">Retry processing</span> replays existing scraped data for
                the site (no new scrape) and re-ingests jobs, while also clearing failures.
              </p>
              <p className="text-[11px] text-slate-500 mt-1">
                <span className="text-cyan-200 font-semibold">Job detail rate limits</span> control batch scraping for
                individual job URLs (default 50/min per domain). Configure per-domain overrides below.
              </p>
              <p className="text-[11px] text-slate-500 mt-1">
                Use <span className="text-emerald-200 font-semibold">Reset processing</span> to move any stuck job-detail
                URLs back to pending for reprocessing.
              </p>
              <p className="text-[11px] text-slate-500">
                <span className="text-indigo-200 font-semibold">Reset completed</span> will reopen finished job-detail
                URLs (e.g., for re-scrape) and move them back to pending.
              </p>
            </div>
          </div>

          <div className="overflow-auto">
            <table className="min-w-full text-left text-xs text-slate-200">
              <thead className="bg-slate-950 text-[11px] uppercase tracking-wide text-slate-400">
                <tr>
                  <th className="px-3 py-2 border-b border-slate-800">Status</th>
                  <th className="px-3 py-2 border-b border-slate-800">Site</th>
                  <th className="px-3 py-2 border-b border-slate-800">URL</th>
                  <th className="px-3 py-2 border-b border-slate-800 whitespace-nowrap">Last run</th>
                  <th className="px-3 py-2 border-b border-slate-800 whitespace-nowrap">Last failure</th>
                  <th className="px-3 py-2 border-b border-slate-800 whitespace-nowrap">Failures</th>
                  <th className="px-3 py-2 border-b border-slate-800 text-right">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800">
                {sorted.length === 0 && (
                  <tr>
                    <td colSpan={7} className="px-3 py-3 text-center text-slate-500">
                      {successfulSites === undefined || failedSites === undefined ? "Loading..." : "No data yet."}
                    </td>
                  </tr>
                )}
                {sorted.map((row) => (
                  <tr key={row._id} className="hover:bg-slate-800/50 transition-colors">
                    <td className="px-3 py-2">
                      <span
                        className={clsx(
                          "px-2 py-0.5 rounded-full text-[10px] font-semibold border",
                          row.status === "success"
                            ? "bg-green-900/30 text-green-300 border-green-800"
                            : "bg-red-900/30 text-red-300 border-red-800"
                        )}
                      >
                        {row.status === "success" ? "Success" : "Failed"}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-sm text-white truncate max-w-[180px]">{row.name || "Untitled"}</td>
                    <td className="px-3 py-2 text-[11px] text-slate-300 font-mono truncate max-w-[260px]">{row.url}</td>
                    <td className="px-3 py-2 text-[11px] text-slate-300 whitespace-nowrap">
                      {row.lastRunAt ? new Date(row.lastRunAt).toLocaleString() : "—"}
                    </td>
                    <td className="px-3 py-2 text-[11px] text-slate-300 whitespace-nowrap">
                      {row.lastFailureAt ? new Date(row.lastFailureAt).toLocaleString() : row.lastError ? "Failed" : "—"}
                      {row.lastError && (
                        <div className="text-[10px] text-red-300 mt-1 line-clamp-2">{row.lastError}</div>
                      )}
                    </td>
                    <td className="px-3 py-2 text-[11px] text-slate-300 text-center">
                      {row.failCount ?? (row.status === "failed" ? 1 : 0)}
                    </td>
                    <td className="px-3 py-2 text-right">
                      {row.status === "failed" && (
                        <div className="flex items-center justify-end gap-2">
                          <button
                            onClick={() => {
                              void (async () => {
                                try {
                                  await retrySite({ id: row._id, clearError: true });
                                  toast.success("Failures cleared; site requeued");
                                } catch {
                                  toast.error("Failed to clear site errors");
                                }
                              })();
                            }}
                            className="text-[11px] px-2 py-1 rounded border border-amber-700 bg-amber-900/30 text-amber-200 hover:bg-amber-800/40 transition-colors"
                          >
                            Clear failures
                          </button>
                          <button
                            onClick={() => {
                              void (async () => {
                                try {
                                  const res = await retryProcessing({ id: row._id });
                                  toast.success(
                                    `Replayed ${res.jobsAttempted ?? 0} jobs from ${res.scrapesProcessed ?? 0} scrapes`
                                  );
                                } catch (err: any) {
                                  toast.error(`Retry processing failed: ${err?.message ?? "unknown error"}`);
                                }
                              })();
                            }}
                            className="text-[11px] px-2 py-1 rounded border border-blue-700 bg-blue-900/30 text-blue-200 hover:bg-blue-800/40 transition-colors"
                          >
                            Retry processing
                          </button>
                        </div>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded shadow-sm overflow-hidden">
          <div className="px-4 py-3 border-b border-slate-800 flex items-center justify-between">
            <div>
              <h3 className="text-sm font-semibold text-white">Job detail rate limits</h3>
              <p className="text-[11px] text-slate-500">
                Default is 50/minute per domain. Override specific domains here (SpiderCloud job-detail batches).
              </p>
            </div>
            <div className="flex items-center gap-2">
              <input
                value={rateDomain}
                onChange={(e) => setRateDomain(e.target.value)}
                placeholder="domain (e.g., boards.greenhouse.io)"
                className="bg-slate-800 text-slate-200 text-xs px-2 py-1 rounded border border-slate-700"
              />
              <input
                value={rateValue}
                onChange={(e) => setRateValue(e.target.value)}
                placeholder="50"
                type="number"
                min={1}
                className="bg-slate-800 text-slate-200 text-xs px-2 py-1 rounded border border-slate-700 w-20"
              />
              <button
                onClick={() => {
                  void (async () => {
                    const domain = rateDomain.trim();
                    const val = Number(rateValue);
                    if (!domain || !val) return toast.error("Domain and rate are required");
                    try {
                      await upsertRateLimit({ domain, maxPerMinute: val });
                      toast.success("Rate limit saved");
                      setRateDomain("");
                    } catch (err: any) {
                      toast.error(err?.message ?? "Failed to save rate limit");
                    }
                  })();
                }}
                className="text-[11px] px-2 py-1 rounded border border-cyan-700 bg-cyan-900/30 text-cyan-200 hover:bg-cyan-800/40 transition-colors"
              >
                Save
              </button>
              <button
                onClick={() => {
                  void (async () => {
                    try {
                      const res = await resetScrapeUrlProcessing({});
                      toast.success(`Reset ${res.updated ?? 0} processing URLs to pending`);
                    } catch (err: any) {
                      toast.error(err?.message ?? "Failed to reset processing URLs");
                    }
                  })();
                }}
                className="text-[11px] px-2 py-1 rounded border border-emerald-700 bg-emerald-900/30 text-emerald-200 hover:bg-emerald-800/40 transition-colors"
              >
                Reset processing
              </button>
              <button
                onClick={() => {
                  void (async () => {
                    try {
                      const res = await resetScrapeUrlsByStatus({});
                      toast.success(`Reset ${res.updated ?? 0} completed URLs to pending`);
                    } catch (err: any) {
                      toast.error(err?.message ?? "Failed to reset completed URLs");
                    }
                  })();
                }}
                className="text-[11px] px-2 py-1 rounded border border-indigo-700 bg-indigo-900/30 text-indigo-200 hover:bg-indigo-800/40 transition-colors"
              >
                Reset completed
              </button>
            </div>
          </div>
          <div className="divide-y divide-slate-800">
            {(rateLimits as any[] | undefined)?.length ? (
              (rateLimits as any[]).map((row: any) => (
                <div key={row._id} className="flex items-center justify-between px-4 py-2 text-xs text-slate-200">
                  <div className="flex items-center gap-3">
                    <span className="font-mono text-[11px] text-slate-300">{row.domain}</span>
                    <span className="text-slate-400">{row.maxPerMinute}/min</span>
                    <span className="text-slate-500 text-[11px]">
                      window sent: {row.sentInWindow ?? 0} (started {new Date(row.lastWindowStart).toLocaleTimeString()})
                    </span>
                  </div>
                  <button
                    onClick={() => {
                      void (async () => {
                        try {
                          await deleteRateLimit({ id: row._id });
                          toast.success("Rate limit removed");
                        } catch {
                          toast.error("Failed to delete rate limit");
                        }
                      })();
                    }}
                    className="text-[11px] px-2 py-1 rounded border border-red-700 bg-red-900/30 text-red-200 hover:bg-red-800/40 transition-colors"
                  >
                    Delete
                  </button>
                </div>
              ))
            ) : (
              <div className="px-4 py-3 text-[11px] text-slate-500">No overrides configured (using defaults).</div>
            )}
          </div>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded shadow-sm overflow-hidden">
          <div className="px-4 py-3 border-b border-slate-800 flex items-center justify-between">
            <div>
              <h2 className="text-sm font-semibold text-white">Scrape Errors</h2>
              <p className="text-xs text-slate-500">Latest Firecrawl/worker failures captured from webhooks.</p>
            </div>
            <span className="text-[10px] text-slate-500 font-mono">{scrapeErrors?.length ?? 0} recent</span>
          </div>

          <div className="overflow-auto">
            <table className="min-w-full text-left text-xs text-slate-200">
              <thead className="bg-slate-950 text-[11px] uppercase tracking-wide text-slate-400">
                <tr>
                  <th className="px-3 py-2 border-b border-slate-800">Job ID</th>
                  <th className="px-3 py-2 border-b border-slate-800">Source</th>
                  <th className="px-3 py-2 border-b border-slate-800">Status</th>
                  <th className="px-3 py-2 border-b border-slate-800">Error</th>
                  <th className="px-3 py-2 border-b border-slate-800 whitespace-nowrap">When</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800">
                {(scrapeErrors ?? []).length === 0 && (
                  <tr>
                    <td colSpan={5} className="px-3 py-3 text-center text-slate-500">
                      {scrapeErrors === undefined ? "Loading..." : "No errors recorded."}
                    </td>
                  </tr>
                )}
                {(scrapeErrors ?? []).map((err: any) => (
                  <tr key={err._id} className="hover:bg-slate-800/40 transition-colors">
                    <td className="px-3 py-2 font-mono text-[11px] text-slate-300 truncate max-w-[160px]">
                      {err.jobId || "—"}
                    </td>
                    <td className="px-3 py-2">
                      <div className="text-[11px] text-slate-200 truncate max-w-[220px]">{err.sourceUrl || "—"}</div>
                      {err.siteId && <div className="text-[10px] text-slate-500">site: {err.siteId}</div>}
                    </td>
                    <td className="px-3 py-2 text-[11px] text-slate-300">
                      <span className="px-1.5 py-0.5 rounded bg-red-900/30 border border-red-800 text-red-200 text-[10px] font-medium">
                        {err.status || "error"}
                      </span>
                      {err.event && <div className="text-[10px] text-slate-500 mt-0.5">{err.event}</div>}
                    </td>
                    <td className="px-3 py-2 text-[11px] text-red-200 max-w-[260px]">
                      <div className="line-clamp-2 leading-snug">{err.error}</div>
                    </td>
                    <td className="px-3 py-2 text-[10px] text-slate-400 whitespace-nowrap">
                      {err.createdAt ? new Date(err.createdAt).toLocaleString() : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

      </div>
      );
}

      function WorkflowMetaSummary({workflow}: {workflow: WorkflowScheduleMeta }) {
  return (
      <div className="text-[11px] text-slate-400 flex flex-wrap items-center gap-2 mt-1">
        <span className="px-1.5 py-0.5 rounded bg-slate-800/80 border border-slate-700 text-slate-100 font-medium">
          {workflow.name}
        </span>
        <span className="px-1.5 py-0.5 rounded bg-slate-950/80 border border-slate-800">Schedule: {workflow.scheduleId}</span>
        <span className="px-1.5 py-0.5 rounded bg-slate-950/80 border border-slate-800">
          Cadence: every {formatInterval(workflow.intervalSeconds)}
        </span>
        {workflow.taskQueue && (
          <span className="px-1.5 py-0.5 rounded bg-slate-950/80 border border-slate-800">Queue: {workflow.taskQueue}</span>
        )}
      </div>
      );
}

      function PendingRequestsSection() {
  const runRequests = useQuery(api.router.listRunRequests, {limit: 25 });
      const pendingWebhooks = useQuery(api.router.listPendingFirecrawlWebhooks, {limit: 25 });

      return (
      <div className="space-y-4">
        <div className="bg-slate-900 border border-slate-800 rounded shadow-sm overflow-hidden">
          <div className="px-4 py-3 border-b border-slate-800 flex items-center justify-between">
            <div>
              <h2 className="text-sm font-semibold text-white">Pending Requests</h2>
              <p className="text-xs text-slate-500">{SITE_LEASE_WORKFLOW.description}</p>
              <WorkflowMetaSummary workflow={SITE_LEASE_WORKFLOW} />
            </div>
            <span className="text-[10px] text-slate-500 font-mono">{runRequests?.length ?? 0} pending</span>
          </div>
          <div className="overflow-auto">
            <table className="min-w-full text-left text-xs text-slate-200">
              <thead className="bg-slate-950 text-[11px] uppercase tracking-wide text-slate-400">
                <tr>
                  <th className="px-3 py-2 border-b border-slate-800">Site</th>
                  <th className="px-3 py-2 border-b border-slate-800">Status</th>
                  <th className="px-3 py-2 border-b border-slate-800">Elapsed</th>
                  <th className="px-3 py-2 border-b border-slate-800 whitespace-nowrap">ETA</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800">
                {(runRequests ?? []).length === 0 && (
                  <tr>
                    <td colSpan={4} className="px-3 py-3 text-center text-slate-500">
                      {runRequests === undefined ? "Loading..." : "No pending run requests."}
                    </td>
                  </tr>
                )}
                {(runRequests ?? []).map((req: any) => (
                  <tr key={req._id} className="hover:bg-slate-800/40 transition-colors">
                    <td className="px-3 py-2">
                      <div className="text-[11px] text-slate-200 truncate max-w-[220px]">{req.siteUrl || "—"}</div>
                      <div className="text-[10px] text-slate-500 font-mono">{String(req.siteId)}</div>
                    </td>
                    <td className="px-3 py-2">
                      <span
                        className={clsx(
                          "px-1.5 py-0.5 rounded border text-[10px] font-medium",
                          req.status === "done"
                            ? "bg-green-900/30 text-green-200 border-green-800"
                            : req.status === "processing"
                              ? "bg-amber-900/30 text-amber-200 border-amber-800"
                              : "bg-slate-900/50 text-slate-300 border-slate-700"
                        )}
                      >
                        {req.status}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-[11px] text-slate-300">
                      {req.createdAt ? (
                        <LiveTimer
                          startTime={req.createdAt}
                          colorize
                          warnAfterMs={2 * 60 * 1000}
                          dangerAfterMs={10 * 60 * 1000}
                          showAgo
                        />
                      ) : (
                        "—"
                      )}
                    </td>
                    <td className="px-3 py-2 text-[10px] text-slate-400 whitespace-nowrap">
                      {req.expectedEta ? new Date(req.expectedEta).toLocaleTimeString() : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded shadow-sm overflow-hidden">
          <div className="px-4 py-3 border-b border-slate-800 flex items-center justify-between">
            <div>
              <h2 className="text-sm font-semibold text-white">Pending Firecrawl Webhooks</h2>
              <p className="text-xs text-slate-500">{PROCESS_WEBHOOK_WORKFLOW.description}</p>
              <WorkflowMetaSummary workflow={PROCESS_WEBHOOK_WORKFLOW} />
            </div>
            <span className="text-[10px] text-slate-500 font-mono">{pendingWebhooks?.length ?? 0} pending</span>
          </div>
          <div className="overflow-auto">
            <table className="min-w-full text-left text-xs text-slate-200">
              <thead className="bg-slate-950 text-[11px] uppercase tracking-wide text-slate-400">
                <tr>
                  <th className="px-3 py-2 border-b border-slate-800">Job</th>
                  <th className="px-3 py-2 border-b border-slate-800">Site</th>
                  <th className="px-3 py-2 border-b border-slate-800">Event</th>
                  <th className="px-3 py-2 border-b border-slate-800">Received</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800">
                {(pendingWebhooks ?? []).length === 0 && (
                  <tr>
                    <td colSpan={4} className="px-3 py-3 text-center text-slate-500">
                      {pendingWebhooks === undefined ? "Loading..." : "No pending webhooks."}
                    </td>
                  </tr>
                )}
                {(pendingWebhooks ?? []).map((hook: any) => (
                  <tr key={hook._id} className="hover:bg-slate-800/40 transition-colors">
                    <td className="px-3 py-2 font-mono text-[11px] text-slate-300 truncate max-w-[180px]">
                      {hook.jobId || "—"}
                    </td>
                    <td className="px-3 py-2">
                      <div className="text-[11px] text-slate-200 truncate max-w-[220px]">
                        {hook.siteUrl || (hook.metadata || {}).siteUrl || "—"}
                      </div>
                      <div className="text-[10px] text-slate-500 font-mono">{hook.siteId || (hook.metadata || {}).siteId || ""}</div>
                    </td>
                    <td className="px-3 py-2 text-[11px] text-slate-300">{hook.event || "—"}</td>
                    <td className="px-3 py-2 text-[11px] text-slate-300">
                      {hook.receivedAt ? (
                        <LiveTimer
                          startTime={hook.receivedAt}
                          colorize
                          warnAfterMs={2 * 60 * 1000}
                          dangerAfterMs={10 * 60 * 1000}
                          showAgo
                        />
                      ) : (
                        "—"
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
      );
}

      function DatabaseSection() {
  const insertFakeJobs = useMutation(api.seedData.insertFakeJobs);
      const normalizeDevTestJobs = useMutation(api.jobs.normalizeDevTestJobs);
      const reparseAllJobs = useMutation(api.jobs.reparseAllJobs);
      const deleteJob = useMutation(api.jobs.deleteJob);
      const recentJobs = useQuery(api.jobs.getRecentJobs);

  const handleInsertFakeJobs = async () => {
    try {
      const result = await insertFakeJobs({ });
      toast.success(result.message);
    } catch {
        toast.error("Failed to insert fake jobs");
    }
  };

      return (
      <div className="space-y-4">
        <div className="bg-slate-900 p-4 rounded border border-slate-800 shadow-sm">
          <h2 className="text-lg font-semibold text-white mb-4">Actions</h2>
          <div className="flex flex-wrap gap-3">
            <button
              onClick={() => { void handleInsertFakeJobs(); }}
              className="px-3 py-1.5 bg-indigo-600 text-white text-sm font-medium rounded hover:bg-indigo-500 transition-colors"
            >
              Insert 10 Fake Jobs
            </button>
            <button
              onClick={() => {
                void (async () => {
                  try {
                    const res = await normalizeDevTestJobs({});
                    toast.success(`Normalized ${res.updated} jobs`);
                  } catch {
                    toast.error("Failed to normalize");
                  }
                })();
              }}
              className="px-3 py-1.5 bg-emerald-600 text-white text-sm font-medium rounded hover:bg-emerald-500 transition-colors"
            >
              Normalize Dev/Test Jobs
            </button>
            <button
              onClick={() => {
                void (async () => {
                  try {
                    const res = await reparseAllJobs({});
                    toast.success(`Re-parsed ${res.updated} of ${res.scanned} jobs`);
                  } catch (err: any) {
                    toast.error(err?.message ?? "Failed to re-parse");
                  }
                })();
              }}
              className="px-3 py-1.5 bg-amber-600 text-white text-sm font-medium rounded hover:bg-amber-500 transition-colors"
            >
              Re-parse All Jobs
            </button>
          </div>
        </div>

        <div className="bg-slate-900 p-4 rounded border border-slate-800 shadow-sm">
          <h2 className="text-lg font-semibold text-white mb-4">Current Jobs ({recentJobs?.length || 0})</h2>
          <div className="space-y-2">
            {recentJobs ? (
              recentJobs.map((job) => (
                <div key={job._id} className="flex items-center justify-between p-3 bg-slate-950/30 border border-slate-800 rounded hover:border-slate-700 transition-colors group">
                  <div className="min-w-0">
                    <h3 className="text-sm font-medium text-slate-200 truncate">{job.title}</h3>
                    <p className="text-xs text-slate-500 truncate">{job.company} • {job.location}</p>
                  </div>
                  <div className="flex items-center gap-3 pl-4">
                    <span className="text-[10px] text-slate-600 font-mono whitespace-nowrap">
                      {new Date(job.postedAt).toLocaleDateString()}
                    </span>
                    <button
                      onClick={() => {
                        void (async () => {
                          try {
                            await deleteJob({ jobId: job._id as any });
                            toast.success("Deleted");
                          } catch {
                            toast.error("Failed");
                          }
                        })();
                      }}
                      className="opacity-0 group-hover:opacity-100 px-2 py-1 text-[10px] bg-red-900/20 text-red-400 border border-red-900/30 rounded hover:bg-red-900/40 transition-all"
                    >
                      Delete
                    </button>
                  </div>
                </div>
              ))
            ) : (
              <p className="text-sm text-slate-500">Loading...</p>
            )}
          </div>
        </div>
      </div>
      );
}
