import { useQuery } from "convex/react";
import { api } from "../../convex/_generated/api";
import { useEffect, useState } from "react";
import clsx from "clsx";

type Props = { url: string | null; onBack: () => void };

const temporalUiBase = (import.meta as any).env?.VITE_TEMPORAL_UI as string | undefined;
const temporalNamespace = ((import.meta as any).env?.VITE_TEMPORAL_NAMESPACE as string | undefined) ?? "default";
const resolvedTemporalUiBase = (temporalUiBase || "http://localhost:8233").replace(/\/+$/, "");

const formatElapsed = (value: number | null | undefined, now: number) => {
  if (!value) return { label: "-", tone: "text-slate-600" };
  const diff = now - value;
  const totalSeconds = Math.max(0, Math.floor(diff / 1000));
  const hours = Math.floor(totalSeconds / 3600)
    .toString()
    .padStart(2, "0");
  const minutes = Math.floor((totalSeconds % 3600) / 60)
    .toString()
    .padStart(2, "0");
  const seconds = (totalSeconds % 60).toString().padStart(2, "0");
  const label = `${hours}:${minutes}:${seconds}`;
  const tone =
    diff < 3 * 60 * 60 * 1000
      ? "text-green-400"
      : diff < 24 * 60 * 60 * 1000
        ? "text-amber-400"
        : "text-red-400";
  return { label, tone };
};

const formatDuration = (start?: number | null, end?: number | null) => {
  if (!start || !end) return "-";
  const diff = Math.max(0, end - start);
  const seconds = Math.floor(diff / 1000);
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return `${mins}m ${secs.toString().padStart(2, "0")}s`;
};

export function WorkflowRunsSection({ url, onBack }: Props) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  const runs = useQuery(
    api.temporal.listWorkflowRunsByUrl,
    url ? { url, limit: 50 } : { url: "", limit: 50 }
  );

  if (!url) {
    return (
      <div className="bg-slate-900 border border-slate-800 rounded p-4 text-sm text-slate-400">
        Choose a site from Scrape Activity to view workflow runs.
        <button
          onClick={onBack}
          className="ml-2 text-xs text-blue-300 hover:text-white underline"
        >
          Back
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <div className="text-xs text-slate-500">Workflow runs for</div>
          <div className="text-sm text-slate-200 font-mono break-all">{url}</div>
        </div>
        <button
          onClick={onBack}
          className="text-xs text-slate-400 hover:text-white px-2 py-1 border border-slate-700 rounded"
        >
          Back
        </button>
      </div>
      {!temporalUiBase && (
        <div className="text-[11px] text-slate-500">
          Using default Temporal UI base <span className="font-mono text-slate-300">{resolvedTemporalUiBase}</span>.
          Set <code className="bg-slate-900 px-1 rounded text-slate-200">VITE_TEMPORAL_UI</code> to your UI host if different.
        </div>
      )}

      {runs === undefined && <div className="text-xs text-slate-500">Loading runs...</div>}
      {runs && runs.length === 0 && <div className="text-xs text-slate-500">No runs recorded yet.</div>}
      {runs && runs.length > 0 && (
        <div className="space-y-2">
          {runs.map((run: any) => {
            const duration = formatDuration(run.startedAt, run.completedAt);
            const statusColor =
              run.status === "completed"
                ? "bg-green-900/30 text-green-200 border-green-800"
                : "bg-red-900/30 text-red-200 border-red-800";
            const temporalLink =
              run.workflowId && run.runId
                ? `${resolvedTemporalUiBase}/namespaces/${encodeURIComponent(
                    temporalNamespace
                  )}/workflows/${encodeURIComponent(run.workflowId)}/${encodeURIComponent(run.runId)}`
                : null;
            return (
              <div key={run._id} className="border border-slate-800 rounded p-3 bg-slate-950/50">
                <div className="flex items-center justify-between gap-3 mb-2">
                  <div className="text-sm text-slate-200 font-mono truncate">{run.runId}</div>
                  <span className={clsx("text-[10px] px-2 py-1 rounded-full border", statusColor)}>
                    {run.status}
                  </span>
                </div>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-2 text-[11px] text-slate-300">
                  <div>
                    <div className="text-[10px] text-slate-500">Started</div>
                    <div className="flex items-center gap-2">
                      <span>{run.startedAt ? new Date(run.startedAt).toLocaleString() : "-"}</span>
                      {run.startedAt && (
                        <span
                          className={clsx(
                            "text-[10px] font-mono px-2 py-0.5 rounded-full border border-slate-800 bg-slate-950/70",
                            formatElapsed(run.startedAt, now).tone
                          )}
                        >
                          {formatElapsed(run.startedAt, now).label} ago
                        </span>
                      )}
                    </div>
                  </div>
                  <div>
                    <div className="text-[10px] text-slate-500">Ended</div>
                    <div className="flex items-center gap-2">
                      <span>{run.completedAt ? new Date(run.completedAt).toLocaleString() : "-"}</span>
                      {run.completedAt && (
                        <span
                          className={clsx(
                            "text-[10px] font-mono px-2 py-0.5 rounded-full border border-slate-800 bg-slate-950/70",
                            formatElapsed(run.completedAt, now).tone
                          )}
                        >
                          {formatElapsed(run.completedAt, now).label} ago
                        </span>
                      )}
                    </div>
                  </div>
                  <div>
                    <div className="text-[10px] text-slate-500">Duration</div>
                    <div>{duration}</div>
                  </div>
                  <div>
                    <div className="text-[10px] text-slate-500">Worker</div>
                    <div className="font-mono text-[11px] text-slate-300 truncate">{run.workerId || "-"}</div>
                  </div>
                </div>
                <div className="mt-2 text-[10px] text-slate-500 flex flex-wrap gap-2">
                  <span>Workflow: {run.workflowName || run.workflowId}</span>
                  <span>Sites: {run.siteUrls?.length || 0}</span>
                  <span>Jobs scraped: {run.jobsScraped ?? 0}</span>
                  {temporalLink && (
                    <a
                      href={temporalLink}
                      target="_blank"
                      rel="noreferrer"
                      className="text-blue-300 hover:text-white underline"
                    >
                      Open in Temporal UI
                    </a>
                  )}
                </div>
                {run.error && (
                  <div className="mt-2 text-[11px] text-red-300 bg-red-900/20 border border-red-800/50 rounded p-2 font-mono break-all">
                    {run.error}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
