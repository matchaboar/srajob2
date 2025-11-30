import { useQuery } from "convex/react";
import { api } from "../../convex/_generated/api";
import { useEffect, useMemo, useState, type ReactNode } from "react";
import clsx from "clsx";
import { LiveTimer } from "./LiveTimer";

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

const formatDataPreview = (data: any) => {
  if (data === null || data === undefined) return "";
  try {
    const text = typeof data === "string" ? data : JSON.stringify(data, null, 2);
    if (text.length > 800) return `${text.slice(0, 800)}… (+${text.length - 800} chars)`;
    return text;
  } catch {
    return String(data);
  }
};

function ScratchpadEntries({ runId }: { runId: string }) {
  const entries = useQuery(api.scratchpad.listByRun, runId ? { runId, limit: 10 } : { runId: "", limit: 10 });

  if (!runId) {
    return null;
  }

  if (entries === undefined) {
    return <div className="text-[11px] text-slate-500">Loading scratchpad…</div>;
  }

  if (!entries || entries.length === 0) {
    return <div className="text-[11px] text-slate-500">No scratchpad entries yet.</div>;
  }

  return (
    <div className="space-y-2">
      {entries.map((entry: any) => {
        const rows: { key: string; value: ReactNode }[] = [
          {
            key: "event",
            value: (
              <span
                className={clsx(
                  "inline-flex items-center gap-1 px-2 py-0.5 rounded-full border text-[10px] font-mono",
                  entry.level === "error"
                    ? "border-red-800 text-red-300 bg-red-900/30"
                    : entry.level === "warn"
                      ? "border-amber-800 text-amber-300 bg-amber-900/30"
                      : "border-slate-700 text-slate-200 bg-slate-900"
                )}
              >
                {entry.event}
              </span>
            ),
          },
        ];

        if (entry.message) {
          rows.push({ key: "message", value: <span className="text-slate-100">{entry.message}</span> });
        }

        if (entry.data !== undefined && entry.data !== null) {
          rows.push({
            key: "data",
            value: (
              <pre className="text-[10px] text-slate-200 bg-slate-950/60 border border-slate-900/70 rounded p-2 whitespace-pre-wrap break-words overflow-x-auto">
                {formatDataPreview(entry.data)}
              </pre>
            ),
          });
        }

        const createdLabel = entry.createdAt ? new Date(entry.createdAt).toLocaleString() : "";

        return (
          <div key={entry._id} className="border border-slate-800 rounded bg-slate-950/40 overflow-hidden">
            <table className="w-full text-[11px] text-slate-300">
              <tbody>
                {rows.map((row, idx) => (
                  <tr key={row.key} className={idx === 0 ? "" : "border-t border-slate-800/70"}>
                    {idx === 0 && (
                      <td
                        rowSpan={rows.length}
                        className="w-48 align-top bg-slate-950/60 border-r border-slate-800 px-3 py-2 text-left"
                      >
                        <div className="flex flex-col gap-1">
                          <span className="font-mono text-[10px] text-slate-400">{createdLabel}</span>
                          {entry.createdAt && (
                            <span className="inline-flex items-center gap-1 text-[10px] px-2 py-1 rounded-full border border-slate-800 bg-slate-900/70 text-slate-200 w-fit">
                              <LiveTimer
                                startTime={entry.createdAt}
                                colorize
                                warnAfterMs={5 * 60 * 1000}
                                dangerAfterMs={30 * 60 * 1000}
                                showAgo
                                suffixClassName="text-slate-400"
                              />
                            </span>
                          )}
                        </div>
                      </td>
                    )}
                    <td className="w-28 uppercase tracking-wide text-[10px] text-slate-500 px-2 py-2 align-top">{row.key}</td>
                    <td className="px-2 py-2 align-top text-slate-100 break-words">{row.value}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        );
      })}
    </div>
  );
}

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
  const pendingWebhooks = useQuery(api.router.listPendingFirecrawlWebhooks, { limit: 50 });

  const pendingForUrl = useMemo(() => {
    if (!url || !pendingWebhooks || !Array.isArray(pendingWebhooks)) return [];
    return pendingWebhooks
      .map((event: any) => {
        const meta = event?.metadata && typeof event.metadata === "object" ? event.metadata : {};
        const siteUrl = event?.siteUrl || meta?.siteUrl || meta?.sourceUrl || meta?.url;
        if (siteUrl !== url) return null;
        return {
          id: event?.jobId ?? meta?.jobId ?? event?._id ?? "unknown",
          receivedAt: event?.receivedAt ?? null,
          statusUrl:
            event?.statusUrl ||
            event?.status_url ||
            meta?.statusUrl ||
            meta?.status_url ||
            meta?.status_endpoint ||
            null,
          siteId: event?.siteId ?? meta?.siteId ?? null,
          waitingFor: event?.event || "webhook",
        };
      })
      .filter(Boolean) as {
      id: string;
      receivedAt: number | null;
      statusUrl: string | null;
      siteId: string | null;
      waitingFor: string;
    }[];
  }, [pendingWebhooks, url]);

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
      {pendingForUrl.length > 0 && (
        <div className="border border-amber-800 rounded bg-amber-900/20 p-3 text-[12px] text-amber-100 space-y-2">
          <div className="text-[11px] uppercase tracking-wide text-amber-200">Waiting for webhooks</div>
          <div className="space-y-2">
            {pendingForUrl.map((item) => (
              <div
                key={`${item.id}-${item.statusUrl ?? "n/a"}`}
                className="border border-amber-700/60 rounded bg-amber-950/30 p-2"
              >
                <div className="flex flex-wrap items-center gap-2 text-[11px]">
                  <span className="font-mono text-amber-100">job {item.id}</span>
                  {item.siteId && (
                    <span className="text-amber-200/80">
                      site <span className="font-mono">{item.siteId}</span>
                    </span>
                  )}
                  <span className="text-amber-200/80">waiting for {item.waitingFor}</span>
                  <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full border border-amber-700 bg-amber-900/50 text-[10px]">
                    <LiveTimer
                      startTime={item.receivedAt ?? Date.now()}
                      colorize
                      warnAfterMs={15 * 60 * 1000}
                      dangerAfterMs={60 * 60 * 1000}
                      showAgo
                      suffixClassName="text-amber-200/70"
                    />
                  </span>
                </div>
                {item.statusUrl && (
                  <div className="text-[10px] text-amber-200/80 break-all mt-1">
                    status: <span className="font-mono">{item.statusUrl}</span>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
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
                <div className="mt-3">
                  <div className="text-[10px] uppercase text-slate-500 mb-1">Scratchpad</div>
                  {run.runId ? (
                    <ScratchpadEntries runId={run.runId} />
                  ) : (
                    <div className="text-[11px] text-slate-500">No run id available.</div>
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
