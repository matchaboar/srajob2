import { useQuery, useMutation } from "convex/react";
import { api } from "../../../convex/_generated/api";
import { useState, useEffect } from "react";
import clsx from "clsx";
import { toast } from "sonner";

export function WorkerStatusSection() {
  const successfulSites = useQuery(api.sites.listSuccessfulSites, { limit: 100 });
  const failedSites = useQuery(api.sites.listFailedSites, { limit: 100 });
  const retrySite = useMutation(api.sites.retrySite);
  const retryProcessing = useMutation(api.sites.retryProcessing);
  const clearIgnoredJobsForSource = useMutation(api.router.clearIgnoredJobsForSource);
  const scrapeErrors = useQuery(api.router.listScrapeErrors, { limit: 25 });
  const [dbosStatus, setDbosStatus] = useState<any | null>(null);
  const [dbosStatusError, setDbosStatusError] = useState<string | null>(null);

  useEffect(() => {
    let mounted = true;
    const loadStatus = async () => {
      try {
        const res = await fetch("/api/workflows/status");
        if (!res.ok) {
          throw new Error(`DBOS status failed (${res.status})`);
        }
        const data = await res.json();
        if (mounted) {
          setDbosStatus(data);
          setDbosStatusError(null);
        }
      } catch (err: any) {
        if (mounted) {
          setDbosStatusError(err?.message ?? "Failed to load DBOS status");
        }
      }
    };
    void loadStatus();
    const interval = window.setInterval(loadStatus, 10_000);
    return () => {
      mounted = false;
      window.clearInterval(interval);
    };
  }, []);

  const rows: any[] = [];
  if (successfulSites) {
    for (const s of successfulSites as any[]) rows.push({ ...s, status: "success" });
  }
  if (failedSites) {
    for (const s of failedSites as any[]) rows.push({ ...s, status: "failed" });
  }

  const sorted = rows.sort((a, b) => {
    const aTime = a.lastRunAt ?? a.lastFailureAt ?? 0;
    const bTime = b.lastRunAt ?? b.lastFailureAt ?? 0;
    return bTime - aTime;
  });

  return (
    <div className="space-y-4">
      <div className="bg-slate-900 border border-slate-800 rounded shadow-sm overflow-hidden">
        <div className="px-4 py-3 border-b border-slate-800">
          <h2 className="text-sm font-semibold text-white">DBOS Queue Status</h2>
          <p className="text-xs text-slate-500">Live snapshot from the DBOS runner.</p>
        </div>
        <div className="px-4 py-3">
          {dbosStatusError && <div className="text-xs text-amber-300">{dbosStatusError}</div>}
          {!dbosStatus && !dbosStatusError && (
            <div className="text-xs text-slate-400">Loading DBOS status...</div>
          )}
          {dbosStatus && (
            <div className="grid grid-cols-2 gap-3 text-xs text-slate-300">
              <div className="rounded border border-slate-800 bg-slate-950/40 p-3">
                <div className="text-[11px] uppercase text-slate-500">Listing Queue</div>
                <div className="mt-2 space-y-1">
                  <div>Pending: <span className="text-white font-mono">{dbosStatus.listing?.pending ?? 0}</span></div>
                  <div>Processing: <span className="text-white font-mono">{dbosStatus.listing?.processing ?? 0}</span></div>
                  <div>Completed: <span className="text-white font-mono">{dbosStatus.listing?.completed ?? 0}</span></div>
                  <div>Failed: <span className="text-white font-mono">{dbosStatus.listing?.failed ?? 0}</span></div>
                </div>
              </div>
              <div className="rounded border border-slate-800 bg-slate-950/40 p-3">
                <div className="text-[11px] uppercase text-slate-500">Job Detail Queue</div>
                <div className="mt-2 space-y-1">
                  <div>Pending: <span className="text-white font-mono">{dbosStatus.detail?.pending ?? 0}</span></div>
                  <div>Processing: <span className="text-white font-mono">{dbosStatus.detail?.processing ?? 0}</span></div>
                  <div>Completed: <span className="text-white font-mono">{dbosStatus.detail?.completed ?? 0}</span></div>
                  <div>Failed: <span className="text-white font-mono">{dbosStatus.detail?.failed ?? 0}</span></div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
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
                    <div className="flex items-center justify-end gap-2">
                      <button
                        onClick={() => {
                          void (async () => {
                            try {
                              const res = await clearIgnoredJobsForSource({
                                sourceUrl: row.url,
                                provider: row.scrapeProvider,
                                reason: "missing_required_keyword",
                              });
                              toast.success(`Cleared ${res.deleted ?? 0} skipped jobs`);
                            } catch (err: any) {
                              toast.error(err?.message ?? "Failed to clear skipped jobs");
                            }
                          })();
                        }}
                        className="text-[11px] px-2 py-1 rounded border border-purple-700 bg-purple-900/30 text-purple-200 hover:bg-purple-800/40 transition-colors"
                      >
                        Clear skipped
                      </button>
                      {row.status === "failed" && (
                        <>
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
                        </>
                      )}
                    </div>
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
