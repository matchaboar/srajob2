import { useQuery } from "convex/react";
import { api } from "../../../convex/_generated/api";
import { useState, useEffect } from "react";
import clsx from "clsx";

interface ScrapeActivitySectionProps {
  onOpenRuns: (url: string) => void;
}

export function ScrapeActivitySection({ onOpenRuns }: ScrapeActivitySectionProps) {
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
    if (!value) return { label: "—", tone: "text-slate-600" };
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

    return { label, tone };
  };

  const formatElapsed = (value?: number | null) => {
    if (!value) return { label: "-", tone: "text-slate-600" };
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
    return { label, tone };
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
