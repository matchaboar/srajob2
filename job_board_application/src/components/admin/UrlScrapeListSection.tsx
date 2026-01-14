import { useQuery } from "convex/react";
import { api } from "../../../convex/_generated/api";
import clsx from "clsx";
import { ExpandableJsonCell } from "./ExpandableJsonCell";

export function UrlScrapeListSection() {
  const logs = useQuery(api.router.listUrlScrapeLogs, { limit: 200, includeJobLookup: true });

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

  const describeReason = (row: any) => {
    const reason = row.reason as string | undefined;
    const normalizedCount = typeof row.normalizedCount === "number" ? row.normalizedCount : 0;
    const rawUrlCount = typeof row.rawUrlCount === "number" ? row.rawUrlCount : 0;
    const hasExistingJob = Boolean(row.jobId);
    const fallbackDetail = hasExistingJob ? "Matched an existing job in Job Board." : undefined;

    switch (reason) {
      case "already_saved":
        return {
          label: "Already in Job Board",
          detail: "URL matches an existing job; skipped ingestion.",
          tone: "bg-amber-900/30 text-amber-200 border-amber-800",
        };
      case "listing_only":
        return {
          label: "Listing URLs only",
          detail:
            rawUrlCount > 0
              ? `Extracted ${rawUrlCount} URL${rawUrlCount === 1 ? "" : "s"} without job payloads.`
              : "Response had URLs but no job payloads.",
          tone: "bg-slate-900/60 text-slate-200 border-slate-700",
        };
      case "no_items_existing_job":
        return {
          label: "No items returned",
          detail: "Provider returned no items; URL already exists in Job Board.",
          tone: "bg-amber-900/30 text-amber-200 border-amber-800",
        };
      case "no_items":
        return {
          label: "No items returned",
          detail:
            normalizedCount > 0
              ? `Normalized ${normalizedCount} item${normalizedCount === 1 ? "" : "s"} but none had URLs.`
              : fallbackDetail ?? "Provider returned zero items for this URL.",
          tone: "bg-amber-900/30 text-amber-200 border-amber-800",
        };
      case "missing_url":
        return {
          label: "Missing job URL",
          detail: "Normalized item missing a URL field.",
          tone: "bg-amber-900/30 text-amber-200 border-amber-800",
        };
      default:
        if (!reason) {
          return {
            label: row.action === "scraped" ? "Scraped" : "Skipped",
            detail: fallbackDetail,
            tone:
              row.action === "scraped"
                ? "bg-emerald-900/30 text-emerald-200 border-emerald-800"
                : "bg-slate-900/60 text-slate-200 border-slate-700",
          };
        }
        return {
          label: reason,
          detail: fallbackDetail,
          tone: "bg-slate-900/60 text-slate-200 border-slate-700",
        };
    }
  };

  const TimestampCell = ({ timestamp }: { timestamp?: number | string }) => {
    const parsed = typeof timestamp === "string" ? Date.parse(timestamp) : timestamp;
    if (!parsed || Number.isNaN(parsed)) return <span className="text-slate-600">—</span>;

    const formatted = new Date(parsed).toLocaleString();

    return (
      <span className="block truncate font-mono text-[10px] text-slate-200" title={formatted}>
        {formatted}
      </span>
    );
  };

  return (
    <div className="flex flex-col w-full h-full min-h-screen bg-slate-950">
      <div className="flex items-center justify-end px-4 py-3 border-b border-slate-900 bg-slate-950">
        <span className="text-xs text-slate-400">Showing {logs.length}</span>
      </div>
      <div className="flex-1 overflow-hidden">
        <div className="h-full overflow-auto">
          <table className="min-w-full w-full text-left text-[10px] text-slate-200 table-fixed">
            <thead className="bg-slate-800 text-slate-50 uppercase tracking-wide border-b border-slate-700 shadow-inner sticky top-0 z-10">
              <tr>
                <th className="px-2 py-1 w-56 font-bold">URL</th>
                <th className="px-2 py-1 w-36 font-bold">Timestamp</th>
                <th className="px-2 py-1 w-56 font-bold">Description URI</th>
                <th className="px-2 py-1 w-40 font-bold">Reason</th>
                <th className="px-2 py-1 w-20 font-bold">Action</th>
                <th className="px-2 py-1 w-24 font-bold">Provider</th>
                <th className="px-2 py-1 w-32 font-bold">Workflow</th>
                <th className="px-2 py-1 w-44 font-bold">Workflow ID</th>
                <th className="px-2 py-1 w-72 font-bold">Request</th>
                <th className="px-2 py-1 w-72 font-bold">Response</th>
                <th className="px-2 py-1 w-72 font-bold">Async Response</th>
              </tr>
            </thead>
            <tbody className="bg-slate-950 divide-y divide-slate-800">
              {logs.map((row: any, idx: number) => (
                <tr key={`${row.url}-${idx}`} className="hover:bg-slate-900 transition-colors h-7">
                  <td className="px-2 py-1 align-middle truncate">
                    {row.url ? (
                      <a
                        href={row.url}
                        className="text-blue-300 hover:text-blue-100 underline truncate block"
                        title={row.url}
                      >
                        {row.url}
                      </a>
                    ) : (
                      "—"
                    )}
                  </td>
                  <td className="px-2 py-1 align-middle truncate">
                    <TimestampCell timestamp={row.timestamp} />
                  </td>
                  <td className="px-2 py-1 align-middle truncate">
                    {row.jobUrl ? (
                      <a
                        href={row.jobUrl}
                        className="text-emerald-300 hover:text-emerald-200 font-semibold underline underline-offset-2 truncate block"
                        title={[row.jobUrl, row.jobTitle, row.jobCompany].filter(Boolean).join(" • ")}
                      >
                        {row.jobUrl}
                      </a>
                    ) : (
                      <span className="text-slate-600">—</span>
                    )}
                  </td>
                  <td className="px-2 py-1 align-middle truncate">
                    {(() => {
                      const info = describeReason(row);
                      return (
                        <span
                          className={clsx(
                            "px-1.5 py-0.5 rounded text-[9px] font-semibold border w-fit",
                            info.tone
                          )}
                          title={info.detail || info.label}
                        >
                          {info.label}
                        </span>
                      );
                    })()}
                  </td>
                  <td className="px-2 py-1 align-middle truncate">
                    <span
                      className={clsx(
                        "px-1.5 py-0.5 rounded text-[9px] font-semibold uppercase",
                        row.action === "skipped"
                          ? "bg-amber-900/40 text-amber-200 border border-amber-800"
                          : "bg-green-900/40 text-green-200 border border-green-800"
                      )}
                    >
                      {row.action || "n/a"}
                    </span>
                  </td>
                  <td className="px-2 py-1 align-middle truncate" title={row.provider || "—"}>
                    {row.provider || "—"}
                  </td>
                  <td className="px-2 py-1 align-middle truncate" title={row.workflow || "—"}>
                    {row.workflow || "—"}
                  </td>
                  <td className="px-2 py-1 align-middle truncate">
                    <span className="text-slate-200 font-mono text-[10px]">{row.workflowId || "—"}</span>
                  </td>
                  <td className="px-2 py-1 align-middle">
                    <ExpandableJsonCell value={row.requestData} />
                  </td>
                  <td className="px-2 py-1 align-middle">
                    <ExpandableJsonCell value={row.response} />
                  </td>
                  <td className="px-2 py-1 align-middle">
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
