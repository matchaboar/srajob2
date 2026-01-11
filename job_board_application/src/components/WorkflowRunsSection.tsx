import { useQuery } from "convex/react";
import { api } from "../../convex/_generated/api";
import { useMemo } from "react";
import { LiveTimer } from "./LiveTimer";

type Props = { url: string | null; onBack: () => void };

type PendingWebhookRow = {
  id: string;
  receivedAt: number | null;
  statusUrl: string | null;
  siteId: string | null;
  waitingFor: string;
};

export function WorkflowRunsSection({ url, onBack }: Props) {
  const pendingWebhooks = useQuery(api.router.listPendingFirecrawlWebhooks, { limit: 50 });

  const pendingForUrl = useMemo(() => {
    if (!url || !pendingWebhooks || !Array.isArray(pendingWebhooks)) return [] as PendingWebhookRow[];
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
        } as PendingWebhookRow;
      })
      .filter(Boolean) as PendingWebhookRow[];
  }, [pendingWebhooks, url]);

  if (!url) {
    return (
      <div className="bg-slate-900 border border-slate-800 rounded p-4 text-sm text-slate-400">
        Choose a site from Scrape Activity to view webhook activity.
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
          <div className="text-xs text-slate-500">Webhook activity for</div>
          <div className="text-sm text-slate-200 font-mono break-all">{url}</div>
        </div>
        <button
          onClick={onBack}
          className="text-xs text-slate-400 hover:text-white px-2 py-1 border border-slate-700 rounded"
        >
          Back
        </button>
      </div>

      <div className="bg-slate-900 border border-slate-800 rounded p-4">
        <div className="text-xs text-slate-500 mb-2">
          DBOS run metadata lives in SQLite; webhook activity is shown below.
        </div>
        {pendingForUrl.length === 0 ? (
          <div className="text-xs text-slate-500">No pending webhooks for this site.</div>
        ) : (
          <div className="space-y-2">
            {pendingForUrl.map((row) => (
              <div key={row.id} className="border border-slate-800 rounded p-2 text-xs text-slate-300">
                <div className="flex items-center justify-between">
                  <span className="font-mono text-slate-200">{row.id}</span>
                  <span className="text-[10px] text-slate-500">{row.waitingFor}</span>
                </div>
                <div className="mt-1 flex flex-wrap gap-3 text-[11px] text-slate-400">
                  {row.receivedAt ? (
                    <span>
                      Received <LiveTimer startTime={row.receivedAt} showAgo colorize />
                    </span>
                  ) : (
                    <span>Received -</span>
                  )}
                  {row.statusUrl && <span className="truncate">Status URL: {row.statusUrl}</span>}
                  {row.siteId && <span>Site ID: {row.siteId}</span>}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
