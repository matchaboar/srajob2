interface QueueDurationInfo {
  key: string;
  durationLabel: string;
  status?: string;
  hostname?: string;
}

interface ScrapeInfoSectionProps {
  scrapedAt?: number | null;
  scrapedWith?: string | null;
  workflowName?: string | null;
  scrapedCostMilliCents?: number | null;
  sourceUrl?: string | null;
  scrapeUrl?: string | null;
  queueDurationInfos?: QueueDurationInfo[];
}

function renderScrapeCost(milliCents: number): string {
  const cents = milliCents / 1000;
  if (cents >= 100) {
    return `$${(cents / 100).toFixed(2)}`;
  }
  return `${cents.toFixed(2)}¢`;
}

export function ScrapeInfoSection({
  scrapedAt,
  scrapedWith,
  workflowName,
  scrapedCostMilliCents,
  sourceUrl,
  scrapeUrl,
  queueDurationInfos = [],
}: ScrapeInfoSectionProps) {
  return (
    <div className="rounded-lg border border-slate-800/70 bg-slate-900/40 p-2 space-y-2">
      <div className="text-[10px] uppercase tracking-wider font-semibold text-slate-500">Scrape Info</div>
      <div className="flex items-start gap-2 text-sm text-slate-200">
        <span className="w-28 text-slate-500">Scraped</span>
        <span className="font-semibold text-slate-100 break-words">
          {typeof scrapedAt === "number"
            ? new Date(scrapedAt).toLocaleString(undefined, {
                month: "short",
                day: "numeric",
                hour: "2-digit",
                minute: "2-digit",
              })
            : "None"}
          {scrapedWith ? ` • ${scrapedWith}` : ""}
        </span>
      </div>
      <div className="flex items-start gap-2 text-sm text-slate-200">
        <span className="w-28 text-slate-500">Workflow</span>
        <span className="font-semibold text-slate-100 break-words">{workflowName || "None"}</span>
      </div>
      <div className="flex items-start gap-2 text-sm text-slate-200">
        <span className="w-28 text-slate-500">Scrape Cost</span>
        <span className="font-semibold text-slate-100 break-words">
          {typeof scrapedCostMilliCents === "number" ? renderScrapeCost(scrapedCostMilliCents) : "None"}
        </span>
      </div>
      <div className="flex items-start gap-2 text-sm text-slate-200">
        <span className="w-28 text-slate-500">source_url</span>
        {sourceUrl ? (
          <a
            href={sourceUrl}
            target="_blank"
            rel="noreferrer"
            className="font-mono text-xs text-blue-300 hover:text-blue-200 break-all"
          >
            {sourceUrl}
          </a>
        ) : (
          <span className="font-semibold text-slate-100 break-words">None</span>
        )}
      </div>
      <div className="flex items-start gap-2 text-sm text-slate-200">
        <span className="w-28 text-slate-500">scrape_url</span>
        {scrapeUrl ? (
          <a
            href={scrapeUrl}
            target="_blank"
            rel="noreferrer"
            className="font-mono text-xs text-blue-300 hover:text-blue-200 break-all"
          >
            {scrapeUrl}
          </a>
        ) : (
          <span className="font-semibold text-slate-100 break-words">None</span>
        )}
      </div>
      {queueDurationInfos.length > 0 && (
        <div className="flex items-start gap-2 text-sm text-slate-200">
          <span className="w-28 text-slate-500">Queued</span>
          <div className="flex flex-col gap-1">
            {queueDurationInfos.map((info) => (
              <span key={info.key} className="font-semibold text-slate-100 break-words">
                {info.durationLabel}
                {info.status ? ` • ${info.status}` : ""}
                {info.hostname ? ` • ${info.hostname}` : ""}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export type { QueueDurationInfo };
