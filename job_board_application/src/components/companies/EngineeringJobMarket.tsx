import { useState } from "react";
import { useQuery } from "convex/react";
import { api } from "../../../convex/_generated/api";
import { CompanyIcon } from "../CompanyIcon";
import { LiveTimer } from "../LiveTimer";

type SortBy = "recent" | "count";

interface EngineeringJobMarketProps {
  onCompanyClick: (companyName: string) => void;
}

export function EngineeringJobMarket({ onCompanyClick }: EngineeringJobMarketProps) {
  const [sortBy, setSortBy] = useState<SortBy>("recent");

  const companies = useQuery(api.jobs.listEngineeringCompanyStats, {
    sortBy,
    limit: 200,
  });

  return (
    <div className="flex-1 overflow-y-auto px-6 py-4">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h2 className="text-lg font-semibold text-white">Engineering Job Market</h2>
          <p className="text-xs text-slate-500">Companies with engineering roles, sorted by activity.</p>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-slate-400 mr-2">
            {(companies?.length ?? 0).toLocaleString()} companies
          </span>
          <div className="flex rounded-lg border border-slate-700 bg-slate-800/50 p-0.5">
            <button
              type="button"
              onClick={() => setSortBy("recent")}
              className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
                sortBy === "recent"
                  ? "bg-blue-600 text-white"
                  : "text-slate-400 hover:text-slate-200"
              }`}
            >
              By Recent
            </button>
            <button
              type="button"
              onClick={() => setSortBy("count")}
              className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
                sortBy === "count"
                  ? "bg-blue-600 text-white"
                  : "text-slate-400 hover:text-slate-200"
              }`}
            >
              By Count
            </button>
          </div>
        </div>
      </div>

      {!companies && (
        <div className="text-sm text-slate-500">Loading engineering companies...</div>
      )}

      {companies && companies.length === 0 && (
        <div className="text-sm text-slate-500">No engineering companies available yet.</div>
      )}

      {companies && companies.length > 0 && (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          {companies.map((company) => (
            <button
              key={company.name}
              type="button"
              onClick={() => onCompanyClick(company.name)}
              className="group text-left rounded-xl border border-slate-800 bg-slate-900/60 p-4 hover:border-blue-500/60 hover:bg-slate-900/80 transition-colors"
            >
              <div className="flex items-center gap-3">
                <CompanyIcon
                  company={company.name}
                  size={34}
                  url={company.sampleUrl ?? undefined}
                />
                <div className="min-w-0">
                  <div className="text-sm font-semibold text-slate-100 truncate">{company.name}</div>
                  <div className="text-xs text-slate-400">
                    {company.engineerCount30d.toLocaleString()} engineer jobs in 30d
                  </div>
                </div>
              </div>
              <div className="mt-3 flex items-center justify-between gap-3">
                <span className="text-[10px] uppercase tracking-wider text-slate-500">
                  {sortBy === "count" ? "30-day count" : "Last posted"}
                </span>
                {sortBy === "count" ? (
                  <span className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-full border border-blue-500/30 bg-blue-500/10 text-blue-200 font-mono">
                    {company.engineerCount30d.toLocaleString()}
                  </span>
                ) : (
                  <span className="inline-flex items-center gap-1 text-[10px] px-2 py-1 rounded-full border border-slate-800 bg-slate-950/60 text-slate-200">
                    <LiveTimer
                      startTime={company.engineerLastPostedAt}
                      showAgo
                      showSeconds={false}
                      className="text-[10px] font-mono text-slate-200"
                      suffixClassName="text-slate-400"
                    />
                  </span>
                )}
              </div>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
