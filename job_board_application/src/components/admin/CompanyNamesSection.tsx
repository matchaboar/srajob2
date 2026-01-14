import { useQuery, useMutation } from "convex/react";
import { api } from "../../../convex/_generated/api";
import { toast } from "sonner";
import { useState } from "react";
import clsx from "clsx";

export function CompanyNamesSection() {
  const domainAliases = useQuery(api.router.listDomainAliases);
  const setDomainAlias = useMutation(api.router.setDomainAlias);
  const [drafts, setDrafts] = useState<Record<string, string>>({});
  const [savingDomain, setSavingDomain] = useState<string | null>(null);
  const [domainSearch, setDomainSearch] = useState("");
  const [showAllDomains, setShowAllDomains] = useState(false);

  const handleSave = async (domain: string, fallbackAlias: string, siteUrl?: string) => {
    const nextAlias = (drafts[domain] ?? fallbackAlias ?? "").trim();
    if (!nextAlias) {
      toast.error("Alias cannot be empty");
      return;
    }
    try {
      setSavingDomain(domain);
      const res = await setDomainAlias({ domainOrUrl: siteUrl || domain, alias: nextAlias });
      const updatedJobs = (res as any)?.updatedJobs ?? 0;
      const updatedSites = (res as any)?.updatedSites ?? 0;
      const jobMessage =
        updatedJobs > 0 ? ` • ${updatedJobs} job${updatedJobs === 1 ? "" : "s"} retagged` : "";
      const siteMessage =
        updatedSites > 0 ? ` • ${updatedSites} site${updatedSites === 1 ? "" : "s"} renamed` : "";
      toast.success(`Alias saved${jobMessage}${siteMessage}`);
      setDrafts((prev) => ({ ...prev, [domain]: nextAlias }));
    } catch {
      toast.error("Failed to save alias");
    } finally {
      setSavingDomain((prev) => (prev === domain ? null : prev));
    }
  };

  const handleResetDraft = (domain: string, derivedName?: string) => {
    setDrafts((prev) => {
      const next = { ...prev };
      if (derivedName) {
        next[domain] = derivedName;
      } else {
        delete next[domain];
      }
      return next;
    });
  };

  if (domainAliases === undefined) {
    return <div className="text-slate-400 p-4">Loading company names...</div>;
  }

  if (!domainAliases?.length) {
    return (
      <div className="bg-slate-900 p-4 rounded border border-slate-800 shadow-sm">
        <div className="flex items-center justify-between mb-2">
          <div>
            <h2 className="text-lg font-semibold text-white">Company Names</h2>
            <p className="text-xs text-slate-400">
              Map scrape domains to the names that should appear on every job.
            </p>
          </div>
        </div>
        <div className="text-slate-400 text-sm p-4 text-center border border-slate-800 rounded bg-slate-950/30">
          No scrape domains found yet.
        </div>
      </div>
    );
  }

  const rows = domainAliases as any[];
  const domainSearchLower = domainSearch.trim().toLowerCase();
  const hasAlias = (row: any) => typeof row?.alias === "string" && row.alias.trim().length > 0;
  const searchFilteredRows = !domainSearchLower
    ? rows
    : rows.filter((row) => {
        const fields = [row.domain, row.derivedName, row.alias, row.siteName, row.siteUrl]
          .filter((v) => typeof v === "string")
          .map((v) => v.toLowerCase());
        return fields.some((v) => v.includes(domainSearchLower));
      });
  const filteredRows =
    showAllDomains || domainSearchLower ? searchFilteredRows : searchFilteredRows.filter(hasAlias);
  const hiddenCount =
    showAllDomains || domainSearchLower ? 0 : rows.length - filteredRows.length;
  const countLabel =
    showAllDomains || domainSearchLower
      ? `${filteredRows.length} of ${rows.length} domains`
      : `${filteredRows.length} aliased domain${filteredRows.length === 1 ? "" : "s"}${hiddenCount ? ` • ${hiddenCount} hidden` : ""}`;

  return (
    <div className="bg-slate-900 p-4 rounded border border-slate-800 shadow-sm">
      <div className="flex items-start justify-between mb-4">
        <div>
          <h2 className="text-lg font-semibold text-white">Company Names</h2>
          <p className="text-xs text-slate-400 max-w-2xl">
            Give each scrape domain a clean, human-friendly company name. Aliases are applied to
            historical jobs and to all future scrapes for that domain. This is the single place to
            manage company display names.
          </p>
        </div>
        <div className="text-[11px] text-slate-400 px-2 py-1 border border-slate-800 rounded bg-slate-950/50">
          {countLabel}
        </div>
      </div>

      <div className="mb-3 flex flex-wrap items-center gap-3">
        <input
          type="text"
          value={domainSearch}
          onChange={(e) => setDomainSearch(e.target.value)}
          placeholder="Search domains or aliases..."
          className="flex-1 min-w-[220px] md:max-w-sm bg-slate-950 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-100 placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
        />
        <label className="flex items-center gap-2 text-xs text-slate-400">
          <input
            type="checkbox"
            checked={showAllDomains}
            onChange={(e) => setShowAllDomains(e.target.checked)}
            className="h-4 w-4 rounded border-slate-600 bg-slate-900 text-emerald-500 focus:ring-emerald-500"
          />
          Show domains without aliases
        </label>
      </div>
      <p className="text-[11px] text-slate-500 mb-3">
        Search filters the list; edit aliases in the table below.
      </p>

      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm text-slate-200">
          <thead className="text-[11px] uppercase tracking-wide bg-slate-950 text-slate-400 border border-slate-800">
            <tr>
              <th className="px-3 py-2 border-r border-slate-800 w-1/2">Domain</th>
              <th className="px-3 py-2">Company name</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {filteredRows.length === 0 ? (
              <tr className="bg-slate-950/50">
                <td colSpan={2} className="px-3 py-6 text-center text-sm text-slate-500">
                  {domainSearchLower
                    ? "No domains match your search."
                    : showAllDomains
                      ? "No domains found."
                      : "No domains with aliases yet."}
                </td>
              </tr>
            ) : (
              filteredRows.map((row) => {
                const draftValue = drafts[row.domain] ?? row.alias ?? row.derivedName ?? "";
                const usesAlias =
                  (row.alias ?? row.derivedName ?? "") !== (row.derivedName ?? "");
                return (
                  <tr key={row.domain} className="bg-slate-950/50 hover:bg-slate-900/60">
                    <td className="px-3 py-3 align-top">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="font-mono text-xs text-white">{row.domain}</div>
                          {row.siteUrl && (
                            <div
                              className="text-[11px] text-slate-500 truncate"
                              title={row.siteUrl}
                            >
                              {row.siteUrl}
                            </div>
                          )}
                          <div className="text-[11px] text-slate-500 mt-1">
                            Derived: {row.derivedName}
                          </div>
                        </div>
                        {row.siteName && (
                          <div className="shrink-0 inline-flex mt-0.5 px-2 py-0.5 rounded text-[10px] bg-slate-800 text-slate-200 border border-slate-700">
                            {row.siteName}
                          </div>
                        )}
                      </div>
                    </td>
                    <td className="px-3 py-3 align-top">
                      <div className="flex flex-col gap-2">
                        <div className="flex flex-wrap items-center gap-2">
                          <input
                            type="text"
                            value={draftValue}
                            onChange={(e) =>
                              setDrafts((prev) => ({ ...prev, [row.domain]: e.target.value }))
                            }
                            placeholder={row.derivedName}
                            className="flex-1 min-w-[12rem] bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-100 placeholder-slate-500 focus:outline-none focus:border-emerald-500"
                          />
                          <button
                            type="button"
                            onClick={() => {
                              void handleSave(
                                row.domain,
                                draftValue || row.derivedName,
                                row.siteUrl
                              );
                            }}
                            disabled={savingDomain === row.domain}
                            className={clsx(
                              "px-3 py-1.5 rounded text-sm font-medium transition-colors",
                              savingDomain === row.domain
                                ? "bg-emerald-800/60 text-emerald-100 cursor-not-allowed"
                                : "bg-emerald-600 text-white hover:bg-emerald-500"
                            )}
                          >
                            {savingDomain === row.domain ? "Saving..." : "Save alias"}
                          </button>
                          <button
                            type="button"
                            onClick={() => handleResetDraft(row.domain, row.derivedName)}
                            className="px-3 py-1.5 rounded text-sm font-medium bg-slate-800 text-slate-200 border border-slate-700 hover:bg-slate-700 transition-colors"
                          >
                            Reset
                          </button>
                        </div>
                        <div className="flex items-center gap-2 text-[11px] text-slate-500">
                          {usesAlias ? (
                            <span className="px-2 py-0.5 rounded bg-emerald-900/30 text-emerald-200 border border-emerald-800">
                              Alias applied
                            </span>
                          ) : (
                            <span className="px-2 py-0.5 rounded bg-slate-800 text-slate-300 border border-slate-700">
                              Using derived name
                            </span>
                          )}
                          {row.updatedAt && (
                            <span className="text-[10px] text-slate-500">
                              Updated {new Date(row.updatedAt).toLocaleString()}
                            </span>
                          )}
                        </div>
                      </div>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
