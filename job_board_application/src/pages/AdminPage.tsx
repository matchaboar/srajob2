import { useMutation, useQuery } from "convex/react";
import { api } from "../../convex/_generated/api";
import { toast } from "sonner";
import { useState, useEffect, useMemo, useRef } from "react";
import type { FormEvent } from "react";
import clsx from "clsx";
import { WorkflowRunsSection } from "../components/WorkflowRunsSection";
import { LiveTimer } from "../components/LiveTimer";
import {
  UrlScrapeListSection,
  CompanyNamesSection,
  ScrapeActivitySection,
  WorkerStatusSection,
  DatabaseSection,
} from "../components/admin";
import type { Id } from "../../convex/_generated/dataModel";
import {
  toTitleCaseSlug,
  baseDomainFromHost,
  safeParseUrl,
  isGreenhouseUrlString,
  greenhouseSlugFromUrl,
} from "../lib/domainUtils";

type AdminSection = "scraper" | "activity" | "activityRuns" | "worker" | "database" | "urlScrapes" | "companyNames";
type AdminSectionExtended = AdminSection;
type ScheduleDay = "mon" | "tue" | "wed" | "thu" | "fri" | "sat" | "sun";
type ScrapeProvider = "fetchfox" | "firecrawl" | "spidercloud" | "fetchfox_spidercloud";
type ScheduleId = Id<"scrape_schedules">;
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
const DEFAULT_SCHEDULE_STORAGE_KEY = "admin-default-schedule-id";

const resolveScrapeUrl = (rawUrl: string, siteType?: string): string => {
  const parsed = safeParseUrl(rawUrl);
  if (!parsed) return (rawUrl || "").trim();
  parsed.hash = "";
  const host = (parsed.hostname || "").toLowerCase();
  const isGreenhouse = siteType === "greenhouse" || host.includes("greenhouse");

  if (isGreenhouse) {
    const slug = greenhouseSlugFromUrl(parsed.toString());
    if (slug) return `https://boards.greenhouse.io/v1/boards/${slug}/jobs`;
  }

  if (host.endsWith("github.careers")) {
    const cleanPath = parsed.pathname.replace(/\/+$/, "");
    if (cleanPath === "/api/jobs") {
      return parsed.toString();
    }
    const params = new URLSearchParams(parsed.search);
    params.delete("page");
    const base = `${parsed.protocol}//${parsed.hostname}/api/jobs`;
    const query = params.toString();
    return query ? `${base}?${query}` : base;
  }

  if (host.endsWith("ashbyhq.com")) {
    const slug = parsed.pathname.split("/").filter(Boolean)[0];
    if (slug) return `https://api.ashbyhq.com/posting-api/job-board/${slug}`;
  }

  return parsed.toString();
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

const resolvePipeline = (provider: ScrapeProvider, siteType?: string) => {
  const normalized = provider || (siteType === "greenhouse" ? "spidercloud" : "spidercloud");
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


export function AdminPage() {
  // Use URL hash to persist active section across refreshes
  const parseHash = () => {
    const raw = window.location.hash.replace("#admin-", "");
    const [section, query] = raw.split("?");
    const urlParam = new URLSearchParams(query || "").get("url");
    const allowed = ["scraper", "activity", "activityRuns", "worker", "database", "urlScrapes", "companyNames"] as const;
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
            label="Company Names"
            active={section === "companyNames"}
            onClick={() => setNavState({ section: "companyNames", runsUrl: null })}
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
            label="Database"
            active={section === "database"}
            onClick={() => setNavState({ section: "database", runsUrl: null })}
          />
          <SidebarItem
            label="URL scrape list"
            active={section === "urlScrapes"}
            onClick={() => setNavState({ section: "urlScrapes", runsUrl: null })}
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
          {section === "scraper" && (
            <ScraperConfigSection onOpenCompanyNames={() => setNavState({ section: "companyNames", runsUrl: null })} />
          )}
          {section === "companyNames" && <CompanyNamesSection />}
          {section === "activity" && <ScrapeActivitySection onOpenRuns={(url) => setNavState({ section: "activityRuns", runsUrl: url })} />}
          {section === "activityRuns" && <WorkflowRunsSection url={runsUrl} onBack={() => setNavState({ section: "activity", runsUrl: null })} />}
          {section === "worker" && <WorkerStatusSection />}
          {section === "database" && <DatabaseSection />}
          {section === "urlScrapes" && <UrlScrapeListSection />}
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


function ScraperConfigSection({ onOpenCompanyNames }: { onOpenCompanyNames?: () => void }) {
  const [showDisabled, setShowDisabled] = useState(false);
  const sites = useQuery(api.router.listSites, { enabledOnly: !showDisabled });
  const allSites = useQuery(api.router.listSites, { enabledOnly: false });
  const disabledCount = allSites ? allSites.filter((s: any) => !s.enabled).length : 0;
  const schedules = useQuery(api.router.listSchedules);
  const upsertSite = useMutation(api.router.upsertSite);
  const bulkUpsertSites = useMutation(api.router.bulkUpsertSites);
  const runSiteNow = useMutation(api.router.runSiteNow);
  const updateSiteEnabled = useMutation(api.router.updateSiteEnabled);
  const updateSiteSchedule = useMutation(api.router.updateSiteSchedule);
  const upsertSchedule = useMutation(api.router.upsertSchedule);
  const deleteSchedule = useMutation(api.router.deleteSchedule);
  const deleteSite = useMutation(api.router.deleteSite);

  const [mode, setMode] = useState<"single" | "bulk">("single");
  const [selectedScheduleId, setSelectedScheduleId] = useState<ScheduleId | "">("");
  const [bulkScheduleId, setBulkScheduleId] = useState<ScheduleId | "">("");
  const [defaultScheduleId, setDefaultScheduleId] = useState<ScheduleId | "">(() => {
    if (typeof window === "undefined") return "";
    try {
      return (window.localStorage.getItem(DEFAULT_SCHEDULE_STORAGE_KEY) as ScheduleId | "") || "";
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
  const [editingScheduleId, setEditingScheduleId] = useState<ScheduleId | null>(null);
  const [savingSchedule, setSavingSchedule] = useState(false);
  const [deletingScheduleId, setDeletingScheduleId] = useState<ScheduleId | null>(null);
  const [updatingSiteScheduleId, setUpdatingSiteScheduleId] = useState<string | null>(null);
  const [deletingSiteId, setDeletingSiteId] = useState<string | null>(null);
  const [expandedSites, setExpandedSites] = useState<Set<string>>(new Set());
  const siteRowColumns = "grid grid-cols-[minmax(0,1.7fr)_minmax(0,1.05fr)_minmax(0,0.8fr)_minmax(0,0.8fr)_minmax(0,0.7fr)]";

  // Single add state
  const [url, setUrl] = useState("");
  const [siteType, setSiteType] = useState<"general" | "greenhouse">("general");
  const [scrapeProvider, setScrapeProvider] = useState<ScrapeProvider>("spidercloud");
  const [pattern, setPattern] = useState("");
  const [enabled, setEnabled] = useState(true);

  // Bulk add state
  const [bulkText, setBulkText] = useState("");
  const [bulkSiteType, setBulkSiteType] = useState<"general" | "greenhouse">("general");
  const [bulkScrapeProvider, setBulkScrapeProvider] = useState<ScrapeProvider>("spidercloud");
  const [dbosStatus, setDbosStatus] = useState<any | null>(null);
  const [dbosStatusError, setDbosStatusError] = useState<string | null>(null);

  const isGreenhouseUrl = useMemo(() => isGreenhouseUrlString(url), [url]);
  const generatedName = useMemo(() => deriveSiteName(url), [url]);
  const previewScrapeUrl = useMemo(() => {
    if (!url.trim()) return "";
    const normalizedType = isGreenhouseUrl ? "greenhouse" : siteType;
    return resolveScrapeUrl(url, normalizedType);
  }, [url, siteType, isGreenhouseUrl]);

  const setDefaultSchedule = (id: ScheduleId | "") => {
    setDefaultScheduleId(id);
    if (typeof window !== "undefined") {
      try {
        window.localStorage.setItem(DEFAULT_SCHEDULE_STORAGE_KEY, id || "");
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
    const interval = window.setInterval(loadStatus, 15_000);
    return () => {
      mounted = false;
      window.clearInterval(interval);
    };
  }, []);

  useEffect(() => {
    if (!schedules || schedules.length === 0) return;
    const first = schedules[0]?._id as ScheduleId | undefined;
    if (!defaultScheduleId && first) {
      setDefaultSchedule(first);
      return;
    }
    const hasDefault =
      Boolean(defaultScheduleId) &&
      (schedules as any[]).some((s) => (s._id as ScheduleId) === defaultScheduleId);
    const target: ScheduleId | "" = hasDefault ? defaultScheduleId : first ?? "";

    if (!hasDefault && defaultScheduleId && typeof window !== "undefined") {
      try {
        window.localStorage.setItem(DEFAULT_SCHEDULE_STORAGE_KEY, target || "");
      } catch {
        // ignore
      }
      setDefaultScheduleId(target);
    }

    if (!selectedScheduleId && target) {
      setSelectedScheduleId(target);
    }
    if (!bulkScheduleId && target) {
      setBulkScheduleId(target);
    }
  }, [schedules, selectedScheduleId, bulkScheduleId, defaultScheduleId]);

  const scheduleMap = useMemo(() => {
    const map = new Map<ScheduleId, any>();
    (schedules ?? []).forEach((s: any) => {
      map.set(s._id as ScheduleId, s);
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
    setEditingScheduleId(schedule._id as ScheduleId);
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
      const savedId = (await upsertSchedule({
        id: editingScheduleId ?? undefined,
        name: scheduleName.trim() || "Untitled schedule",
        days: Array.from(scheduleDays),
        startTime: scheduleStartTime,
        intervalMinutes: totalMinutes,
        timezone: scheduleTimezone || defaultTimezone,
      })) as ScheduleId;
      toast.success(editingScheduleId ? "Schedule updated" : "Schedule created");
      setSelectedScheduleId((prev) => prev || savedId);
      setBulkScheduleId((prev) => prev || savedId);
      resetScheduleForm();
    } catch {
      toast.error("Failed to save schedule");
    } finally {
      setSavingSchedule(false);
    }
  };

  const handleDeleteSchedule = async (id: ScheduleId) => {
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

  const handleSiteScheduleChange = async (siteId: string, scheduleId: ScheduleId | "") => {
    try {
      setUpdatingSiteScheduleId(siteId);
      await updateSiteSchedule({
        id: siteId as any,
        scheduleId: scheduleId || undefined,
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
      setScrapeProvider("spidercloud");
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
          parsedType = lowered;
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
        : parsedProvider ?? bulkScrapeProvider ?? "spidercloud";
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

      {dbosStatusError && <div className="text-xs text-amber-300 mb-3">{dbosStatusError}</div>}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 mb-6">
        <div className="rounded border border-slate-800 bg-slate-950/50 p-3">
          <div className="text-[10px] uppercase text-slate-500">Listing Pending</div>
          <div className="mt-2 text-xl font-semibold text-white font-mono">{dbosStatus?.listing?.pending ?? "—"}</div>
        </div>
        <div className="rounded border border-slate-800 bg-slate-950/50 p-3">
          <div className="text-[10px] uppercase text-slate-500">Listing Processing</div>
          <div className="mt-2 text-xl font-semibold text-white font-mono">{dbosStatus?.listing?.processing ?? "—"}</div>
        </div>
        <div className="rounded border border-slate-800 bg-slate-950/50 p-3">
          <div className="text-[10px] uppercase text-slate-500">Detail Pending</div>
          <div className="mt-2 text-xl font-semibold text-white font-mono">{dbosStatus?.detail?.pending ?? "—"}</div>
        </div>
        <div className="rounded border border-slate-800 bg-slate-950/50 p-3">
          <div className="text-[10px] uppercase text-slate-500">Detail Processing</div>
          <div className="mt-2 text-xl font-semibold text-white font-mono">{dbosStatus?.detail?.processing ?? "—"}</div>
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
                        onClick={() => setDefaultSchedule(sched._id as ScheduleId)}
                        disabled={sched._id === defaultScheduleId}
                        className={clsx(
                          "text-[11px] px-2 py-1 rounded border transition-colors",
                          sched._id === defaultScheduleId
                            ? "border-blue-900/60 text-blue-200 bg-blue-900/20 cursor-not-allowed"
                            : "border-blue-800 text-blue-100 hover:bg-blue-900/30"
                        )}
                      >
                        {sched._id === defaultScheduleId ? "Default" : "Set default"}
                      </button>
                      <button
                        onClick={() => handleEditSchedule(sched)}
                        className="text-[11px] px-2 py-1 rounded border border-slate-700 bg-slate-800 text-slate-200 hover:bg-slate-700 transition-colors"
                      >
                        Edit
                      </button>
                      <button
                        onClick={() => { void handleDeleteSchedule(sched._id as ScheduleId); }}
                        disabled={sched.siteCount > 0 || deletingScheduleId === (sched._id as ScheduleId)}
                        className={clsx(
                          "text-[11px] px-2 py-1 rounded border transition-colors",
                          sched.siteCount > 0
                            ? "border-slate-800 text-slate-600 cursor-not-allowed"
                            : "border-red-900/50 text-red-300 hover:bg-red-900/20"
                        )}
                      >
                        {deletingScheduleId === (sched._id as ScheduleId) ? "Deleting..." : "Delete"}
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
                {!!url.trim() && !!previewScrapeUrl && (
                  <p className="text-[11px] text-slate-500 leading-snug truncate" title={previewScrapeUrl}>
                    Scrape URL: <span className="text-slate-200 font-mono">{previewScrapeUrl}</span>
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
              <p className="text-[11px] text-slate-500 mt-1">SpiderCloud is the default for new sites.</p>
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
                title={isGreenhouseUrl || siteType === "greenhouse" ? "Greenhouse sites don't need a pattern" : "Optional pattern for detail pages; blank = treat as job listing page"}
              />
              <p className="text-[11px] text-slate-500 mt-1">Leave blank to treat the URL as a listing page and discover job links automatically.</p>
            </div>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-12 gap-3 items-end">
            <div className="md:col-span-6">
              <label className="text-xs text-slate-400 block mb-1">Schedule</label>
              <select
                value={selectedScheduleId}
                onChange={(e) => setSelectedScheduleId((e.target.value as ScheduleId | "") || "")}
                disabled={!schedules}
                className="w-full bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500 disabled:opacity-60"
              >
                {!schedules && <option value="">Loading schedules...</option>}
                <option value="">{schedules ? "No schedule (manual)" : " "}</option>
                {schedules?.map((sched: any) => (
                  <option key={sched._id as ScheduleId} value={sched._id as ScheduleId}>
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
                Names are auto-generated from the URL. Type can be <code className="bg-slate-900 px-1 rounded text-slate-300">general</code> or <code className="bg-slate-900 px-1 rounded text-slate-300">greenhouse</code>; providers accept <code className="bg-slate-900 px-1 rounded text-slate-300">fetchfox</code>, <code className="bg-slate-900 px-1 rounded text-slate-300">fetchfox_spidercloud</code> (crawl + SpiderCloud detail), <code className="bg-slate-900 px-1 rounded text-slate-300">firecrawl</code>, or <code className="bg-slate-900 px-1 rounded text-slate-300">spidercloud</code>. Leaving the pattern blank treats the URL as a listing page and discovers job links automatically.
              </div>
            </div>
            <div>
              <label className="text-xs text-slate-400 block mb-1">Schedule</label>
              <select
                value={bulkScheduleId || selectedScheduleId}
                onChange={(e) => setBulkScheduleId((e.target.value as ScheduleId | "") || "")}
                className="w-full bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
              >
                {!schedules && <option value="">Loading schedules...</option>}
                <option value="">{schedules ? "No schedule (manual)" : " "}</option>
                {schedules?.map((sched: any) => (
                  <option key={sched._id as ScheduleId} value={sched._id as ScheduleId}>
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
            const scheduleId: ScheduleId | "" = s.scheduleId ? (s.scheduleId as ScheduleId) : "";
            const schedule = scheduleId ? scheduleMap.get(scheduleId) : null;
            const scheduleLabel = schedule ? formatScheduleSummary(schedule) : "No schedule";
            const siteType = (s as any).type ?? "general";
            const siteTypeLabel = siteType === "greenhouse" ? "Greenhouse" : "General";
            const scrapeProvider: ScrapeProvider = (s as any).scrapeProvider ?? "spidercloud";
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
            const scrapeUrl = resolveScrapeUrl(s.url, siteType);

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
                      {scrapeUrl && scrapeUrl !== s.url && (
                        <div className="text-[11px] text-slate-600 truncate font-mono" title={scrapeUrl}>
                          scrape: {scrapeUrl}
                        </div>
                      )}
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
                      disabled={deletingSiteId === siteId}
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
                      disabled={!s.enabled || deletingSiteId === siteId}
                      title={s.enabled ? "Trigger on next workflow cycle" : "Enable site to run"}
                    >
                      Run now
                    </button>
                    <button
                      onClick={() => {
                        void (async () => {
                          const label = s.name || s.url;
                          if (!window.confirm(`Delete ${label}? This removes the site and clears queued URLs.`)) {
                            return;
                          }
                          try {
                            setDeletingSiteId(siteId);
                            await deleteSite({ id: siteId as any });
                            toast.success("Site deleted");
                          } catch {
                            toast.error("Failed to delete site");
                          } finally {
                            setDeletingSiteId(null);
                          }
                        })();
                      }}
                      className={clsx(
                        "px-2 py-1 text-[11px] font-medium rounded border transition-colors whitespace-nowrap",
                        deletingSiteId === siteId
                          ? "border-slate-800 text-slate-600 cursor-not-allowed"
                          : "border-red-900/60 text-red-300 hover:bg-red-900/30"
                      )}
                      disabled={deletingSiteId === siteId}
                    >
                      {deletingSiteId === siteId ? "Deleting..." : "Delete"}
                    </button>
                  </div>
                </div>

                {isExpanded && (
                  <div className="pt-3 border-t border-slate-800 space-y-3 mt-2">
                    <div className="flex flex-wrap items-center gap-2 text-[11px]">
                      <span className="text-slate-400">
                        Company name:
                        <span className="text-slate-100 font-semibold ml-1">{s.name || "Untitled"}</span>
                      </span>
                      <span className="text-slate-500">Manage aliases in Company Names to retag jobs.</span>
                      {onOpenCompanyNames && (
                        <button
                          type="button"
                          onClick={onOpenCompanyNames}
                          className="text-[11px] px-2 py-1 rounded border border-slate-700 text-slate-200 hover:bg-slate-800 transition-colors"
                        >
                          Open Company Names
                        </button>
                      )}
                    </div>

                    <div className="text-[11px] text-slate-300 space-y-1">
                      <div className="flex flex-wrap gap-2">
                        <span className="px-2 py-1 rounded bg-slate-900 border border-slate-800 text-slate-200">
                          URL: <span className="font-mono text-slate-100">{s.url}</span>
                        </span>
                        <span className="px-2 py-1 rounded bg-slate-900 border border-slate-800 text-slate-200" title={scrapeUrl}>
                          Scrape URL: <span className="font-mono text-slate-100">{scrapeUrl}</span>
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
                          onChange={(e) => { void handleSiteScheduleChange(siteId, (e.target.value as ScheduleId | "") || ""); }}
                          disabled={!schedules || updatingSiteScheduleId === siteId}
                          className="bg-slate-900 border border-slate-700 rounded px-2 py-1.5 text-xs text-slate-200 focus:outline-none focus:border-blue-500"
                        >
                          <option value="">No schedule</option>
                          {schedules?.map((sched: any) => (
                            <option key={sched._id as ScheduleId} value={sched._id as ScheduleId}>
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

