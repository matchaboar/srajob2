import React, { useState, useEffect, useRef, useCallback, useMemo, type CSSProperties, type ReactNode, type HTMLAttributes } from "react";
import { usePaginatedQuery, useMutation, useQuery, type PaginatedQueryItem } from "convex/react";
import type { FunctionReturnType } from "convex/server";
import { api } from "../convex/_generated/api";
import type { Id } from "../convex/_generated/dataModel";
import { toast } from "sonner";
import { motion, AnimatePresence } from "framer-motion";
import ReactMarkdown, { type Components } from "react-markdown";
import remarkGfm from "remark-gfm";
import remarkBreaks from "remark-breaks";
import { JobRow } from "./components/JobRow";
import { CompanyIcon } from "./components/CompanyIcon";
import { StatusTracker } from "./components/StatusTracker";
import { LiveTimer } from "./components/LiveTimer";
import { Keycap } from "./components/Keycap";
import { DiagonalFraction } from "./components/DiagonalFraction";
import { buildCompensationMeta, formatCompensationDisplay, formatCurrencyCompensation, parseCompensationInput } from "./lib/compensation";

type Level = "junior" | "mid" | "senior" | "staff";
const TARGET_STATES = ["Washington", "New York", "California", "Arizona"] as const;
type TargetState = (typeof TARGET_STATES)[number];
type JobId = Id<"jobs">;
type SavedFilterId = Id<"saved_filters">;
type ListedJob = PaginatedQueryItem<typeof api.jobs.listJobs>;
type AppliedJobsResult = FunctionReturnType<typeof api.jobs.getAppliedJobs>;
type RejectedJobsResult = FunctionReturnType<typeof api.jobs.getRejectedJobs>;
type CompanySummariesResult = FunctionReturnType<typeof api.jobs.listCompanySummaries>;
type AppliedJob = AppliedJobsResult extends Array<infer Item> ? NonNullable<Item> : never;
type RejectedJob = RejectedJobsResult extends Array<infer Item> ? NonNullable<Item> : never;
type CompanySummary = CompanySummariesResult extends Array<infer Item> ? NonNullable<Item> : never;
type DetailItem = { label: string; value: string | string[]; badge?: string; type?: "link" };

interface Filters {
  search: string;
  includeRemote: boolean;
  state: TargetState | null;
  country: string;
  level: Level | null;
  minCompensation: number | null;
  maxCompensation: number | null;
  hideUnknownCompensation: boolean;
  engineer: boolean;
  companies: string[];
}

interface SavedFilter {
  _id: SavedFilterId;
  name: string;
  search?: string;
  useSearch?: boolean;
  remote?: boolean;
  includeRemote?: boolean;
  state?: TargetState | null;
  country?: string | null;
  level?: Level | null;
  minCompensation?: number;
  maxCompensation?: number;
  hideUnknownCompensation?: boolean;
  engineer?: boolean;
  isSelected: boolean;
  companies?: string[];
}

const buildEmptyFilters = (): Filters => ({
  search: "",
  includeRemote: true,
  state: null,
  country: "",
  level: null,
  minCompensation: null,
  maxCompensation: null,
  hideUnknownCompensation: false,
  engineer: false,
  companies: [],
});

const resolveShareBaseUrl = () => {
  const explicit = (import.meta.env.VITE_CONVEX_HTTP_URL as string | undefined) ?? undefined;
  const fallback = (import.meta.env.VITE_CONVEX_URL as string | undefined) ?? undefined;
  const candidate = explicit || fallback;
  if (candidate) {
    try {
      const parsed = new URL(candidate);
      if (parsed.hostname.endsWith(".convex.cloud")) {
        parsed.hostname = parsed.hostname.replace(".convex.cloud", ".convex.site");
      }
      return parsed.origin;
    } catch {
      // ignore invalid env URLs
    }
  }
  return window.location.origin;
};

const buildFilterLabel = (filter: {
  search?: string | null;
  state?: TargetState | null;
  country?: string | null;
  includeRemote?: boolean | null;
  level?: Level | null;
  remote?: boolean | null;
  minCompensation?: number | null;
  maxCompensation?: number | null;
  hideUnknownCompensation?: boolean | null;
  companies?: Array<string | null> | null;
  engineer?: boolean | null;
}) => {
  const parts: string[] = [];
  const companies = (filter.companies ?? []).filter((name): name is string => typeof name === "string" && !!name.trim());
  if (companies.length > 0) {
    const [first, ...rest] = companies;
    parts.push(rest.length > 0 ? `${first} +${rest.length}` : first);
  }
  const trimmedSearch = (filter.search ?? "").trim();
  if (trimmedSearch) {
    parts.push(trimmedSearch);
  }
  if (filter.level) {
    parts.push(filter.level.charAt(0).toUpperCase() + filter.level.slice(1));
  }
  if (filter.engineer) {
    parts.push("Engineer");
  }
  if (filter.country && filter.country !== "United States") {
    parts.push(filter.country);
  }
  if (filter.state) {
    parts.push(filter.state);
  }
  const includeRemote = filter.includeRemote ?? (filter.remote !== false);
  if (includeRemote === false) {
    parts.push("On-site only");
  }

  const formatSalary = (value: number) => `$${Math.round(value / 1000)}k`;
  const hasMin = filter.minCompensation !== null && filter.minCompensation !== undefined;
  const hasMax = filter.maxCompensation !== null && filter.maxCompensation !== undefined;

  if (hasMin && hasMax) {
    parts.push(`${formatSalary(filter.minCompensation as number)}-${formatSalary(filter.maxCompensation as number)}`);
  } else if (hasMin) {
    parts.push(`${formatSalary(filter.minCompensation as number)}+`);
  } else if (hasMax) {
    parts.push(`Up to ${formatSalary(filter.maxCompensation as number)}`);
  }
  if (filter.hideUnknownCompensation) {
    parts.push("Hide unknown comp");
  }

  return parts.join(" • ") || "All jobs";
};

const normalizeMarkdown = (value: string): string => {
  const lines = value.split(/\r?\n/);
  const normalized: string[] = [];

  for (const line of lines) {
    const trimmedPrev = normalized.length > 0 ? normalized[normalized.length - 1].trim() : "";
    const isBlockStart = /^(\s*[-*+]\s+|\s*\d+\.\s+|#+\s+|>\s+)/.test(line);
    if (isBlockStart && trimmedPrev !== "") {
      normalized.push("");
    }
    normalized.push(line);
  }

  return normalized.join("\n").replace(/\n{3,}/g, "\n\n");
};

const selectSurfaceStyle: CSSProperties = {
  colorScheme: "dark",
  backgroundColor: "#0f172a",
  color: "#e2e8f0",
  borderColor: "#1f2937",
};

const selectOptionStyle: CSSProperties = {
  backgroundColor: "#0f172a",
  color: "#e2e8f0",
};

type KeyboardShortcut = {
  keys: Array<string>;
  label: string;
};

type CompanySuggestion = {
  name: string;
  count: number;
};

type MarkdownCodeProps = HTMLAttributes<HTMLElement> & {
  inline?: boolean;
  className?: string;
  children?: ReactNode;
  node?: unknown;
};

const keyboardShortcuts: Array<KeyboardShortcut> = [
  { keys: ["j", "k"], label: "Navigate" },
  { keys: ["a"], label: "Apply" },
  { keys: ["r"], label: "Reject" },
  { keys: ["Enter"], label: "View Details" },
  { keys: ["Esc"], label: "Close / Back" },
];

const DeleteXIcon = ({ className }: { className?: string }) => (
  <svg
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="1.7"
    strokeLinecap="round"
    strokeLinejoin="round"
    className={className}
    aria-hidden="true"
  >
    <path d="m6 6 12 12M18 6 6 18" />
  </svg>
);

export function JobBoard() {
  // Use URL hash to persist active tab across refreshes
  const [activeTab, setActiveTab] = useState<"jobs" | "companies" | "applied" | "rejected" | "live" | "ignored">(() => {
    const hash = window.location.hash.replace("#", "");
    if (hash === "companies" || hash === "applied" || hash === "rejected" || hash === "live" || hash === "ignored") return hash as any;
    return "jobs";
  });
  const ignoredReasonDetails: Record<string, string> = {
    listing_page: "Looks like a listing/filter page, not a single job detail. We use it to discover job URLs but do not ingest it as a job.",
    listing_payload: "Payload contains multiple job URLs rather than a single job detail; used for URL discovery only.",
    error_landing: "Looks like an error/expired posting page.",
    missing_required_keyword: "Job title/description is missing required keywords (e.g., engineer).",
    stale_scrape_queue_entry: "URL sat in the scrape queue too long and was marked stale.",
    http_404: "Job detail URL returned 404 after retries.",
    max_attempts: "Job detail failed too many times and was retired.",
    filtered: "Filtered out by scraping rules.",
  };
  const companyFilterFromUrl = useMemo(() => {
    const params = new URLSearchParams(window.location.search);
    const raw = params.get("company");
    return raw && raw.trim() ? raw.trim() : null;
  }, []);
  const companyFilterAppliedRef = useRef(false);

  const [filters, setFilters] = useState<Filters>(buildEmptyFilters);
  const [throttledFilters, setThrottledFilters] = useState<Filters>(buildEmptyFilters);
  const [filtersReady, setFiltersReady] = useState(false);
  const [selectedSavedFilterId, setSelectedSavedFilterId] = useState<SavedFilterId | null>(null);
  const [companyInput, setCompanyInput] = useState("");
  const [debouncedCompanyInput, setDebouncedCompanyInput] = useState("");
  const [companyInputFocused, setCompanyInputFocused] = useState(false);
  const [minCompensationInput, setMinCompensationInput] = useState("");
  const [sliderValue, setSliderValue] = useState(200000);
  const [filterUpdatePending, setFilterUpdatePending] = useState(false);
  const [filterCountdownMs, setFilterCountdownMs] = useState(0);
  const [showShortcuts, setShowShortcuts] = useState(false);
  const [showJobDetails, setShowJobDetails] = useState(false);
  const [keyboardNavActive, setKeyboardNavActive] = useState(false);
  const [keyboardTopIndex, setKeyboardTopIndex] = useState<number | null>(null);
  const [filtersOpen, setFiltersOpen] = useState(false);
  const jobListRef = useRef<HTMLDivElement | null>(null);
  const companyBlurTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const buildCompanyJobsUrl = useCallback((companyName: string) => {
    const trimmed = companyName.trim();
    if (!trimmed) return "";
    const url = new URL(window.location.href);
    url.searchParams.set("company", trimmed);
    url.hash = "jobs";
    return url.toString();
  }, []);
  const markdownComponents = useMemo<Components>(() => {
    const renderCode = ({ inline, children, className: _className, ...props }: MarkdownCodeProps) =>
      inline ? (
        <code {...props} className="font-mono px-1 py-0.5 rounded bg-slate-800 text-slate-100">
          {children}
        </code>
      ) : (
        <code
          {...props}
          className="font-mono block bg-slate-900 p-3 rounded border border-slate-800 overflow-x-auto text-slate-100"
        >
          {children}
        </code>
      );

    return {
      p: ({ node: _node, ...props }) => <p {...props} className="mb-3 last:mb-0" />,
      a: ({ node: _node, ...props }) => (
        <a {...props} className="text-blue-300 hover:text-blue-200 underline">
          {props.children}
        </a>
      ),
      ul: ({ node: _node, ...props }) => <ul {...props} className="list-disc ml-5 space-y-1" />,
      ol: ({ node: _node, ...props }) => <ol {...props} className="list-decimal ml-5 space-y-1" />,
      li: ({ node: _node, ...props }) => <li {...props} className="list-disc ml-5" />,
      code: renderCode,
      strong: ({ node: _node, ...props }) => <strong {...props} className="font-semibold text-slate-100" />,
      em: ({ node: _node, ...props }) => <em {...props} className="italic text-slate-200" />,
    };
  }, []);

  const [locallyAppliedJobs, setLocallyAppliedJobs] = useState<Set<JobId>>(new Set());
  const [exitingJobs, setExitingJobs] = useState<Record<string, "apply" | "reject">>({});
  const [locallyWithdrawnJobs] = useState<Set<JobId>>(new Set());

  // Selection state for keyboard navigation
  const [selectedJobId, setSelectedJobId] = useState<JobId | null>(null);
  const defaultFilterRequestedRef = useRef(false);
  const applyingSavedFilterRef = useRef(false);
  const pendingSelectionClearRef = useRef(false);
  const minCompInputFocusedRef = useRef(false);
  const throttleTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lastThrottleRef = useRef(0);
  const countdownIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const throttleDeadlineRef = useRef<number | null>(null);

  // Update URL hash when active tab changes
  useEffect(() => {
    const currentHash = window.location.hash.replace("#", "");
    const expectedHash = activeTab === "jobs" ? "" : activeTab;

    // Only update if the hash doesn't match what it should be
    if (currentHash !== expectedHash) {
      if (activeTab === "jobs") {
        window.location.hash = "";
      } else {
        window.location.hash = activeTab;
      }
    }
  }, [activeTab]);

  // Listen for hash changes (back/forward navigation)
  useEffect(() => {
    const handleHashChange = () => {
      const hash = window.location.hash.replace("#", "");
      if (hash === "companies" || hash === "applied" || hash === "rejected" || hash === "live" || hash === "ignored") {
        setActiveTab(hash);
      } else if (hash === "" || hash === "jobs") {
        setActiveTab("jobs");
      }
    };
    window.addEventListener("hashchange", handleHashChange);
    return () => window.removeEventListener("hashchange", handleHashChange);
  }, []);

  useEffect(() => {
    const handle = setTimeout(() => {
      setDebouncedCompanyInput(companyInput.trim());
    }, 200);
    return () => clearTimeout(handle);
  }, [companyInput]);

  useEffect(() => {
    const handleResize = () => {
      if (window.innerWidth >= 1024) {
        setFiltersOpen(false);
      }
    };

    handleResize();
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  useEffect(() => {
    if (activeTab !== "jobs") {
      setFiltersOpen(false);
    }
  }, [activeTab]);

  useEffect(() => {
    const handleResize = () => {
      if (window.innerWidth >= 1024) {
        setFiltersOpen(false);
      }
    };

    handleResize();
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  useEffect(() => {
    if (activeTab !== "jobs") {
      setFiltersOpen(false);
    }
  }, [activeTab]);

  const isJobsTab = activeTab === "jobs";
  const isCompaniesTab = activeTab === "companies";
  const isAppliedTab = activeTab === "applied";
  const isRejectedTab = activeTab === "rejected";
  const isLiveTab = activeTab === "live";
  const isIgnoredTab = activeTab === "ignored";
  const isAdmin = useQuery(api.auth.isAdmin);
  const jobsPageSize = isAdmin ? 500 : 50;
  const jobsLoadMoreSize = isAdmin ? 500 : 20;
  const companyBannerName = useMemo(() => {
    if (filters.companies.length === 1) return filters.companies[0] ?? null;
    if (!filtersReady && companyFilterFromUrl) return companyFilterFromUrl;
    return null;
  }, [companyFilterFromUrl, filters.companies, filtersReady]);
  const shouldFetchCompanySuggestions = isJobsTab && companyInputFocused && !!debouncedCompanyInput.trim();

  const { results, status, loadMore } = usePaginatedQuery<typeof api.jobs.listJobs>(
    api.jobs.listJobs,
    isJobsTab ? {
      search: throttledFilters.search.trim() || undefined,
      state: throttledFilters.state ?? undefined,
      country: throttledFilters.country?.trim() || undefined,
      includeRemote: throttledFilters.includeRemote,
      level: throttledFilters.level ?? undefined,
      minCompensation: throttledFilters.minCompensation ?? undefined,
      maxCompensation: throttledFilters.maxCompensation ?? undefined,
      hideUnknownCompensation: throttledFilters.hideUnknownCompensation,
      engineer: throttledFilters.engineer ? true : undefined,
      companies: throttledFilters.companies.length > 0 ? throttledFilters.companies : undefined,
    } : "skip",
    { initialNumItems: jobsPageSize } // Load more items for the dense list
  );

  const [displayedResults, setDisplayedResults] = useState<ListedJob[]>(results);
  useEffect(() => {
    // Keep showing the previous page while a new filter set is loading.
    if (status === "LoadingFirstPage") return;
    setDisplayedResults(results);
  }, [results, status]);

  const companySuggestions = useQuery(
    api.jobs.searchCompanies,
    shouldFetchCompanySuggestions
      ? {
        search: debouncedCompanyInput.trim(),
        limit: 8,
      }
      : "skip"
  ) as CompanySuggestion[] | undefined;
  const companySummaries = useQuery(
    api.jobs.listCompanySummaries,
    isCompaniesTab ? { limit: 300 } : "skip"
  ) as CompanySummary[] | undefined;
  const savedFilters = useQuery(api.filters.getSavedFilters, isJobsTab ? {} : "skip");
  const selectedSavedFilter = useMemo(
    () => (savedFilters as SavedFilter[] | undefined)?.find((f) => f.isSelected),
    [savedFilters]
  );
  const shouldFetchIgnored = isIgnoredTab;
  const ignoredJobs = useQuery(
    api.router.listIgnoredJobs,
    shouldFetchIgnored ? { limit: 200 } : "skip"
  ) as
    | Array<{
      _id: string;
      url: string;
      sourceUrl?: string;
      reason?: string;
      provider?: string;
      workflowName?: string;
      createdAt: number;
      details?: Record<string, unknown>;
      title?: string;
      description?: string;
    }>
    | undefined;
  const shouldFetchRecentJobs = isJobsTab || isLiveTab;
  const recentJobs = useQuery(api.jobs.getRecentJobs, shouldFetchRecentJobs ? {} : "skip");
  const appliedJobs = useQuery(api.jobs.getAppliedJobs, isAppliedTab ? {} : "skip");
  const rejectedJobs = useQuery(api.jobs.getRejectedJobs, isRejectedTab ? {} : "skip");
  const applyToJob = useMutation(api.jobs.applyToJob);
  const rejectJob = useMutation(api.jobs.rejectJob);

  // Withdraw not used in this view; keep mutation available for future enhancements
  const ensureDefaultFilter = useMutation(api.filters.ensureDefaultFilter);
  const saveFilter = useMutation(api.filters.saveFilter);
  const selectSavedFilter = useMutation(api.filters.selectSavedFilter);
  const deleteSavedFilter = useMutation(api.filters.deleteSavedFilter);

  // Filter out locally applied/rejected jobs
  const filteredResults: ListedJob[] = displayedResults.filter((job) => !locallyAppliedJobs.has(job._id));
  const appliedList: AppliedJob[] = (appliedJobs ?? []).filter(
    (job): job is AppliedJob => Boolean(job) && !locallyWithdrawnJobs.has((job)._id)
  );
  const rejectedList: RejectedJob[] = (rejectedJobs ?? []).filter(Boolean);
  const selectedJob =
    filteredResults.find((job) => job._id === selectedJobId) ??
    displayedResults.find((job) => job._id === selectedJobId) ??
    null;
  const selectedAppliedJob = useMemo<AppliedJob | null>(() => {
    if (appliedList.length === 0) return null;
    if (!selectedJobId) return appliedList[0];
    return appliedList.find((job) => job._id === selectedJobId) ?? appliedList[0];
  }, [appliedList, selectedJobId]);
  const selectedJobDetails = useQuery(
    api.jobs.getJobDetails,
    isJobsTab && selectedJob?._id ? { jobId: selectedJob._id } : "skip"
  );
  const selectedAppliedJobDetails = useQuery(
    api.jobs.getJobDetails,
    isAppliedTab && selectedAppliedJob?._id ? { jobId: selectedAppliedJob._id } : "skip"
  );
  const selectedJobFull = useMemo(
    () => (selectedJob ? { ...selectedJob, ...(selectedJobDetails ?? {}) } : null),
    [selectedJob, selectedJobDetails]
  );
  const selectedAppliedJobFull = useMemo(
    () => (selectedAppliedJob ? { ...selectedAppliedJob, ...(selectedAppliedJobDetails ?? {}) } : null),
    [selectedAppliedJob, selectedAppliedJobDetails]
  );
  const formatPostedLabel = useCallback((timestamp: number) => {
    const days = Math.max(0, Math.floor((Date.now() - timestamp) / (1000 * 60 * 60 * 24)));
    const dateLabel = new Date(timestamp).toLocaleDateString(undefined, { month: "short", day: "numeric" });
    return `${dateLabel} • ${days}d ago`;
  }, []);
  const formatCompanySalary = useCallback((summary: CompanySummary) => {
    const currencyCode = summary.currencyCode || "USD";
    const formatValue = (value: number | null) => {
      if (value === null || !Number.isFinite(value) || value <= 0) return null;
      if (currencyCode && currencyCode !== "USD") {
        return formatCurrencyCompensation(value, currencyCode);
      }
      return formatCompensationDisplay(value);
    };

    const parts = [
      { label: "Junior", value: formatValue(summary.avgCompensationJunior) ?? "N/A" },
      { label: "Mid", value: formatValue(summary.avgCompensationMid) ?? "N/A" },
      { label: "Senior", value: formatValue(summary.avgCompensationSenior) ?? "N/A" },
    ];

    return parts.map((part) => `${part.label} ${part.value}`).join(" • ");
  }, []);
  const selectedCompMeta = useMemo(() => buildCompensationMeta(selectedJobFull), [selectedJobFull]);
  const selectedCompColorClass = selectedCompMeta.isEstimated ? "text-slate-300" : "text-emerald-200";
  const groupedLocationsLabel = useCallback((job: ListedJob) => {
    const locs = Array.isArray(job.locations) ? job.locations.filter(Boolean) : [];
    if (locs.length === 0) return job.location || "Unknown";
    if (locs.length === 1) return locs[0];
    return `${locs[0]} +${locs.length - 1} more`;
  }, []);
  const selectedJobLocations = useMemo(() => {
    if (!selectedJobFull) return [];
    const raw = Array.isArray(selectedJobFull.locations) ? selectedJobFull.locations : [];
    const seen = new Set<string>();
    const normalized: string[] = [];

    for (const entry of raw) {
      const cleaned = (entry || "").trim();
      if (!cleaned || cleaned.toLowerCase() === "unknown") continue;
      const key = cleaned.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      normalized.push(cleaned);
    }

    const fallback = (selectedJobFull.location || "").trim();
    if (fallback && fallback.toLowerCase() !== "unknown" && !seen.has(fallback.toLowerCase())) {
      seen.add(fallback.toLowerCase());
      normalized.push(fallback);
    }

    return normalized;
  }, [selectedJobFull]);
  const selectedLocationDetail = useMemo(() => {
    if (!selectedJobFull) return "Unknown";
    if (selectedJobLocations.length > 0) return selectedJobLocations[0];
    const parts: string[] = [];
    const seen = new Set<string>();
    const add = (value?: string | null) => {
      const cleaned = (value || "").trim();
      if (!cleaned || cleaned.toLowerCase() === "unknown") return;
      const lowered = cleaned.toLowerCase();
      if (seen.has(lowered)) return;
      seen.add(lowered);
      parts.push(cleaned);
    };
    add(selectedJobFull.city as string | undefined);
    add(selectedJobFull.state as string | undefined);
    add(selectedJobFull.country as string | undefined);
    if (parts.length) return parts.join(" • ");
    const fallback = (selectedJobFull.location || "").trim();
    return fallback || "Unknown";
  }, [selectedJobFull, selectedJobLocations]);
  const formatLevelLabel = useCallback((level?: string | null) => {
    if (!level || typeof level !== "string") return "Not specified";
    return level.charAt(0).toUpperCase() + level.slice(1);
  }, []);
  const formatRelativeTime = useCallback((timestamp?: number | null) => {
    if (typeof timestamp !== "number") return null;
    const delta = Math.max(0, Date.now() - timestamp);
    const minutes = Math.round(delta / (1000 * 60));
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);
    let relative: string;
    if (days > 0) {
      relative = `${days}d ago`;
    } else if (hours > 0) {
      relative = `${hours}h ago`;
    } else if (minutes > 0) {
      relative = `${minutes}m ago`;
    } else {
      relative = "just now";
    }
    const absolute = new Date(timestamp).toLocaleString();
    return `${relative} • ${absolute}`;
  }, []);
  const renderScrapeCost = useCallback((mc: number) => {
    if (mc >= 1000) return `${(mc / 1000).toFixed(2)} ¢`;
    if (mc === 0) return "0 ¢";
    if (mc > 0 && 1000 % mc === 0) {
      return (
        <span className="inline-flex items-center gap-1 text-[#d9ae52]">
          <DiagonalFraction numerator={1} denominator={1000 / mc} />
          <span
            className="text-[#eac56e] font-semibold"
            style={{ fontFamily: '"Times New Roman","Times",serif', fontSize: "14px" }}
          >
            ¢
          </span>
        </span>
      );
    }
    if (mc > 0 && mc < 1000) {
      return (
        <span className="inline-flex items-center gap-1 text-[#d9ae52]">
          <DiagonalFraction numerator={mc} denominator={1000} />
          <span
            className="text-[#eac56e] font-semibold"
            style={{ fontFamily: '"Times New Roman","Times",serif', fontSize: "14px" }}
          >
            ¢
          </span>
        </span>
      );
    }
    return "0 ¢";
  }, []);
  const selectedJobDetailItems = useMemo<DetailItem[]>(() => {
    if (!selectedJobFull) return [];
    const details: DetailItem[] = [];
    const locationValues = selectedJobLocations.length ? selectedJobLocations : ["Unknown"];
    const locationLabel = locationValues.length > 1 ? "Locations" : "Location";

    details.push({ label: "Company", value: selectedJobFull.company });
    details.push({ label: "Level", value: formatLevelLabel(selectedJobFull.level) });
    details.push({
      label: locationLabel,
      value: locationValues,
      badge: selectedJobFull.remote ? "Remote" : undefined,
    });
    details.push({
      label: "Compensation",
      value: selectedCompMeta.display,
    });

    // Scrape metadata rendered below description; omit here.

    if (selectedJobFull.url) {
      details.push({
        label: "Job URL",
        value: selectedJobFull.url,
        type: "link",
      });
    }

    return details;
  }, [selectedJobFull, selectedCompMeta, formatLevelLabel, formatPostedLabel, selectedJobLocations]);
  const jobUrlDetail = useMemo<(DetailItem & { value: string }) | null>(() => {
    const entry = selectedJobDetailItems.find((item) => item.label === "Job URL");
    if (entry && typeof entry.value === "string") {
      return entry as DetailItem & { value: string };
    }
    return null;
  }, [selectedJobDetailItems]);
  const parsingSteps = useMemo(() => {
    if (!selectedJobFull) return [];
    const scrapedWith = selectedJobFull.scrapedWith || selectedJobFull.workflowName;
    const scrapedAt = selectedJobFull.scrapedAt;
    const heuristicAttempts = selectedJobFull.heuristicAttempts ?? 0;
    const heuristicLastTried = selectedJobFull.heuristicLastTried;
    const heuristicVersion = selectedJobFull.heuristicVersion;
    const heuristicRan =
      (selectedJobFull.workflowName || "").toLowerCase().includes("heuristic") ||
      (selectedJobFull.compensationReason || "").toLowerCase().includes("heuristic") ||
      heuristicAttempts > 0 ||
      typeof heuristicLastTried === "number";

    const heuristicParts: string[] = [];
    if (heuristicVersion !== undefined) {
      heuristicParts.push(`v${heuristicVersion}`);
    }
    if (heuristicAttempts > 0) {
      heuristicParts.push(`${heuristicAttempts} attempt${heuristicAttempts === 1 ? "" : "s"}`);
    }
    if (heuristicLastTried) {
      heuristicParts.push(`last ${formatRelativeTime(heuristicLastTried) || new Date(heuristicLastTried).toLocaleString()}`);
    }

    return [
      {
        label: "Initial scrape",
        checked: Boolean(scrapedWith),
        status: scrapedAt ? "Completed" : "Pending",
        note: scrapedAt
          ? `${new Date(scrapedAt).toLocaleString()}${scrapedWith ? ` • ${scrapedWith}` : ""}`
          : "Not scraped yet",
      },
      {
        label: "Heuristic parsing",
        checked: heuristicRan,
        status: heuristicRan ? "Completed" : "Pending",
        note: heuristicRan
          ? (heuristicParts.length ? heuristicParts.join(" • ") : selectedJobFull.workflowName || "HeuristicJobDetails")
          : `Not run${selectedJobFull.compensationReason ? ` (reason: ${selectedJobFull.compensationReason})` : ""}`,
        subtext: heuristicRan && selectedJobFull.workflowName ? `Workflow: ${selectedJobFull.workflowName}` : undefined,
      },
      {
        label: "LLM parsing",
        checked: false,
        status: "Pending",
        note: "Optional enrichment (not requested)",
      },
    ];
  }, [formatRelativeTime, selectedJobFull]);
  const parseNotes = useMemo(() => {
    if (!selectedJobFull) return "No additional notes.";
    return selectedJobFull.compensationReason || selectedCompMeta.reason || "No additional notes.";
  }, [selectedJobFull, selectedCompMeta]);
  const descriptionText = useMemo(() => {
    const raw = selectedJobFull?.description || selectedJobFull?.job_description || "";
    const trimmed = raw.trim();
    if (!trimmed) return "No description available.";
    return normalizeMarkdown(trimmed);
  }, [selectedJobFull]);
  const descriptionWordCount = useMemo(() => {
    if (!descriptionText) return null;
    return descriptionText.split(/\s+/).filter(Boolean).length;
  }, [descriptionText]);
  const metadataText = useMemo(() => {
    const raw = (selectedJobFull as { metadata?: string } | null)?.metadata || "";
    const trimmed = raw.trim();
    if (!trimmed) return "";
    return normalizeMarkdown(trimmed);
  }, [selectedJobFull]);
  const appliedCompMeta = useMemo(() => buildCompensationMeta(selectedAppliedJobFull), [selectedAppliedJobFull]);
  const appliedCompColorClass = appliedCompMeta.isEstimated ? "text-slate-300" : "text-emerald-200";
  const appliedDescriptionText = useMemo(() => {
    const raw = selectedAppliedJobFull?.description || selectedAppliedJobFull?.job_description || "";
    const trimmed = raw.trim();
    if (!trimmed) return "No description available.";
    return normalizeMarkdown(trimmed);
  }, [selectedAppliedJobFull]);
  const appliedMetadataText = useMemo(() => {
    const raw = (selectedAppliedJobFull as { metadata?: string } | null)?.metadata || "";
    const trimmed = raw.trim();
    if (!trimmed) return "";
    return normalizeMarkdown(trimmed);
  }, [selectedAppliedJobFull]);
  const appliedDescriptionWordCount = useMemo(() => {
    if (!appliedDescriptionText) return null;
    return appliedDescriptionText.split(/\s+/).filter(Boolean).length;
  }, [appliedDescriptionText]);
  const blurFromIndex =
    keyboardNavActive && keyboardTopIndex !== null ? keyboardTopIndex + 3 : Infinity;
  const scrollToJob = useCallback(
    (jobId: JobId, alignToFloor: boolean) => {
      const container = jobListRef.current;
      if (!container) return;
      const row = container.querySelector<HTMLElement>(`[data-job-id="${jobId}"]`);
      if (!row) return;
      if (!alignToFloor) {
        row.scrollIntoView({ block: "nearest" });
        return;
      }
      const rowRect = row.getBoundingClientRect();
      const containerRect = container.getBoundingClientRect();
      const rowHeight = rowRect.height || 0;
      const offsetTop = row.offsetTop - container.scrollTop;
      const bottomOffset = offsetTop + rowHeight - containerRect.height;
      const needsScroll =
        offsetTop < 0 ||
        bottomOffset > 0 ||
        offsetTop > rowHeight * 3 + 2; // more than ~3 rows above should snap down
      const desiredTop = Math.max(0, row.offsetTop - rowHeight * 3);
      if (needsScroll && Math.abs(container.scrollTop - desiredTop) > 1) {
        container.scrollTo({ top: desiredTop, behavior: "auto" });
      }
    },
    [],
  );
  const formatCountdown = useCallback((ms: number) => {
    const totalSeconds = Math.max(0, Math.ceil(ms / 1000));
    const minutes = Math.floor(totalSeconds / 60)
      .toString()
      .padStart(2, "0");
    const seconds = (totalSeconds % 60).toString().padStart(2, "0");
    return `${minutes}:${seconds}`;
  }, []);

  const applySavedFilterToState = useCallback((filter: SavedFilter) => {
    applyingSavedFilterRef.current = true;
    lastThrottleRef.current = 0;
    setSelectedSavedFilterId(filter._id);
    setFilters({
      search: filter.search ?? "",
      includeRemote: filter.includeRemote ?? (filter.remote !== false),
      state: (filter.state as TargetState | null) ?? null,
      country: filter.country ?? "",
      level: (filter.level as Level | null) ?? null,
      minCompensation: filter.minCompensation ?? null,
      maxCompensation: filter.maxCompensation ?? null,
      hideUnknownCompensation: filter.hideUnknownCompensation ?? false,
      engineer: filter.engineer ?? false,
      companies: filter.companies ?? [],
    });
    setCompanyInput("");
    setSelectedJobId(null);
    setTimeout(() => {
      applyingSavedFilterRef.current = false;
    }, 0);
  }, []);

  const clearSavedSelection = useCallback(() => {
    if (!selectedSavedFilterId || applyingSavedFilterRef.current) return;

    setSelectedSavedFilterId(null);
    pendingSelectionClearRef.current = true;
    selectSavedFilter({ filterId: undefined })
      .catch(() => toast.error("Failed to clear saved filter"))
      .finally(() => {
        pendingSelectionClearRef.current = false;
      });
  }, [selectedSavedFilterId, selectSavedFilter]);

  const updateFilters = useCallback((partial: Partial<Filters>, opts?: { forceImmediate?: boolean }) => {
    if (opts?.forceImmediate) {
      lastThrottleRef.current = 0;
    }
    setFilters((prev) => ({ ...prev, ...partial }));
    if (selectedSavedFilterId && !applyingSavedFilterRef.current) {
      clearSavedSelection();
    }
  }, [clearSavedSelection, selectedSavedFilterId]);

  const FILTER_THROTTLE_MS = 5000;
  const MIN_SALARY = 50000;
  const MAX_SALARY = 800000;
  const SALARY_STEP = 20000;
  const DEFAULT_SLIDER_VALUE = 200000;
  const countrySelectId = "job-board-country-filter";
  const stateSelectId = "job-board-state-filter";
  const levelSelectId = "job-board-level-filter";
  const engineerCheckboxId = "job-board-engineer-filter";
  const clampToSliderRange = useCallback(
    (value: number) => Math.min(Math.max(value, MIN_SALARY), MAX_SALARY),
    [MAX_SALARY, MIN_SALARY]
  );
  useEffect(() => {
    const now = Date.now();
    const elapsed = now - lastThrottleRef.current;
    const remaining = FILTER_THROTTLE_MS - elapsed;

    if (remaining <= 0) {
      lastThrottleRef.current = now;
      throttleDeadlineRef.current = null;
      setThrottledFilters(filters);
      setFilterUpdatePending(false);
      setFilterCountdownMs(0);
      if (throttleTimeoutRef.current) {
        clearTimeout(throttleTimeoutRef.current);
        throttleTimeoutRef.current = null;
      }
      return;
    }

    setFilterUpdatePending(true);
    throttleDeadlineRef.current = now + remaining;
    setFilterCountdownMs(remaining);
    if (throttleTimeoutRef.current) {
      clearTimeout(throttleTimeoutRef.current);
    }
    throttleTimeoutRef.current = setTimeout(() => {
      lastThrottleRef.current = Date.now();
      throttleDeadlineRef.current = null;
      setThrottledFilters(filters);
      setFilterUpdatePending(false);
      setFilterCountdownMs(0);
      throttleTimeoutRef.current = null;
    }, remaining);
  }, [FILTER_THROTTLE_MS, filters]);
  useEffect(() => {
    if (!filterUpdatePending) {
      if (countdownIntervalRef.current) {
        clearInterval(countdownIntervalRef.current);
        countdownIntervalRef.current = null;
      }
      setFilterCountdownMs(0);
      return;
    }

    if (countdownIntervalRef.current) {
      clearInterval(countdownIntervalRef.current);
    }
    countdownIntervalRef.current = setInterval(() => {
      if (!throttleDeadlineRef.current) {
        setFilterCountdownMs(0);
        return;
      }
      setFilterCountdownMs(Math.max(0, throttleDeadlineRef.current - Date.now()));
    }, 200);
    return () => {
      if (countdownIntervalRef.current) {
        clearInterval(countdownIntervalRef.current);
        countdownIntervalRef.current = null;
      }
    };
  }, [filterUpdatePending]);
  useEffect(() => {
    return () => {
      if (throttleTimeoutRef.current) {
        clearTimeout(throttleTimeoutRef.current);
      }
      if (countdownIntervalRef.current) {
        clearInterval(countdownIntervalRef.current);
      }
      if (companyBlurTimeoutRef.current) {
        clearTimeout(companyBlurTimeoutRef.current);
      }
    };
  }, []);
  const commitMinCompensation = useCallback(() => {
    const parsed = parseCompensationInput(minCompensationInput, { max: MAX_SALARY });
    setMinCompensationInput(parsed === null ? "" : formatCompensationDisplay(parsed));
    setSliderValue(clampToSliderRange(parsed ?? DEFAULT_SLIDER_VALUE));
    updateFilters({ minCompensation: parsed }, { forceImmediate: true });
  }, [clampToSliderRange, minCompensationInput, updateFilters]);

  useEffect(() => {
    if (!minCompInputFocusedRef.current) {
      setMinCompensationInput(formatCompensationDisplay(filters.minCompensation));
    }
    if (filters.minCompensation === null) {
      setSliderValue(DEFAULT_SLIDER_VALUE);
    } else {
      setSliderValue(clampToSliderRange(filters.minCompensation));
    }
  }, [DEFAULT_SLIDER_VALUE, clampToSliderRange, filters.minCompensation]);


  const generatedFilterName = useMemo(() => buildFilterLabel(filters), [filters]);

  const resetFilters = useCallback((alsoClearSavedSelection: boolean = true) => {
    setFilters(buildEmptyFilters());
    setSelectedJobId(null);
    setCompanyInput("");

    if (alsoClearSavedSelection) {
      setSelectedSavedFilterId(null);
      pendingSelectionClearRef.current = true;
      selectSavedFilter({ filterId: undefined })
        .catch(() => toast.error("Failed to clear saved filter"))
        .finally(() => {
          pendingSelectionClearRef.current = false;
        });
    }
  }, [selectSavedFilter]);

  const handleCompanyCardClick = useCallback(
    (companyName: string) => {
      const trimmed = companyName.trim();
      if (!trimmed) return;
      setActiveTab("jobs");
      updateFilters({ ...buildEmptyFilters(), companies: [trimmed] }, { forceImmediate: true });
      setCompanyInput("");
      setSelectedJobId(null);

      const url = new URL(window.location.href);
      url.searchParams.set("company", trimmed);
      url.hash = "jobs";
      window.history.pushState({}, "", url.toString());
    },
    [updateFilters],
  );

  const handleSaveCurrentFilter = useCallback(async () => {
    const trimmedName = generatedFilterName.trim() || "Saved filter";
    try {
      await saveFilter({
        name: trimmedName,
        search: filters.search || undefined,
        includeRemote: filters.includeRemote,
        state: filters.state || undefined,
        country: filters.country || undefined,
        level: filters.level ?? undefined,
        minCompensation: filters.minCompensation ?? undefined,
        maxCompensation: filters.maxCompensation ?? undefined,
        hideUnknownCompensation: filters.hideUnknownCompensation,
        engineer: filters.engineer,
        companies: filters.companies.length > 0 ? filters.companies : undefined,
      });
      toast.success("Filter saved");
    } catch {
      toast.error("Failed to save filter");
    }
  }, [filters, generatedFilterName, saveFilter]);

  const handleSelectSavedFilter = useCallback(async (filterId: SavedFilterId | null) => {
    try {
      await selectSavedFilter({ filterId: filterId ?? undefined });
      if (filterId) {
        const match = (savedFilters || []).find((f: any) => f._id === filterId);
        if (match) {
          applySavedFilterToState(match as SavedFilter);
        }
      } else {
        resetFilters(false);
        setSelectedSavedFilterId(null);
      }
    } catch {
      toast.error("Failed to select filter");
    }
  }, [applySavedFilterToState, resetFilters, savedFilters, selectSavedFilter]);

  useEffect(() => {
    if (savedFilters === undefined) return;

    if (companyFilterFromUrl) {
      if (!companyFilterAppliedRef.current) {
        companyFilterAppliedRef.current = true;
        lastThrottleRef.current = 0;
        setActiveTab("jobs");
        setSelectedSavedFilterId(null);
        setFilters({
          ...buildEmptyFilters(),
          companies: [companyFilterFromUrl],
        });
        setCompanyInput("");
        setSelectedJobId(null);
        pendingSelectionClearRef.current = true;
        selectSavedFilter({ filterId: undefined })
          .catch(() => toast.error("Failed to clear saved filter"))
          .finally(() => {
            pendingSelectionClearRef.current = false;
          });
      }
      if (savedFilters.length === 0 && !defaultFilterRequestedRef.current) {
        defaultFilterRequestedRef.current = true;
        ensureDefaultFilter().catch(() => setFiltersReady(true));
      }
      setFiltersReady(true);
      return;
    }

    if (savedFilters.length === 0) {
      if (!defaultFilterRequestedRef.current) {
        defaultFilterRequestedRef.current = true;
        ensureDefaultFilter().catch(() => setFiltersReady(true));
      } else {
        setFiltersReady(true);
      }
      return;
    }

    const selected = (savedFilters as SavedFilter[]).find((f) => f.isSelected);
    if (selected) {
      const alreadySelected = selectedSavedFilterId === selected._id;
      if (!alreadySelected && !pendingSelectionClearRef.current) {
        applySavedFilterToState(selected);
      } else if (!alreadySelected) {
        setSelectedSavedFilterId(selected._id);
      }
    } else if (selectedSavedFilterId) {
      setSelectedSavedFilterId(null);
    }

    setFiltersReady(true);
  }, [applySavedFilterToState, companyFilterFromUrl, ensureDefaultFilter, savedFilters, selectedSavedFilterId, selectSavedFilter]);

  useEffect(() => {
    if (filtersReady) return;
    if (!selectedSavedFilter) return;
    if (minCompInputFocusedRef.current) return;
    setMinCompensationInput(formatCompensationDisplay(selectedSavedFilter.minCompensation ?? null));
  }, [filtersReady, selectedSavedFilter]);

  const savedFilterList = useMemo(
    () => (savedFilters as SavedFilter[] | undefined) || [],
    [savedFilters]
  );
  const anyServerSelection = savedFilterList.some((f) => f.isSelected);
  const noFilterActive = !selectedSavedFilterId && !anyServerSelection;

  const handleDeleteSavedFilter = useCallback(async (filterId: SavedFilterId) => {
    const matchingFilter = savedFilterList.find((f) => f._id === filterId);
    const wasActive = filterId === selectedSavedFilterId || matchingFilter?.isSelected;

    try {
      await deleteSavedFilter({ filterId: filterId as any });
      if (wasActive) {
        resetFilters();
      }
      toast.success("Filter deleted");
    } catch {
      toast.error("Failed to delete filter");
    }
  }, [deleteSavedFilter, resetFilters, savedFilterList, selectedSavedFilterId]);

  const selectedCompanySet = useMemo(
    () => new Set(filters.companies.map((c) => c.trim().toLowerCase()).filter(Boolean)),
    [filters.companies]
  );

  const filteredCompanySuggestions = useMemo(() => {
    if (!companySuggestions) return [];
    return companySuggestions.filter((suggestion) => {
      const key = suggestion.name.trim().toLowerCase();
      return key && !selectedCompanySet.has(key);
    });
  }, [companySuggestions, selectedCompanySet]);

  const countryOptions = useMemo(() => {
    const uniqueCountries = new Set<string>();
    const addCountry = (value?: string | null) => {
      const trimmed = (value ?? "").trim();
      if (trimmed) {
        uniqueCountries.add(trimmed);
      }
    };

    results.forEach((job) => {
      (job.countries ?? []).forEach(addCountry);
      addCountry((job).country);
    });
    (recentJobs ?? []).forEach((job) => {
      (job.countries ?? []).forEach(addCountry);
      addCountry((job).country);
    });

    uniqueCountries.add("United States");
    uniqueCountries.add("Other");

    const prioritized = Array.from(uniqueCountries).filter(
      (country) => country !== "United States" && country.toLowerCase() !== "other"
    );
    prioritized.sort((a, b) => a.localeCompare(b));

    return ["", "United States", ...prioritized, "Other"];
  }, [recentJobs, results]);

  const addCompanyFilter = useCallback((name: string) => {
    const trimmed = name.trim();
    if (!trimmed) return;
    const alreadySelected = filters.companies.some((c) => c.toLowerCase() === trimmed.toLowerCase());
    if (alreadySelected) return;
    updateFilters({ companies: [...filters.companies, trimmed] }, { forceImmediate: true });
    setCompanyInput("");
  }, [filters.companies, updateFilters]);

  const removeCompanyFilter = useCallback((name: string) => {
    updateFilters({ companies: filters.companies.filter((c) => c !== name) }, { forceImmediate: true });
  }, [filters.companies, updateFilters]);

  // Select top job on load or when results change
  useEffect(() => {
    if (activeTab !== "jobs") return;
    if (filteredResults.length === 0) {
      setSelectedJobId(null);
      setShowJobDetails(false);
      return;
    }
    const stillVisible = filteredResults.some((job) => job._id === selectedJobId);
    if (!selectedJobId || !stillVisible) {
      setSelectedJobId(filteredResults[0]._id);
    }
  }, [activeTab, filteredResults, selectedJobId]);

  useEffect(() => {
    if (activeTab !== "applied") return;
    if (appliedList.length === 0) {
      setSelectedJobId(null);
      return;
    }
    const stillVisible = appliedList.some((job) => job._id === selectedJobId);
    if (!selectedJobId || !stillVisible) {
      setSelectedJobId(appliedList[0]._id);
    }
  }, [activeTab, appliedList, selectedJobId]);

  const handleSelectJob = useCallback((jobId: JobId) => {
    setSelectedJobId(jobId);
    setShowJobDetails(true);
    setKeyboardNavActive(false);
    setKeyboardTopIndex(null);
  }, []);

  const handleApply = useCallback(async (jobId: JobId, type: "ai" | "manual", url?: string) => {
    if (exitingJobs[jobId]) return;

    setExitingJobs(prev => ({ ...prev, [jobId]: "apply" }));

    // Move selection to next job immediately if possible
    const currentIndex = filteredResults.findIndex(j => j._id === jobId);
    if (currentIndex !== -1 && currentIndex < filteredResults.length - 1) {
      setSelectedJobId(filteredResults[currentIndex + 1]._id);
    }

    setTimeout(() => {
      setLocallyAppliedJobs(prev => new Set([...prev, jobId]));
      setExitingJobs(prev => {
        const copy = { ...prev };
        delete copy[jobId];
        return copy;
      });
    }, 200); // Faster animation for compact view

    try {
      await applyToJob({ jobId, type });
      toast.success(`Applied to job!`);
      if (type === "manual" && url) {
        window.open(url, "_blank");
      }
    } catch {
      // Revert
      setExitingJobs(prev => {
        const copy = { ...prev };
        delete copy[jobId];
        return copy;
      });
      setLocallyAppliedJobs(prev => {
        const newSet = new Set(prev);
        newSet.delete(jobId);
        return newSet;
      });
      toast.error("Failed to apply");
    }
  }, [applyToJob, exitingJobs, filteredResults]);

  const handleReject = useCallback(async (jobId: JobId) => {
    if (exitingJobs[jobId]) return;

    setExitingJobs(prev => ({ ...prev, [jobId]: "reject" }));

    // Move selection to next job
    const currentIndex = filteredResults.findIndex(j => j._id === jobId);
    if (currentIndex !== -1 && currentIndex < filteredResults.length - 1) {
      setSelectedJobId(filteredResults[currentIndex + 1]._id);
    }

    setTimeout(() => {
      setLocallyAppliedJobs(prev => new Set([...prev, jobId]));
      setExitingJobs(prev => {
        const copy = { ...prev };
        delete copy[jobId];
        return copy;
      });
    }, 200);

    try {
      await rejectJob({ jobId });
      toast.success("Job rejected");
    } catch {
      setExitingJobs(prev => {
        const copy = { ...prev };
        delete copy[jobId];
        return copy;
      });
      setLocallyAppliedJobs(prev => {
        const newSet = new Set(prev);
        newSet.delete(jobId);
        return newSet;
      });
      toast.error("Failed to reject");
    }
  }, [rejectJob, exitingJobs, filteredResults]);

  const buildJobDetailsLink = useCallback((jobId: JobId) => {
    const shareBase = resolveShareBaseUrl();
    const shareUrl = new URL("/share/job", shareBase);
    shareUrl.searchParams.set("id", jobId);
    shareUrl.searchParams.set("app", window.location.origin);
    return shareUrl.toString();
  }, []);

  const copyToClipboard = useCallback(async (text: string) => {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return;
    }
    const textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.setAttribute("readonly", "");
    textarea.style.position = "fixed";
    textarea.style.opacity = "0";
    textarea.style.left = "-9999px";
    document.body.appendChild(textarea);
    textarea.select();
    const success = document.execCommand("copy");
    document.body.removeChild(textarea);
    if (!success) {
      throw new Error("Copy failed");
    }
  }, []);

  const handleCopyJobLink = useCallback(async (jobId: JobId) => {
    try {
      const link = buildJobDetailsLink(jobId);
      await copyToClipboard(link);
      toast.success("Job link copied");
    } catch {
      toast.error("Failed to copy job link");
    }
  }, [buildJobDetailsLink, copyToClipboard]);



  // Keyboard Navigation
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Global shortcuts (Escape)
      if (e.key === "Escape") {
        if (showShortcuts) {
          setShowShortcuts(false);
          return;
        }
        if (showJobDetails) {
          setShowJobDetails(false);
          return;
        }
      }

      if (!["jobs", "applied", "rejected"].includes(activeTab)) return;

      // Ignore keyboard shortcuts if Ctrl, Cmd, or Alt is pressed
      if (e.ctrlKey || e.metaKey || e.altKey) return;

      const target = e.target as HTMLElement | null;
      const typingTarget = target?.closest("input, textarea, select, button, [role='textbox']");
      if (target?.isContentEditable || typingTarget) return;

      const currentList: any[] = activeTab === "jobs" ? filteredResults : activeTab === "applied" ? appliedList : rejectedList;
      if (!selectedJobId || currentList.length === 0) return;

      const currentIndex = currentList.findIndex(j => j._id === selectedJobId);
      if (currentIndex === -1) return;

      switch (e.key) {
        case "ArrowDown":
        case "j":
          e.preventDefault();
          setKeyboardNavActive(true);
          if (currentIndex < currentList.length - 1) {
            const nextId = currentList[currentIndex + 1]._id;
            const nextIndex = currentIndex + 1;
            setKeyboardTopIndex(nextIndex >= 3 ? nextIndex - 3 : 0);
            setSelectedJobId(nextId);
            scrollToJob(nextId, currentIndex + 1 >= 3);
          } else if (activeTab === "jobs" && status === "CanLoadMore") {
            loadMore(jobsLoadMoreSize);
          }
          break;
        case "ArrowUp":
        case "k":
          e.preventDefault();
          setKeyboardNavActive(true);
          if (currentIndex > 0) {
            const prevId = currentList[currentIndex - 1]._id;
            const prevIndex = currentIndex - 1;
            setKeyboardTopIndex((prevTop) => {
              const top = prevTop ?? (prevIndex >= 3 ? prevIndex - 3 : 0);
              if (prevIndex < top) {
                return Math.max(0, prevIndex - 3);
              }
              return top;
            });
            setSelectedJobId(prevId);
            scrollToJob(prevId, false);
          }
          break;
        case "a": {
          e.preventDefault();
          const jobToApply = currentList[currentIndex];
          void handleApply(jobToApply._id, "manual", jobToApply.url);
          break;
        }
        case "r": {
          e.preventDefault();
          void handleReject(currentList[currentIndex]._id);
          break;
        }
        case "Enter":
          e.preventDefault();
          setShowJobDetails(true);
          break;
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [activeTab, selectedJobId, filteredResults, handleApply, handleReject, status, loadMore, showShortcuts, showJobDetails, scrollToJob]);


  return (
    <div className="flex flex-col h-[calc(100vh-64px)] bg-slate-950 text-slate-200 overflow-hidden">
      {/* Top Bar / Tabs */}
      <div className="flex items-center justify-between px-4 sm:px-6 py-3 border-b border-slate-800 bg-slate-900/50 gap-3">
        <div className="flex items-center gap-3 overflow-x-auto">
          {companyBannerName && (
            <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg border border-slate-800 bg-slate-900/70">
              <CompanyIcon company={companyBannerName} size={26} />
              <div className="flex flex-col leading-tight">
                <span className="text-[10px] uppercase tracking-wide text-slate-500">Company</span>
                <span className="text-sm font-semibold text-slate-100 max-w-[140px] truncate" title={companyBannerName}>
                  {companyBannerName}
                </span>
              </div>
            </div>
          )}
          <div className="flex space-x-1 bg-slate-900 p-1 rounded-lg border border-slate-800 overflow-x-auto">
            {(["jobs", "companies", "applied", "rejected", "live", "ignored"] as const).map((tab) => (
              <button
                key={tab}
                onClick={() => setActiveTab(tab)}
                className={`px-3 sm:px-4 py-1.5 rounded-md text-xs sm:text-sm font-medium transition-all whitespace-nowrap ${activeTab === tab
                  ? "bg-slate-800 text-white shadow-sm"
                  : "text-slate-400 hover:text-slate-200 hover:bg-slate-800/50"
                  }`}
              >
                {tab.charAt(0).toUpperCase() + tab.slice(1)}
                {tab === "applied" && appliedList.length > 0 && (
                  <span className="ml-2 px-1.5 py-0.5 bg-blue-500/20 text-blue-400 text-[10px] rounded-full">
                    {appliedList.length}
                  </span>
                )}
                {tab === "rejected" && rejectedList.length > 0 && (
                  <span className="ml-2 px-1.5 py-0.5 bg-red-500/20 text-red-300 text-[10px] rounded-full">
                    {rejectedList.length}
                  </span>
                )}
              </button>
            ))}
          </div>
        </div>
        <div className="flex items-center gap-2 sm:gap-3 text-xs text-slate-400">
          {activeTab === "jobs" && (
            <button
              onClick={() => setFiltersOpen((prev) => !prev)}
              className="inline-flex items-center gap-2 px-3 py-1.5 rounded border border-slate-700 bg-slate-900 text-slate-200 hover:border-blue-500 hover:text-white transition-colors text-xs font-medium"
              aria-expanded={filtersOpen}
              aria-label="Toggle filters"
            >
              <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                <path d="M4 6h16M4 12h10M4 18h7" />
              </svg>
              Filters
            </button>
          )}
          <button
            onClick={() => setShowShortcuts((prev) => !prev)}
            className="flex items-center gap-2 px-2.5 py-1 rounded border border-slate-700 bg-slate-900 text-slate-200 hover:border-slate-500 hover:text-white transition-colors text-[11px]"
            aria-expanded={showShortcuts}
            aria-label="Toggle keyboard shortcuts"
          >
            <span className="relative flex h-2 w-2">
              <span className="absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-40 animate-ping" />
              <span className="relative inline-flex h-2 w-2 rounded-full bg-blue-300" />
            </span>
            Shortcuts
          </button>
        </div>
      </div>

      {/* Shortcuts Overlay */}
      <AnimatePresence>
        {showShortcuts && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/80 backdrop-blur-sm">
            <motion.div
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.95 }}
              className="w-full max-w-lg bg-slate-900 border border-slate-800 rounded-xl shadow-2xl overflow-hidden"
            >
              <div className="flex items-center justify-between px-6 py-4 border-b border-slate-800 bg-slate-900/50">
                <h3 className="text-lg font-semibold text-white">Keyboard Shortcuts</h3>
                <button
                  onClick={() => setShowShortcuts(false)}
                  className="p-1.5 text-slate-400 hover:text-white hover:bg-slate-800 rounded-lg transition-colors"
                >
                  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
              <div className="p-6">
                <div className="grid grid-cols-2 gap-x-8 gap-y-6">
                  {keyboardShortcuts.map((shortcut) => (
                    <div key={shortcut.label} className="flex items-center justify-between">
                      <span className="text-sm text-slate-400">{shortcut.label}</span>
                      <div className="flex items-center gap-1.5">
                        {shortcut.keys.map((key) => (
                          <Keycap key={key} label={key === "Enter" ? "↵" : key === "Esc" ? "ESC" : key.toUpperCase()} />
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
              <div className="px-6 py-4 bg-slate-900/50 border-t border-slate-800 text-center">
                <p className="text-xs text-slate-500">
                  Press <span className="font-mono text-slate-400">Esc</span> to close this window
                </p>
              </div>
            </motion.div>
          </div>
        )}
      </AnimatePresence>

      <div className="flex flex-1 overflow-hidden">
        {activeTab === "jobs" && (
          filtersReady ? (
            <>
              {filtersOpen && (
                <button
                  type="button"
                  aria-label="Close filters"
                  data-testid="filters-overlay"
                  onClick={() => setFiltersOpen(false)}
                  className="fixed inset-0 z-20 bg-slate-950/40 backdrop-blur-[1px]"
                />
              )}
              {/* Sidebar Filters */}
              <div
                className={`w-full sm:w-80 bg-slate-900/95 border-r border-slate-800 p-4 flex flex-col gap-6 overflow-y-auto transition-transform duration-200 ${filtersOpen ? "translate-x-0" : "-translate-x-full"} fixed inset-y-[64px] left-0 z-30 shadow-2xl backdrop-blur-sm`}
                role="complementary"
                aria-label="Job filters"
                data-testid="filters-panel"
              >
                <div
                  className="sticky top-0 z-10 -mx-4 -mt-4 px-4 pt-4 pb-2 border-b border-slate-800 bg-slate-900/95 backdrop-blur-sm flex items-center justify-between"
                  data-testid="filters-header"
                >
                  <h3 className="text-sm font-semibold text-white">Filters</h3>
                  <button
                    onClick={() => setFiltersOpen(false)}
                    className="p-2 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition-colors"
                    aria-label="Close filters"
                    data-testid="filters-close"
                  >
                    <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                    </svg>
                  </button>
                </div>
                <div className="space-y-3">
                  <div className="space-y-2">
                    <label className="block text-xs font-semibold text-slate-500 uppercase">Search</label>
                    <input
                      type="text"
                      value={filters.search}
                      onChange={(e) => updateFilters({ search: e.target.value })}
                      placeholder="Search titles..."
                      className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500 focus:ring-1 focus:ring-blue-500 placeholder-slate-600"
                    />
                    <p className="text-[11px] text-slate-500">
                      Full-text search returns up to 100 of the most recent matching jobs.
                    </p>
                  </div>
                </div>

                <div>
                  <label className="block text-xs font-semibold text-slate-500 uppercase mb-2">Companies</label>
                  <div className="space-y-2">
                    <div className="flex flex-wrap gap-2">
                      {filters.companies.map((company) => (
                        <span
                          key={company}
                          className="inline-flex items-center gap-1.5 px-2 py-1 rounded-full bg-slate-800/70 text-xs text-slate-100"
                        >
                          <span className="truncate max-w-[8rem]">{company}</span>
                          <button
                            type="button"
                            onClick={() => removeCompanyFilter(company)}
                            className="text-slate-400 hover:text-white transition-colors"
                            aria-label={`Remove company filter ${company}`}
                          >
                            <DeleteXIcon className="w-3 h-3" />
                          </button>
                        </span>
                      ))}
                    </div>
                    <div className="relative">
                      <input
                        type="text"
                        value={companyInput}
                        onChange={(e) => setCompanyInput(e.target.value)}
                        onFocus={() => {
                          if (companyBlurTimeoutRef.current) {
                            clearTimeout(companyBlurTimeoutRef.current);
                          }
                          setCompanyInputFocused(true);
                        }}
                        onBlur={() => {
                          if (companyBlurTimeoutRef.current) {
                            clearTimeout(companyBlurTimeoutRef.current);
                          }
                          companyBlurTimeoutRef.current = setTimeout(() => {
                            setCompanyInputFocused(false);
                          }, 120);
                        }}
                        onKeyDown={(e) => {
                          if (e.key === "Enter") {
                            e.preventDefault();
                            if (filteredCompanySuggestions.length > 0) {
                              addCompanyFilter(filteredCompanySuggestions[0].name);
                            } else {
                              addCompanyFilter(companyInput);
                            }
                          }
                          if (e.key === "Backspace" && !companyInput && filters.companies.length > 0) {
                            removeCompanyFilter(filters.companies[filters.companies.length - 1]);
                          }
                        }}
                        placeholder="Add a company..."
                        className="w-full bg-slate-950 border-b border-slate-700 px-2 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-blue-500 focus:ring-0 placeholder-slate-600"
                      />
                      {companyInputFocused && filteredCompanySuggestions.length > 0 && (
                        <div className="absolute left-0 right-0 mt-1 bg-slate-900 border border-slate-800 rounded-md shadow-xl overflow-hidden z-10">
                          {filteredCompanySuggestions.map((suggestion) => (
                            <button
                              key={suggestion.name}
                              type="button"
                              onMouseDown={(e) => e.preventDefault()}
                              onClick={() => addCompanyFilter(suggestion.name)}
                              className="w-full text-left px-3 py-2 hover:bg-slate-800 text-sm text-slate-200 flex items-center justify-between"
                            >
                              <span className="truncate">{suggestion.name}</span>
                              <span className="text-[11px] text-slate-500 ml-3">{suggestion.count} roles</span>
                            </button>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>
                </div>

                <div>
                  <label
                    htmlFor={countrySelectId}
                    className="block text-xs font-semibold text-slate-500 uppercase mb-2"
                  >
                    Country
                  </label>
                  <select
                    id={countrySelectId}
                    aria-label="Location"
                    value={filters.country}
                    onChange={(e) => {
                      const next = (e.target.value || "").trim();
                      updateFilters(
                        {
                          country: next,
                          state: next === "United States" ? filters.state : null,
                        },
                        { forceImmediate: true },
                      );
                    }}
                    style={selectSurfaceStyle}
                    className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
                  >
                    {countryOptions.map((country) => (
                      <option key={country} style={selectOptionStyle} value={country}>
                        {country === ""
                          ? "Any country"
                          : country === "Other"
                            ? "Other (non-US)"
                            : country}
                      </option>
                    ))}
                  </select>
                  <p className="text-[11px] text-slate-500 mt-1">
                    Choose "Any country" to show everything or "Other" to focus on non-US roles.
                  </p>
                </div>

                <div>
                  <label
                    htmlFor={stateSelectId}
                    className="block text-xs font-semibold text-slate-500 uppercase mb-2"
                  >
                    State
                  </label>
                  <select
                    id={stateSelectId}
                    value={filters.state ?? ""}
                    onChange={(e) =>
                      updateFilters(
                        {
                          state: (e.target.value || null) as TargetState | null,
                        },
                        { forceImmediate: true },
                      )
                    }
                    style={selectSurfaceStyle}
                    className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
                  >
                    <option style={selectOptionStyle} value="">Any Target State</option>
                    {TARGET_STATES.map((state) => (
                      <option key={state} style={selectOptionStyle} value={state}>
                        {state}
                      </option>
                    ))}
                  </select>
                </div>

                <div className="flex items-center justify-between gap-3 rounded border border-slate-800 bg-slate-900/40 px-3 py-2">
                  <div className="text-xs font-semibold text-slate-500 uppercase">Remote</div>
                  <button
                    type="button"
                    role="switch"
                    aria-checked={filters.includeRemote}
                    onClick={() => updateFilters({ includeRemote: !filters.includeRemote }, { forceImmediate: true })}
                    className={`relative h-6 w-11 rounded-full border transition-colors duration-150 overflow-hidden ${filters.includeRemote ? "bg-emerald-500/40 border-emerald-400" : "bg-slate-800 border-slate-700"
                      }`}
                    aria-label={filters.includeRemote ? "Remote on" : "Remote off"}
                  >
                    <span
                      className={`absolute left-0.5 top-0.5 h-5 w-5 rounded-full bg-white shadow-sm transition-transform duration-150 ${filters.includeRemote ? "translate-x-5" : "translate-x-0"
                        }`}
                    />
                  </button>
                </div>

                <div>
                  <label
                    htmlFor={levelSelectId}
                    className="block text-xs font-semibold text-slate-500 uppercase mb-2"
                  >
                    Level
                  </label>
                  <select
                    id={levelSelectId}
                    value={filters.level || ""}
                    onChange={(e) =>
                      updateFilters({
                        level: e.target.value === "" ? null : (e.target.value as Level),
                      })
                    }
                    style={selectSurfaceStyle}
                    className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
                  >
                    <option style={selectOptionStyle} value="">Any Level</option>
                    <option style={selectOptionStyle} value="staff">Staff</option>
                    <option style={selectOptionStyle} value="senior">Senior</option>
                    <option style={selectOptionStyle} value="mid">Mid</option>
                    <option style={selectOptionStyle} value="junior">Junior</option>
                  </select>
                </div>

                <label
                  htmlFor={engineerCheckboxId}
                  className="flex items-center justify-between gap-3 rounded border border-slate-800 bg-slate-900/40 px-3 py-2 cursor-pointer"
                >
                  <span className="text-[11px] font-semibold uppercase text-slate-500">Engineer titles only</span>
                  <input
                    id={engineerCheckboxId}
                    type="checkbox"
                    className="h-4 w-4 rounded border-slate-700 bg-slate-900 text-blue-500 focus:ring-blue-500"
                    checked={filters.engineer}
                    onChange={(e) => updateFilters({ engineer: e.target.checked }, { forceImmediate: true })}
                  />
                </label>

                <div>
                  <label className="block text-xs font-semibold text-slate-500 uppercase mb-2">Min Salary</label>
                  <input
                    type="text"
                    value={minCompensationInput}
                    onChange={(e) => {
                      const rawValue = e.target.value;
                      setMinCompensationInput(rawValue);
                    }}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        e.preventDefault();
                        minCompInputFocusedRef.current = false;
                        commitMinCompensation();
                      }
                    }}
                    onFocus={() => {
                      minCompInputFocusedRef.current = true;
                    }}
                    onBlur={() => {
                      minCompInputFocusedRef.current = false;
                      commitMinCompensation();
                    }}
                    placeholder="$50k"
                    className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500 placeholder-slate-600"
                  />
                  <div className="mt-3">
                    <div className="flex items-center justify-between text-[11px] text-slate-500 mb-1">
                      <span>$50k</span>
                      <span>$800k</span>
                    </div>
                    <input
                      type="range"
                      min={MIN_SALARY}
                      max={MAX_SALARY}
                      step={SALARY_STEP}
                      value={sliderValue}
                      onChange={(e) => {
                        const value = parseInt(e.target.value, 10);
                        setSliderValue(value);
                        setMinCompensationInput(formatCompensationDisplay(value));
                        updateFilters({ minCompensation: value });
                      }}
                      className="salary-slider w-full"
                    />
                  </div>
                  <label className="mt-3 flex items-center justify-between gap-3 rounded border border-slate-800 bg-slate-900/40 px-3 py-2 cursor-pointer">
                    <span className="text-[11px] font-semibold uppercase text-slate-500">Hide unknown compensation</span>
                    <input
                      type="checkbox"
                      className="h-4 w-4 rounded border-slate-700 bg-slate-900 text-blue-500 focus:ring-blue-500"
                      checked={filters.hideUnknownCompensation}
                      onChange={(e) => updateFilters({ hideUnknownCompensation: e.target.checked }, { forceImmediate: true })}
                    />
                  </label>
                </div>

                <div className="space-y-2">
                  <button
                    onClick={() => { void handleSaveCurrentFilter(); }}
                    className="w-full px-3 py-2 text-xs bg-blue-600 text-white rounded hover:bg-blue-500 transition-colors"
                  >
                    Save as filter
                  </button>
                  <p className="text-[11px] text-slate-500 leading-tight">
                    Saves as "{generatedFilterName}" based on the fields above.
                  </p>
                </div>

                <div className="border-t border-slate-800 pt-4 space-y-3">
                  <div>
                    <label className="block text-xs font-semibold text-slate-500 uppercase mb-2">Saved Filters</label>
                    <div className="flex flex-col gap-2">
                      <div className="min-w-0">
                        <button
                          onClick={() => { void handleSelectSavedFilter(null); }}
                          className={`w-full px-3 py-1.5 rounded-md border text-xs transition-colors overflow-hidden min-w-0 ${noFilterActive
                            ? "border-blue-500/60 bg-blue-900/40 text-blue-100"
                            : "border-slate-700 text-slate-300 hover:border-slate-500"
                            }`}
                        >
                          No filter
                        </button>
                      </div>
                      {savedFilterList.map((filter) => {
                        const isActive = filter._id === selectedSavedFilterId || filter.isSelected;
                        const filterLabel = buildFilterLabel({
                          search: filter.search ?? "",
                          state: (filter.state as TargetState | null) ?? null,
                          country: filter.country ?? "United States",
                          includeRemote: filter.includeRemote ?? (filter.remote !== false),
                          level: (filter.level as Level | null) ?? null,
                          minCompensation: filter.minCompensation ?? null,
                          maxCompensation: filter.maxCompensation ?? null,
                          hideUnknownCompensation: filter.hideUnknownCompensation ?? false,
                          engineer: filter.engineer ?? false,
                          companies: filter.companies ?? [],
                        });
                        return (
                          <div key={filter._id} className="min-w-0">
                            <div className="flex items-stretch gap-1 w-full">
                              <button
                                onClick={() => { void handleSelectSavedFilter(filter._id); }}
                                className={`flex-1 px-3 py-1.5 rounded-md border text-xs transition-colors text-left overflow-hidden min-w-0 ${isActive
                                  ? "border-blue-500/60 bg-blue-900/40 text-blue-100"
                                  : "border-slate-700 text-slate-300 hover:border-slate-500"
                                  }`}
                              >
                                <div className="font-medium truncate">{filterLabel}</div>
                              </button>
                              <button
                                onClick={() => { void handleDeleteSavedFilter(filter._id); }}
                                className="px-2 py-1.5 rounded-md border border-red-500/40 bg-red-500/5 text-[11px] text-red-200 hover:border-red-400 hover:bg-red-500/10 transition-colors flex items-center justify-center w-9 shrink-0"
                                title="Delete saved filter"
                                aria-label={`Delete saved filter ${filterLabel}`}
                              >
                                <DeleteXIcon className="w-3.5 h-3.5" />
                              </button>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                </div>

                <div className="mt-auto pt-6 border-t border-slate-800">
                  <button
                    onClick={() => resetFilters()}
                    className="w-full py-2 text-xs text-slate-400 hover:text-white hover:bg-slate-800 rounded transition-colors"
                  >
                    Reset Filters
                  </button>
                </div>
              </div>

              {/* Main Job List + Details */}
              <div className="flex-1 flex bg-slate-950 overflow-hidden">
                <div className="flex-1 flex flex-col overflow-hidden">
                  <div className="flex-1 overflow-y-auto relative" ref={jobListRef}>
                    {filterUpdatePending && (
                      <div className="absolute inset-0 z-30 flex items-center justify-center bg-slate-950/70 backdrop-blur-sm pointer-events-none">
                        <div
                          className="flex items-center gap-4 rounded-lg border border-slate-800 bg-slate-900/90 px-4 py-3 shadow-xl"
                          data-testid="filters-pending"
                        >
                          <div className="flex flex-col items-start gap-1">
                            <span className="text-[10px] uppercase tracking-[0.22em] text-slate-500">Throttle</span>
                            <span className="text-lg font-mono text-blue-200 tabular-nums leading-none">
                              {formatCountdown(filterCountdownMs)}
                            </span>
                          </div>
                          <div className="flex items-center gap-2 text-slate-100">
                            <span className="relative flex h-2.5 w-2.5">
                              <span className="absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-50 animate-ping" />
                              <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-blue-400" />
                            </span>
                            <span className="text-sm font-medium">Updating filters...</span>
                          </div>
                        </div>
                      </div>
                    )}

                    {/* Header Row (sticky for alignment with scrollbar) */}
                    <div className="sticky top-0 z-20 relative">
                      <div className="flex items-center gap-2 sm:gap-3 px-3 sm:px-4 py-2 border-b border-slate-800 bg-slate-900/80 backdrop-blur text-[11px] sm:text-xs font-semibold text-slate-500 uppercase tracking-wider">
                        <div className="w-1" /> {/* Spacer for alignment with selection indicator */}
                        <div className="flex-1 grid grid-cols-[auto_6fr_3fr] sm:grid-cols-[auto_8fr_3fr_2fr_2fr_2fr] gap-2 sm:gap-3 items-center">
                          <div className="w-8 h-8" />
                          <div>Job</div>
                          <div className="hidden sm:block">Location(s)</div>
                          <div className="text-right">Salary</div>
                          <div className="text-right hidden sm:block">Posted</div>
                          <div className="text-right hidden sm:block">Scraped</div>
                        </div>
                      </div>
                    </div>

                    <div className="min-h-full">
                      <AnimatePresence initial={false}>
                        {filteredResults.map((job, idx) => (
                          <JobRow
                            key={job._id}
                            job={job}
                            groupedLabel={groupedLocationsLabel(job)}
                            isSelected={selectedJobId === job._id}
                            onSelect={() => handleSelectJob(job._id)}
                            isExiting={exitingJobs[job._id]}
                            keyboardBlur={idx > blurFromIndex}
                            getCompanyJobsUrl={buildCompanyJobsUrl}
                          />
                        ))}
                      </AnimatePresence>

                      {status === "CanLoadMore" && (
                        <div className="p-4 flex justify-center border-t border-slate-800">
                          <button
                            onClick={() => loadMore(jobsLoadMoreSize)}
                            className="px-4 py-2 text-sm text-slate-400 hover:text-white hover:bg-slate-900 rounded transition-colors"
                          >
                            Load More
                          </button>
                        </div>
                      )}

                      {filteredResults.length === 0 && (
                        <div className="flex flex-col items-center justify-center h-64 text-slate-500">
                          <p>No jobs found.</p>
                        </div>
                      )}
                    </div>
                  </div>
                </div>

                {showJobDetails && selectedJobFull && (
                  <div className="w-full sm:w-[50rem] border-l border-slate-800 bg-slate-950 flex flex-col shadow-2xl max-h-[85vh] sm:max-h-none sm:h-auto fixed sm:static inset-x-0 bottom-0 sm:bottom-auto sm:inset-auto z-40 sm:z-auto rounded-t-2xl sm:rounded-none">
                    <div className="flex items-start justify-between px-4 py-3 border-b border-slate-800/50 bg-slate-900/20">
                      <div className="min-w-0 pr-4">
                        <h2 className="text-lg font-bold text-white leading-tight mb-1.5">{selectedJobFull.title}</h2>
                        <div className="flex flex-wrap items-center gap-2 text-[11px] text-slate-300">
                          {selectedJobFull.company ? (
                            <a
                              href={buildCompanyJobsUrl(selectedJobFull.company)}
                              target="_blank"
                              rel="noreferrer"
                              onClick={(event) => event.stopPropagation()}
                              className="text-sm font-medium text-blue-400 mr-1 hover:text-blue-300 underline-offset-2 hover:underline"
                            >
                              {selectedJobFull.company}
                            </a>
                          ) : null}
                          {selectedLocationDetail && selectedLocationDetail !== "Unknown" && (
                            <span
                              className="px-2 py-0.5 rounded-md border border-slate-800 bg-slate-900/70"
                              title={selectedJobFull.location || undefined}
                            >
                              {selectedLocationDetail}
                            </span>
                          )}
                          {selectedJobFull.remote && (
                            <span className="px-2 py-0.5 rounded-md border border-emerald-600/60 bg-emerald-500/10 text-emerald-300 font-semibold">
                              Remote
                            </span>
                          )}
                          {selectedJobFull.level && (
                            <span className="px-2 py-0.5 rounded-md border border-slate-800 bg-slate-900/70">
                              {formatLevelLabel(selectedJobFull.level)}
                            </span>
                          )}
                        {!selectedCompMeta.isUnknown && (
                          <span className="px-2 py-0.5 rounded-md border border-slate-800 bg-slate-900/70">
                            <span
                                className={selectedCompColorClass}
                                title={selectedCompMeta.reason}
                              >
                                {selectedCompMeta.display}
                              </span>
                          </span>
                          )}
                          {typeof selectedJobFull.postedAt === "number" && (
                            <span className="px-2 py-0.5 rounded-md border border-slate-800 bg-slate-900/70">
                              Posted {formatPostedLabel(selectedJobFull.postedAt)}
                            </span>
                          )}
                        </div>
                      </div>
                      <button
                        onClick={() => setShowJobDetails(false)}
                        className="shrink-0 p-2 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition-colors"
                        aria-label="Close job details"
                      >
                        <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                        </svg>
                      </button>
                    </div>

                    <div className="flex-1 overflow-y-auto custom-scrollbar">
                      <div className="p-3 space-y-2">
                        <div className="flex gap-2">
                          {selectedJobFull.url && (
                            <button
                              onClick={() => { void handleApply(selectedJobFull._id, "manual", selectedJobFull.url); }}
                              className="flex-1 px-4 py-2.5 text-sm font-semibold uppercase tracking-wide text-slate-900 bg-emerald-400 hover:bg-emerald-300 border border-emerald-500 shadow-lg shadow-emerald-900/30 transition-transform active:scale-[0.99]"
                            >
                              Direct Apply
                            </button>
                          )}
                          <button
                            onClick={() => { }}
                            disabled
                            className="px-4 py-2.5 text-sm font-semibold uppercase tracking-wide text-slate-500 line-through border border-slate-700 bg-slate-900/70 cursor-not-allowed"
                          >
                            Apply with AI
                          </button>
                        </div>


                        <div className="flex justify-end">

                        </div>

                        {jobUrlDetail && (
                          <div className="rounded-lg border border-slate-800/70 bg-slate-900/50 px-3 py-1.5 flex items-center gap-2">
                            <a
                              href={jobUrlDetail.value}
                              target="_blank"
                              rel="noreferrer"
                              className="text-xs text-blue-300 hover:text-blue-200 underline-offset-2 break-all truncate"
                            >
                              {jobUrlDetail.value}
                            </a>
                          </div>
                        )}




                        <div className="rounded-lg border border-slate-800/70 bg-slate-900/40 p-2">
                          <div className="flex items-center justify-between mb-2">
                            <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider">Description</h3>
                            <div className="flex items-center gap-2 text-[11px] text-slate-500">
                              {descriptionWordCount !== null && (
                                <span>{`${descriptionWordCount} words`}</span>
                              )}
                              <button
                                type="button"
                                onClick={() => { void handleCopyJobLink(selectedJobFull._id); }}
                                className="inline-flex h-6 w-6 items-center justify-center rounded border border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-500 hover:bg-slate-800 transition-colors"
                                aria-label="Copy job link"
                                title="Copy job link"
                              >
                                <svg className="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor">
                                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 13a5 5 0 0 1 0-7l1.5-1.5a5 5 0 0 1 7 7L17 12" />
                                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14 11a5 5 0 0 1 0 7l-1.5 1.5a5 5 0 0 1-7-7L7 12" />
                                </svg>
                              </button>
                            </div>
                          </div>
                          <div className="text-sm leading-relaxed text-slate-300 font-sans max-h-72 overflow-y-auto pr-1 space-y-3">
                            <ReactMarkdown remarkPlugins={[remarkGfm, remarkBreaks]} components={markdownComponents}>
                              {descriptionText}
                            </ReactMarkdown>
                          </div>
                        </div>

                        {metadataText && (
                          <div className="rounded-lg border border-slate-800/70 bg-slate-900/35 p-2">
                            <div className="text-[10px] uppercase tracking-wider font-semibold text-slate-500 mb-2">
                              Metadata
                            </div>
                            <div className="text-sm leading-relaxed text-slate-300 font-sans max-h-56 overflow-y-auto pr-1 space-y-3">
                              <ReactMarkdown remarkPlugins={[remarkGfm, remarkBreaks]} components={markdownComponents}>
                                {metadataText}
                              </ReactMarkdown>
                            </div>
                          </div>
                        )}

                        {selectedJobFull?.alternateUrls && Array.isArray(selectedJobFull.alternateUrls) && selectedJobFull.alternateUrls.length > 1 && (
                          <div className="rounded-lg border border-slate-800/70 bg-slate-900/50 px-3 py-2 flex flex-col gap-2">
                            <div className="text-[10px] uppercase tracking-wider font-semibold text-slate-500">
                              Other locations / links
                            </div>
                            <div className="flex flex-col gap-1 text-sm text-slate-100">
                              {selectedJobFull.alternateUrls.map((link: string) => (
                                <a
                                  key={link}
                                  href={link}
                                  target="_blank"
                                  rel="noreferrer"
                                  className="text-blue-300 hover:text-blue-200 underline-offset-2 break-all"
                                >
                                  {link}
                                </a>
                              ))}
                            </div>
                          </div>
                        )}

                        <div className="rounded-lg border border-slate-800/70 bg-slate-900/40 p-2 space-y-2">
                          <div className="text-[10px] uppercase tracking-wider font-semibold text-slate-500">
                            Scrape Info
                          </div>
                          <div className="flex items-start gap-2 text-sm text-slate-200">
                            <span className="w-28 text-slate-500">Scraped</span>
                            <span className="font-semibold text-slate-100 break-words">
                              {typeof selectedJobFull?.scrapedAt === "number"
                                ? new Date(selectedJobFull.scrapedAt).toLocaleString(undefined, {
                                  month: "short",
                                  day: "numeric",
                                  hour: "2-digit",
                                  minute: "2-digit",
                                })
                                : "None"}
                              {selectedJobFull?.scrapedWith ? ` • ${selectedJobFull.scrapedWith}` : ""}
                            </span>
                          </div>
                          <div className="flex items-start gap-2 text-sm text-slate-200">
                            <span className="w-28 text-slate-500">Workflow</span>
                            <span className="font-semibold text-slate-100 break-words">
                              {selectedJobFull?.workflowName || "None"}
                            </span>
                          </div>
                          <div className="flex items-start gap-2 text-sm text-slate-200">
                            <span className="w-28 text-slate-500">Scrape Cost</span>
                            <span className="font-semibold text-slate-100 break-words">
                              {typeof selectedJobFull?.scrapedCostMilliCents === "number"
                                ? (() => {
                                  return renderScrapeCost(selectedJobFull.scrapedCostMilliCents as number);
                                })()
                                : "None"}
                            </span>
                          </div>
                        </div>

                        <div className="rounded-lg border border-slate-800/70 bg-slate-900/40 p-2 space-y-2">
                          <div className="text-[10px] uppercase tracking-wider font-semibold text-slate-500">
                            Applications
                          </div>
                          <div className="flex items-start gap-2 text-sm text-slate-200">
                            <span className="font-semibold text-slate-100 break-words">
                              {selectedJobFull?.applicationCount ?? 0}
                            </span>
                          </div>
                        </div>

                        <div className="rounded-lg border border-slate-800/70 bg-slate-900/40 p-2 space-y-2">
                          <div className="text-[10px] uppercase tracking-wider font-semibold text-slate-500">
                            Parsing Workflows
                          </div>
                          <div className="flex flex-col gap-1.5">
                            {parsingSteps.map((step) => (
                              <label key={step.label} className="flex items-start gap-2 text-sm text-slate-100">
                                <input
                                  type="checkbox"
                                  checked={step.checked}
                                  readOnly
                                  className="mt-0.5 h-4 w-4 rounded border-slate-700 bg-slate-900 text-emerald-400 focus:ring-emerald-500"
                                />
                                <span className="flex-1 flex flex-col leading-tight gap-0.5">
                                  <span className="flex items-center gap-2">
                                    <span className="font-semibold">{step.label}</span>
                                    <span
                                      className={`px-2 py-0.5 rounded-full text-[10px] font-semibold uppercase tracking-wide border ${step.checked
                                        ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-200"
                                        : "border-amber-500/40 bg-amber-500/10 text-amber-100"
                                        }`}
                                    >
                                      {step.status || (step.checked ? "Completed" : "Pending")}
                                    </span>
                                  </span>
                                  <span className="text-[11px] text-slate-400">{step.note}</span>
                                  {step.subtext && <span className="text-[11px] text-slate-500">{step.subtext}</span>}
                                </span>
                              </label>
                            ))}
                          </div>
                          <div className="text-[10px] uppercase tracking-wider font-semibold text-slate-500 pt-1">
                            Parse Notes
                          </div>
                          <div className="rounded border border-slate-800 bg-slate-950/70 text-sm text-slate-200 px-3 py-2 whitespace-pre-wrap">
                            {parseNotes}
                          </div>
                        </div>


                      </div>
                    </div>
                  </div>
                )}
              </div>

            </>
          ) : (
            <div className="flex flex-1 items-center justify-center bg-slate-950 text-slate-400">
              Loading your filters...
            </div>
          )
        )}
        {activeTab === "companies" && (
          <div className="flex-1 overflow-y-auto px-6 py-4">
            <div className="flex items-center justify-between mb-4">
              <div>
                <h2 className="text-lg font-semibold text-white">Companies</h2>
                <p className="text-xs text-slate-500">Browse companies and jump straight into their open roles.</p>
              </div>
              <span className="text-xs text-slate-400">
                {(companySummaries?.length ?? 0).toLocaleString()} companies
              </span>
            </div>

            {!companySummaries && (
              <div className="text-sm text-slate-500">Loading companies...</div>
            )}

            {companySummaries && companySummaries.length === 0 && (
              <div className="text-sm text-slate-500">No companies available yet.</div>
            )}

            {companySummaries && companySummaries.length > 0 && (
              <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
                {companySummaries.map((company) => (
                  <button
                    key={company.name}
                    type="button"
                    onClick={() => handleCompanyCardClick(company.name)}
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
                          {company.count.toLocaleString()} jobs
                        </div>
                      </div>
                    </div>
                    <div className="mt-3 flex items-center justify-between gap-3">
                      <span className="text-[10px] uppercase tracking-wider text-slate-500">Salary avg</span>
                      <span className="text-xs font-mono text-blue-200">{formatCompanySalary(company)}</span>
                    </div>
                    <div className="mt-2 flex items-center justify-between gap-3">
                      <span className="text-[10px] uppercase tracking-wider text-slate-500">Newest job</span>
                      <span className="inline-flex items-center gap-1 text-[10px] px-2 py-1 rounded-full border border-slate-800 bg-slate-950/60 text-slate-200">
                        <LiveTimer
                          startTime={company.lastScrapedAt}
                          showAgo
                          showSeconds={false}
                          className="text-[10px] font-mono text-slate-200"
                          suffixClassName="text-slate-400"
                        />
                      </span>
                    </div>
                  </button>
                ))}
              </div>
            )}
          </div>
        )}
        {activeTab === "ignored" && (
          <div className="flex-1 flex bg-slate-950 overflow-hidden">
            <div className="flex-1 flex flex-col min-w-0">
              <div className="flex items-center justify-between px-4 py-3 border-b border-slate-800 bg-slate-950/60">
                <h2 className="text-lg font-semibold text-white">Ignored URLs</h2>
                <span className="text-xs text-slate-400">
                  {(ignoredJobs?.length ?? 0).toLocaleString()} entries
                </span>
              </div>
              <div className="flex items-center gap-3 px-4 py-2 border-b border-slate-800 bg-slate-900/50 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                <div className="flex-1 grid grid-cols-[5fr_2fr] sm:grid-cols-[6fr_2fr_2fr_2fr_2fr] gap-3 items-center">
                  <div>Job</div>
                  <div>Reason</div>
                  <div className="hidden sm:block">Provider</div>
                  <div className="hidden sm:block">Source</div>
                  <div className="hidden sm:block text-right">Seen</div>
                </div>
              </div>
              <div className="flex-1 overflow-y-auto">
                {(ignoredJobs || []).map((row) => (
                  <div
                    key={row._id}
                    className="group flex items-start gap-3 px-4 py-3 border-b border-slate-800 transition-colors hover:bg-slate-900/60"
                  >
                    <div className="flex-1 grid grid-cols-[5fr_2fr] sm:grid-cols-[6fr_2fr_2fr_2fr_2fr] gap-3 items-start">
                      <div className="min-w-0 flex flex-col gap-1">
                        <a
                          href={row.url}
                          target="_blank"
                          rel="noreferrer"
                          className="text-sm font-semibold text-slate-100 hover:underline truncate"
                          title={row.title || row.url}
                        >
                          {row.title || "Unknown"}
                        </a>
                        <a
                          href={row.url}
                          target="_blank"
                          rel="noreferrer"
                          className="text-[11px] text-slate-400 hover:underline truncate"
                          title={row.url}
                        >
                          {row.url}
                        </a>
                        {row.description && (
                          <p className="text-[11px] text-slate-300 line-clamp-2 whitespace-pre-wrap break-words">
                            {row.description}
                          </p>
                        )}
                        {row.sourceUrl && (
                          <div className="text-[10px] text-slate-500 truncate sm:hidden" title={row.sourceUrl}>
                            Source: {row.sourceUrl}
                          </div>
                        )}
                      </div>
                      <div className="min-w-0 flex flex-col gap-1">
                        <span className="text-xs text-slate-200">{row.reason || "—"}</span>
                        {row.reason && (
                          <details className="text-[11px] text-slate-400">
                            <summary className="cursor-pointer text-[11px] text-slate-300 hover:text-white">
                              Show why
                            </summary>
                            <div className="mt-1 text-[11px] text-slate-400">
                              {ignoredReasonDetails[row.reason] ?? "No description available for this reason code yet."}
                            </div>
                          </details>
                        )}
                        <div className="sm:hidden text-[10px] text-slate-500">
                          {(row.provider || "Unknown provider") + " • " + new Date(row.createdAt).toLocaleDateString()}
                        </div>
                      </div>
                      <div className="hidden sm:flex flex-col gap-1 text-xs text-slate-400">
                        <span>{row.provider || "Unknown"}</span>
                        {row.workflowName && (
                          <span className="text-[10px] text-slate-500">Workflow: {row.workflowName}</span>
                        )}
                      </div>
                      <div className="hidden sm:flex flex-col gap-1 text-xs text-slate-400 min-w-0">
                        <span className="truncate" title={row.sourceUrl || ""}>
                          {row.sourceUrl || "—"}
                        </span>
                      </div>
                      <div className="hidden sm:block text-right text-xs text-slate-400">
                        {new Date(row.createdAt).toLocaleString()}
                      </div>
                    </div>
                  </div>
                ))}
                {(ignoredJobs?.length ?? 0) === 0 && (
                  <div className="flex flex-col items-center justify-center h-64 text-slate-500">
                    <p>No ignored URLs yet.</p>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {activeTab === "applied" && (
          <div className="flex-1 flex bg-slate-950 overflow-hidden">
            <div className="flex-1 flex flex-col min-w-0">
              {/* Header Row - Matches JobRow 'applied' variant grid */}
              <div className="flex items-center gap-3 px-4 py-2 border-b border-slate-800 bg-slate-900/50 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                <div className="w-8" />
                <div className="flex-1 grid grid-cols-[auto_5fr_3fr] sm:grid-cols-[auto_5fr_5fr_3fr_2fr_2fr] gap-3 items-center">
                  <div className="w-8" />
                  <div>Job</div>
                  <div className="hidden sm:block">Status</div>
                  <div className="hidden sm:block">Location</div>
                  <div className="text-right hidden sm:block">Salary</div>
                  <div className="text-right hidden sm:block">Applied</div>
                </div>
              </div>

              <div className="flex-1 overflow-y-auto">
                {appliedList.map((job) => (
                  <JobRow
                    key={job._id}
                    job={job}
                    variant="applied"
                    isSelected={selectedJobId === job._id}
                    onSelect={() => handleSelectJob(job._id)}
                    getCompanyJobsUrl={buildCompanyJobsUrl}
                  />
                ))}
                {appliedList.length === 0 && (
                  <div className="flex flex-col items-center justify-center h-64 text-slate-500">
                    <p>No applied jobs yet.</p>
                  </div>
                )}
              </div>
            </div>

            <AnimatePresence>
              {showJobDetails && selectedAppliedJobFull && (
                <div className="w-full sm:w-[50rem] border-l border-slate-800 bg-slate-950 flex flex-col shadow-2xl max-h-[85vh] sm:max-h-none sm:h-auto fixed sm:static inset-x-0 bottom-0 sm:bottom-auto sm:inset-auto z-40 sm:z-auto rounded-t-2xl sm:rounded-none">
                  <div className="flex items-start justify-between px-4 py-3 border-b border-slate-800/50 bg-slate-900/20">
                    <div className="min-w-0 pr-4">
                      <h2 className="text-lg font-bold text-white leading-tight mb-1.5">{selectedAppliedJobFull.title}</h2>
                      <div className="flex flex-wrap items-center gap-2 text-[11px] text-slate-300">
                        {selectedAppliedJobFull.company ? (
                          <a
                            href={buildCompanyJobsUrl(selectedAppliedJobFull.company)}
                            target="_blank"
                            rel="noreferrer"
                            onClick={(event) => event.stopPropagation()}
                            className="text-sm font-medium text-blue-400 mr-1 hover:text-blue-300 underline-offset-2 hover:underline"
                          >
                            {selectedAppliedJobFull.company}
                          </a>
                        ) : null}
                        {selectedAppliedJobFull.location && selectedAppliedJobFull.location !== "Unknown" && (
                          <span className="px-2 py-0.5 rounded-md border border-slate-800 bg-slate-900/70">
                            {selectedAppliedJobFull.location}
                          </span>
                        )}
                        {selectedAppliedJobFull.remote && (
                          <span className="px-2 py-0.5 rounded-md border border-emerald-600/60 bg-emerald-500/10 text-emerald-300 font-semibold">
                            Remote
                          </span>
                        )}
                        {selectedAppliedJobFull.level && (
                          <span className="px-2 py-0.5 rounded-md border border-slate-800 bg-slate-900/70">
                            {formatLevelLabel(selectedAppliedJobFull.level)}
                          </span>
                        )}
                        {!appliedCompMeta.isUnknown && (
                          <span className="px-2 py-0.5 rounded-md border border-slate-800 bg-slate-900/70">
                            <span className={appliedCompColorClass}>
                              {appliedCompMeta.display}
                            </span>
                          </span>
                        )}
                        {typeof selectedAppliedJobFull.postedAt === "number" && (
                          <span className="px-2 py-0.5 rounded-md border border-slate-800 bg-slate-900/70">
                            Posted {formatPostedLabel(selectedAppliedJobFull.postedAt)}
                          </span>
                        )}
                      </div>
                    </div>
                    <button
                      onClick={() => setShowJobDetails(false)}
                      className="shrink-0 p-2 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition-colors"
                      aria-label="Close job details"
                    >
                      <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                      </svg>
                    </button>
                  </div>

                  <div className="flex-1 overflow-y-auto custom-scrollbar">
                    <div className="p-3 space-y-2">
                      <div className="flex justify-center w-full pb-2">
                        <StatusTracker
                          status={selectedAppliedJobFull.workerStatus || (selectedAppliedJobFull.userStatus === 'applied' ? 'Applied' : null)}
                          updatedAt={selectedAppliedJobFull.workerUpdatedAt || selectedAppliedJobFull.appliedAt}
                        />
                      </div>

                      <div className="flex gap-2">
                        {selectedAppliedJobFull.url && (
                          <button
                            onClick={() => window.open(selectedAppliedJobFull.url as string, "_blank")}
                            className="flex-1 px-4 py-2.5 text-sm font-semibold uppercase tracking-wide text-slate-900 bg-blue-400 hover:bg-blue-300 border border-blue-500 shadow-lg shadow-blue-900/30 transition-transform active:scale-[0.99]"
                          >
                            View Job Posting
                          </button>
                        )}
                      </div>


                      <div className="rounded-lg border border-slate-800/70 bg-slate-900/50 p-2 space-y-3">
                        <div className="flex items-center justify-between">
                          <h3 className="text-sm font-semibold text-slate-100">Job Description</h3>
                          <div className="flex items-center gap-2 text-[11px] text-slate-500">
                            {appliedDescriptionWordCount !== null && (
                              <span>{`${appliedDescriptionWordCount} words`}</span>
                            )}
                            <button
                              type="button"
                              onClick={() => { void handleCopyJobLink(selectedAppliedJobFull._id); }}
                              className="inline-flex h-6 w-6 items-center justify-center rounded border border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-500 hover:bg-slate-800 transition-colors"
                              aria-label="Copy job link"
                              title="Copy job link"
                            >
                              <svg className="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 13a5 5 0 0 1 0-7l1.5-1.5a5 5 0 0 1 7 7L17 12" />
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14 11a5 5 0 0 1 0 7l-1.5 1.5a5 5 0 0 1-7-7L7 12" />
                              </svg>
                            </button>
                          </div>
                        </div>
                        <div className="text-sm leading-relaxed text-slate-300 font-sans max-h-[60vh] overflow-y-auto pr-1 space-y-3">
                          <ReactMarkdown remarkPlugins={[remarkGfm, remarkBreaks]} components={markdownComponents}>
                            {appliedDescriptionText}
                          </ReactMarkdown>
                        </div>
                      </div>

                      {appliedMetadataText && (
                        <div className="rounded-lg border border-slate-800/70 bg-slate-900/35 p-2">
                          <div className="text-[10px] uppercase tracking-wider font-semibold text-slate-500 mb-2">
                            Metadata
                          </div>
                          <div className="text-sm leading-relaxed text-slate-300 font-sans max-h-56 overflow-y-auto pr-1 space-y-3">
                            <ReactMarkdown remarkPlugins={[remarkGfm, remarkBreaks]} components={markdownComponents}>
                              {appliedMetadataText}
                            </ReactMarkdown>
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              )}
            </AnimatePresence>
          </div>
        )}

        {activeTab === "rejected" && (
          <div className="flex-1 flex bg-slate-950 overflow-hidden">
            <div className="flex-1 flex flex-col min-w-0">
              {/* Header Row - Matches JobRow grid */}
              <div className="flex items-center gap-3 px-4 py-2 border-b border-slate-800 bg-slate-900/50 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                <div className="w-8" />
                <div className="flex-1 grid grid-cols-[auto_6fr_3fr] md:grid-cols-[auto_8fr_3fr_2fr_2fr_2fr] gap-3 items-center">
                  <div className="w-8" />
                  <div>Job</div>
                  <div className="hidden sm:block">Location</div>
                  <div className="text-right hidden sm:block">Salary</div>
                  <div className="text-right hidden sm:block">Rejected</div>
                  <div className="hidden sm:block" />
                </div>
              </div>

              <div className="flex-1 overflow-y-auto">
                {rejectedList.map(job => (
                  <JobRow
                    key={job._id}
                    job={job}
                    variant="rejected"
                    isSelected={selectedJobId === job._id}
                    onSelect={() => handleSelectJob(job._id)}
                    getCompanyJobsUrl={buildCompanyJobsUrl}
                  />
                ))}
                {rejectedList.length === 0 && (
                  <div className="flex flex-col items-center justify-center h-64 text-slate-500">
                    <p>No rejected jobs.</p>
                  </div>
                )}
              </div>
            </div>

            <AnimatePresence>
              {showJobDetails && selectedJobId && (() => {
                const selectedRejectedJob = rejectedList.find(j => j._id === selectedJobId);
                if (!selectedRejectedJob) return null;
                const rejectedCompMeta = buildCompensationMeta(selectedRejectedJob);
                const rejectedCompColorClass = rejectedCompMeta.isEstimated ? "text-slate-300" : "text-emerald-200";
                return (
                  <div className="w-full sm:w-[50rem] border-l border-slate-800 bg-slate-950 flex flex-col shadow-2xl max-h-[85vh] sm:max-h-none sm:h-auto fixed sm:static inset-x-0 bottom-0 sm:bottom-auto sm:inset-auto z-40 sm:z-auto rounded-t-2xl sm:rounded-none">
                    <div className="flex items-start justify-between px-4 py-3 border-b border-slate-800/50 bg-slate-900/20">
                      <div className="min-w-0 pr-4">
                        <h2 className="text-lg font-bold text-white leading-tight mb-1.5">{selectedRejectedJob.title}</h2>
                        <div className="flex flex-wrap items-center gap-2 text-[11px] text-slate-300">
                          {selectedRejectedJob.company ? (
                            <a
                              href={buildCompanyJobsUrl(selectedRejectedJob.company)}
                              target="_blank"
                              rel="noreferrer"
                              onClick={(event) => event.stopPropagation()}
                              className="text-sm font-medium text-blue-400 mr-1 hover:text-blue-300 underline-offset-2 hover:underline"
                            >
                              {selectedRejectedJob.company}
                            </a>
                          ) : null}
                          {selectedRejectedJob.location && selectedRejectedJob.location !== "Unknown" && (
                            <span className="px-2 py-0.5 rounded-md border border-slate-800 bg-slate-900/70">
                              {selectedRejectedJob.location}
                            </span>
                          )}
                          {selectedRejectedJob.remote && (
                            <span className="px-2 py-0.5 rounded-md border border-emerald-600/60 bg-emerald-500/10 text-emerald-300 font-semibold">
                              Remote
                            </span>
                          )}
                          {selectedRejectedJob.level && (
                            <span className="px-2 py-0.5 rounded-md border border-slate-800 bg-slate-900/70">
                              {formatLevelLabel(selectedRejectedJob.level)}
                            </span>
                          )}
                          {!rejectedCompMeta.isUnknown && (
                            <span className="px-2 py-0.5 rounded-md border border-slate-800 bg-slate-900/70">
                              <span className={rejectedCompColorClass}>
                                {rejectedCompMeta.display}
                              </span>
                            </span>
                          )}
                          {typeof selectedRejectedJob.postedAt === "number" && (
                            <span className="px-2 py-0.5 rounded-md border border-slate-800 bg-slate-900/70">
                              Posted {formatPostedLabel(selectedRejectedJob.postedAt)}
                            </span>
                          )}
                        </div>
                      </div>
                      <button
                        onClick={() => setShowJobDetails(false)}
                        className="shrink-0 p-2 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition-colors"
                        aria-label="Close job details"
                      >
                        <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                        </svg>
                      </button>
                    </div>

                    <div className="flex-1 overflow-y-auto custom-scrollbar">
                      <div className="p-3 space-y-2">
                        <div className="flex gap-2">
                          {selectedRejectedJob.url && (
                            <button
                              onClick={() => window.open(selectedRejectedJob.url as string, "_blank")}
                              className="flex-1 px-4 py-2.5 text-sm font-semibold uppercase tracking-wide text-slate-900 bg-red-400 hover:bg-red-300 border border-red-500 shadow-lg shadow-red-900/30 transition-transform active:scale-[0.99]"
                            >
                              View Job Posting
                            </button>
                          )}
                        </div>


                        <div className="rounded-lg border border-slate-800/70 bg-slate-900/50 p-2 space-y-3">
                          <div className="flex items-center justify-between">
                            <h3 className="text-sm font-semibold text-slate-100">Job Description</h3>
                            <button
                              type="button"
                              onClick={() => { void handleCopyJobLink(selectedRejectedJob._id); }}
                              className="inline-flex h-6 w-6 items-center justify-center rounded border border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-500 hover:bg-slate-800 transition-colors"
                              aria-label="Copy job link"
                              title="Copy job link"
                            >
                              <svg className="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 13a5 5 0 0 1 0-7l1.5-1.5a5 5 0 0 1 7 7L17 12" />
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14 11a5 5 0 0 1 0 7l-1.5 1.5a5 5 0 0 1-7-7L7 12" />
                              </svg>
                            </button>
                          </div>
                          <div className="text-sm leading-relaxed text-slate-300 font-sans max-h-[60vh] overflow-y-auto pr-1 space-y-3">
                            <ReactMarkdown remarkPlugins={[remarkGfm, remarkBreaks]} components={markdownComponents}>
                              {selectedRejectedJob.description || "No description available."}
                            </ReactMarkdown>
                          </div>
                        </div>

                        <div className="rounded-lg border border-slate-800/70 bg-slate-900/40 p-2 space-y-2">
                          <div className="text-[11px] uppercase tracking-wider font-semibold text-slate-500">Links</div>
                          {selectedRejectedJob.url ? (
                            <a
                              href={selectedRejectedJob.url}
                              target="_blank"
                              rel="noreferrer"
                              className="text-sm text-blue-300 hover:text-blue-200 underline break-all"
                            >
                              {selectedRejectedJob.url}
                            </a>
                          ) : (
                            <div className="text-sm text-slate-400">No link available</div>
                          )}
                        </div>
                      </div>
                    </div>
                  </div>
                );
              })()}
            </AnimatePresence>
          </div>
        )}

        {activeTab === "live" && (
          <div className="flex-1 p-6 overflow-y-auto">
            <h2 className="text-xl font-semibold mb-4 text-white">Live Feed</h2>
            <div className="space-y-2">
              {recentJobs?.map(job => (
                <div key={job._id} className="group flex items-center justify-between px-4 py-2 border-b border-slate-800 hover:bg-slate-900 transition-colors cursor-default">
                  <div className="flex items-center gap-3 min-w-0">
                    <h3 className="text-sm font-medium text-slate-300 truncate group-hover:text-white">{job.title}</h3>
                    <span className="text-xs text-slate-500 truncate">{job.company}</span>
                  </div>
                  <span className="text-xs text-slate-600 whitespace-nowrap ml-4 font-mono">
                    {new Date(job.postedAt).toLocaleString(undefined, {
                      month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit'
                    })}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div >
  );
}
