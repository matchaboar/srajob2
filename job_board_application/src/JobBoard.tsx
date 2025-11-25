import { useState, useEffect, useRef, useCallback, useMemo, type CSSProperties } from "react";
import { usePaginatedQuery, useMutation, useQuery } from "convex/react";
import { api } from "../convex/_generated/api";
import { toast } from "sonner";
import { motion, AnimatePresence } from "framer-motion";
import { JobRow } from "./components/JobRow";
import { AppliedJobRow } from "./components/AppliedJobRow";
import { RejectedJobRow } from "./components/RejectedJobRow";
import { Keycap } from "./components/Keycap";
import { formatCompensationDisplay, parseCompensationInput } from "./lib/compensation";

type Level = "junior" | "mid" | "senior" | "staff";
const TARGET_STATES = ["Washington", "New York", "California", "Arizona"] as const;
type TargetState = (typeof TARGET_STATES)[number];

interface Filters {
  search: string;
  includeRemote: boolean;
  state: TargetState | null;
  level: Level | null;
  minCompensation: number | null;
  maxCompensation: number | null;
}

interface SavedFilter {
  _id: string;
  name: string;
  search?: string;
  remote?: boolean;
  includeRemote?: boolean;
  state?: TargetState | null;
  level?: Level | null;
  minCompensation?: number;
  maxCompensation?: number;
  isSelected: boolean;
}

const buildEmptyFilters = (): Filters => ({
  search: "",
  includeRemote: true,
  state: null,
  level: null,
  minCompensation: null,
  maxCompensation: null,
});

const buildFilterLabel = (filter: {
  search?: string | null;
  state?: TargetState | null;
  includeRemote?: boolean | null;
  level?: Level | null;
  remote?: boolean | null;
  minCompensation?: number | null;
  maxCompensation?: number | null;
}) => {
  const parts: string[] = [];
  const trimmedSearch = (filter.search ?? "").trim();
  if (trimmedSearch) {
    parts.push(trimmedSearch);
  }
  if (filter.level) {
    parts.push(filter.level.charAt(0).toUpperCase() + filter.level.slice(1));
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

  return parts.join(" • ") || "All jobs";
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
  const [activeTab, setActiveTab] = useState<"jobs" | "applied" | "rejected" | "live">(() => {
    const hash = window.location.hash.replace("#", "");
    if (hash === "applied" || hash === "rejected" || hash === "live") return hash as any;
    return "jobs";
  });

  const [filters, setFilters] = useState<Filters>(buildEmptyFilters);
  const [throttledFilters, setThrottledFilters] = useState<Filters>(buildEmptyFilters);
  const [filtersReady, setFiltersReady] = useState(false);
  const [selectedSavedFilterId, setSelectedSavedFilterId] = useState<string | null>(null);
  const [minCompensationInput, setMinCompensationInput] = useState("");
  const [sliderValue, setSliderValue] = useState(200000);
  const [filterUpdatePending, setFilterUpdatePending] = useState(false);
  const [filterCountdownMs, setFilterCountdownMs] = useState(0);
  const [showShortcuts, setShowShortcuts] = useState(false);
  const [showJobDetails, setShowJobDetails] = useState(false);
  const [keyboardNavActive, setKeyboardNavActive] = useState(false);
  const [keyboardTopIndex, setKeyboardTopIndex] = useState<number | null>(null);
  const jobListRef = useRef<HTMLDivElement | null>(null);

  const [locallyAppliedJobs, setLocallyAppliedJobs] = useState<Set<string>>(new Set());
  const [exitingJobs, setExitingJobs] = useState<Record<string, "apply" | "reject">>({});
  const [locallyWithdrawnJobs] = useState<Set<string>>(new Set());

  // Selection state for keyboard navigation
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
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
      if (hash === "applied" || hash === "rejected" || hash === "live") {
        setActiveTab(hash);
      } else if (hash === "" || hash === "jobs") {
        setActiveTab("jobs");
      }
    };
    window.addEventListener("hashchange", handleHashChange);
    return () => window.removeEventListener("hashchange", handleHashChange);
  }, []);

  const { results, status, loadMore } = usePaginatedQuery(
    api.jobs.listJobs,
    {
      search: throttledFilters.search || undefined,
      state: throttledFilters.state ?? undefined,
      includeRemote: throttledFilters.includeRemote,
      level: throttledFilters.level ?? undefined,
      minCompensation: throttledFilters.minCompensation ?? undefined,
      maxCompensation: throttledFilters.maxCompensation ?? undefined,
    },
    { initialNumItems: 50 } // Load more items for the dense list
  );

  const [displayedResults, setDisplayedResults] = useState(results);
  useEffect(() => {
    // Keep showing the previous page while a new filter set is loading.
    if (status === "LoadingFirstPage") return;
    setDisplayedResults(results);
  }, [results, status]);

  const savedFilters = useQuery(api.filters.getSavedFilters);
  const recentJobs = useQuery(api.jobs.getRecentJobs);
  const appliedJobs = useQuery(api.jobs.getAppliedJobs);
  const rejectedJobs = useQuery(api.jobs.getRejectedJobs);
  const applyToJob = useMutation(api.jobs.applyToJob);
  const rejectJob = useMutation(api.jobs.rejectJob);
  // Withdraw not used in this view; keep mutation available for future enhancements
  const ensureDefaultFilter = useMutation(api.filters.ensureDefaultFilter);
  const saveFilter = useMutation(api.filters.saveFilter);
  const selectSavedFilter = useMutation(api.filters.selectSavedFilter);
  const deleteSavedFilter = useMutation(api.filters.deleteSavedFilter);

  // Filter out locally applied/rejected jobs
  const filteredResults = (displayedResults || []).filter(job => !locallyAppliedJobs.has(job._id));
  const appliedList = (appliedJobs || []).filter(job => !locallyWithdrawnJobs.has(job._id));
  const rejectedList = rejectedJobs || [];
  const selectedJob =
    filteredResults.find((job) => job._id === selectedJobId) ??
    (displayedResults || []).find((job) => job._id === selectedJobId) ??
    null;
  const formatCurrency = useCallback((value: number) => {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      maximumFractionDigits: 0,
    }).format(value);
  }, []);
  const formatPostedLabel = useCallback((timestamp: number) => {
    const days = Math.max(0, Math.floor((Date.now() - timestamp) / (1000 * 60 * 60 * 24)));
    const dateLabel = new Date(timestamp).toLocaleDateString(undefined, { month: "short", day: "numeric" });
    return `${dateLabel} • ${days}d ago`;
  }, []);
  const formatLevelLabel = useCallback((level?: string | null) => {
    if (!level || typeof level !== "string") return "Not specified";
    return level.charAt(0).toUpperCase() + level.slice(1);
  }, []);
  const selectedJobDetails = useMemo(() => {
    if (!selectedJob) return [];
    const details: Array<{ label: string; value: string; badge?: string; type?: "link" }> = [];

    details.push({ label: "Company", value: selectedJob.company });
    details.push({ label: "Level", value: formatLevelLabel(selectedJob.level) });
    details.push({
      label: "Location",
      value: selectedJob.location || "Unknown",
      badge: selectedJob.remote ? "Remote" : undefined,
    });
    details.push({
      label: "Compensation",
      value: formatCurrency(typeof selectedJob.totalCompensation === "number" ? selectedJob.totalCompensation : 0),
    });
    details.push({
      label: "Posted",
      value: typeof selectedJob.postedAt === "number" ? formatPostedLabel(selectedJob.postedAt) : "Not provided",
    });
    // Scrape metadata rendered below description; omit here.
    details.push({
      label: "Applications",
      value: String(selectedJob.applicationCount ?? 0),
    });
    if (selectedJob.url) {
      details.push({
        label: "Job URL",
        value: selectedJob.url,
        type: "link",
      });
    }

    return details;
  }, [selectedJob, formatCurrency, formatLevelLabel, formatPostedLabel]);
  const descriptionText = selectedJob?.description || selectedJob?.job_description || "No description available.";
  const descriptionWordCount =
    selectedJob && (selectedJob.description || selectedJob.job_description)
      ? descriptionText.split(/\s+/).filter(Boolean).length
      : null;
  const blurFromIndex =
    keyboardNavActive && keyboardTopIndex !== null ? keyboardTopIndex + 3 : Infinity;
  const scrollToJob = useCallback(
    (jobId: string, alignToFloor: boolean) => {
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
      level: (filter.level as Level | null) ?? null,
      minCompensation: filter.minCompensation ?? null,
      maxCompensation: filter.maxCompensation ?? null,
    });
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
  const stateSelectId = "job-board-state-filter";
  const levelSelectId = "job-board-level-filter";
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

  const handleSaveCurrentFilter = useCallback(async () => {
    const trimmedName = generatedFilterName.trim() || "Saved filter";
    try {
      await saveFilter({
        name: trimmedName,
        search: filters.search || undefined,
        includeRemote: filters.includeRemote,
        state: filters.state || undefined,
        level: filters.level ?? undefined,
        minCompensation: filters.minCompensation ?? undefined,
        maxCompensation: filters.maxCompensation ?? undefined,
      });
      toast.success("Filter saved");
    } catch (_error) {
      toast.error("Failed to save filter");
    }
  }, [filters, generatedFilterName, saveFilter]);

  const handleSelectSavedFilter = useCallback(async (filterId: string | null) => {
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
    } catch (_error) {
      toast.error("Failed to select filter");
    }
  }, [applySavedFilterToState, resetFilters, savedFilters, selectSavedFilter]);

  useEffect(() => {
    if (savedFilters === undefined) return;

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
  }, [applySavedFilterToState, ensureDefaultFilter, pendingSelectionClearRef, savedFilters, selectedSavedFilterId]);

  const savedFilterList = useMemo(
    () => (savedFilters as SavedFilter[] | undefined) || [],
    [savedFilters]
  );
  const anyServerSelection = savedFilterList.some((f) => f.isSelected);
  const noFilterActive = !selectedSavedFilterId && !anyServerSelection;

  const handleDeleteSavedFilter = useCallback(async (filterId: string) => {
    const matchingFilter = savedFilterList.find((f) => f._id === filterId);
    const wasActive = filterId === selectedSavedFilterId || matchingFilter?.isSelected;

    try {
      await deleteSavedFilter({ filterId: filterId as any });
      if (wasActive) {
        resetFilters();
      }
      toast.success("Filter deleted");
    } catch (_error) {
      toast.error("Failed to delete filter");
    }
  }, [deleteSavedFilter, resetFilters, savedFilterList, selectedSavedFilterId]);

  // Select top job on load or when results change
  useEffect(() => {
    if (filteredResults.length === 0) {
      setSelectedJobId(null);
      setShowJobDetails(false);
      return;
    }
    const stillVisible = filteredResults.some((job) => job._id === selectedJobId);
    if (!selectedJobId || !stillVisible) {
      setSelectedJobId(filteredResults[0]._id);
    }
  }, [filteredResults, selectedJobId]);

  const handleSelectJob = useCallback((jobId: string) => {
    setSelectedJobId(jobId);
    setShowJobDetails(true);
    setKeyboardNavActive(false);
    setKeyboardTopIndex(null);
  }, []);

  const handleApply = useCallback(async (jobId: string, type: "ai" | "manual", url?: string) => {
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
      await applyToJob({ jobId: jobId as any, type });
      toast.success(`Applied to job!`);
      if (type === "manual" && url) {
        window.open(url, "_blank");
      }
    } catch (_error) {
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

  const handleReject = useCallback(async (jobId: string) => {
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
      await rejectJob({ jobId: jobId as any });
      toast.success("Job rejected");
    } catch (_error) {
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

      if (activeTab !== "jobs") return;

      // Ignore keyboard shortcuts if Ctrl, Cmd, or Alt is pressed
      if (e.ctrlKey || e.metaKey || e.altKey) return;

      const target = e.target as HTMLElement | null;
      const typingTarget = target?.closest("input, textarea, select, button, [role='textbox']");
      if (target?.isContentEditable || typingTarget) return;

      if (!selectedJobId || filteredResults.length === 0) return;

      const currentIndex = filteredResults.findIndex(j => j._id === selectedJobId);
      if (currentIndex === -1) return;

      switch (e.key) {
        case "ArrowDown":
        case "j":
          e.preventDefault();
          setKeyboardNavActive(true);
          if (currentIndex < filteredResults.length - 1) {
            const nextId = filteredResults[currentIndex + 1]._id;
            const nextIndex = currentIndex + 1;
            setKeyboardTopIndex(nextIndex >= 3 ? nextIndex - 3 : 0);
            setSelectedJobId(nextId);
            scrollToJob(nextId, currentIndex + 1 >= 3);
          } else if (status === "CanLoadMore") {
            loadMore(20);
          }
          break;
        case "ArrowUp":
        case "k":
          e.preventDefault();
          setKeyboardNavActive(true);
          if (currentIndex > 0) {
            const prevId = filteredResults[currentIndex - 1]._id;
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
          const jobToApply = filteredResults[currentIndex];
          void handleApply(jobToApply._id, "ai", jobToApply.url);
          break;
        }
        case "r": {
          e.preventDefault();
          void handleReject(filteredResults[currentIndex]._id);
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
      <div className="flex items-center justify-between px-6 py-3 border-b border-slate-800 bg-slate-900/50">
        <div className="flex space-x-1 bg-slate-900 p-1 rounded-lg border border-slate-800">
          {(["jobs", "applied", "rejected", "live"] as const).map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-all ${activeTab === tab
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
        <div className="flex items-center gap-3 text-xs text-slate-400">
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
              {/* Sidebar Filters */}
              <div className="w-64 bg-slate-900/30 border-r border-slate-800 p-4 flex flex-col gap-6 overflow-y-auto">
                <div>
                  <label className="block text-xs font-semibold text-slate-500 uppercase mb-2">Search</label>
                  <input
                    type="text"
                    value={filters.search}
                    onChange={(e) => updateFilters({ search: e.target.value })}
                    placeholder="Keywords..."
                    className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500 focus:ring-1 focus:ring-blue-500 placeholder-slate-600"
                  />
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
                    className={`relative h-6 w-11 rounded-full border transition-colors duration-150 overflow-hidden ${
                      filters.includeRemote ? "bg-emerald-500/40 border-emerald-400" : "bg-slate-800 border-slate-700"
                    }`}
                    aria-label={filters.includeRemote ? "Remote on" : "Remote off"}
                  >
                    <span
                      className={`absolute left-0.5 top-0.5 h-5 w-5 rounded-full bg-white shadow-sm transition-transform duration-150 ${
                        filters.includeRemote ? "translate-x-5" : "translate-x-0"
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
                          includeRemote: filter.includeRemote ?? (filter.remote !== false),
                          level: (filter.level as Level | null) ?? null,
                          minCompensation: filter.minCompensation ?? null,
                          maxCompensation: filter.maxCompensation ?? null,
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
                    <div className="sticky top-0 z-20 relative flex items-center gap-4 px-4 pr-36 py-2 border-b border-slate-800 bg-slate-900/80 backdrop-blur text-xs font-semibold text-slate-500 uppercase tracking-wider">
                      <div className="w-1" /> {/* Spacer for alignment with selection indicator */}
                      <div className="flex-1 grid grid-cols-[4fr_3fr_2fr_3fr_2fr] gap-4 items-center">
                        <div>Job</div>
                        <div>Location</div>
                        <div className="text-center">Level</div>
                        <div className="text-right">Salary</div>
                        <div className="text-right">Posted</div>
                      </div>
                      <div className="absolute inset-y-0 right-0 flex items-center justify-end gap-0 w-36 pl-2 pr-0 pointer-events-none" aria-hidden="true">
                        <span className="px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-slate-400">Action</span>
                      </div>
                    </div>

                    <div className="min-h-full">
                      <AnimatePresence initial={false}>
                        {filteredResults.map((job, idx) => (
                          <JobRow
                            key={job._id}
                            job={job}
                            isSelected={selectedJobId === job._id}
                            onSelect={() => handleSelectJob(job._id)}
                            onApply={(type) => { void handleApply(job._id, type, job.url); }}
                            onReject={() => { void handleReject(job._id); }}
                            isExiting={exitingJobs[job._id]}
                            keyboardBlur={idx > blurFromIndex}
                          />
                        ))}
                      </AnimatePresence>

                      {status === "CanLoadMore" && (
                        <div className="p-4 flex justify-center border-t border-slate-800">
                          <button
                            onClick={() => loadMore(20)}
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

                {showJobDetails && selectedJob && (
                  <div className="w-[32rem] border-l border-slate-800 bg-slate-950 flex flex-col shadow-2xl">
                    <div className="flex items-start justify-between px-6 py-4 border-b border-slate-800/50 bg-slate-900/20">
                      <div className="min-w-0 pr-4">
                        <h2 className="text-lg font-bold text-white leading-tight mb-1">{selectedJob.title}</h2>
                        <div className="text-sm font-medium text-blue-400">{selectedJob.company}</div>
                        <div className="mt-2 flex flex-wrap gap-2 text-[11px] text-slate-300">
                          <span className="px-2 py-1 rounded-md border border-slate-800 bg-slate-900/70">{selectedJob.location || "Unknown"}</span>
                          {selectedJob.remote && (
                            <span className="px-2 py-1 rounded-md border border-emerald-600/60 bg-emerald-500/10 text-emerald-300 font-semibold">
                              Remote
                            </span>
                          )}
                          <span className="px-2 py-1 rounded-md border border-slate-800 bg-slate-900/70">
                            {formatLevelLabel(selectedJob.level)}
                          </span>
                          <span className="px-2 py-1 rounded-md border border-slate-800 bg-slate-900/70">
                            {formatCurrency(typeof selectedJob.totalCompensation === "number" ? selectedJob.totalCompensation : 0)}
                          </span>
                          <span className="px-2 py-1 rounded-md border border-slate-800 bg-slate-900/70">
                            {typeof selectedJob.postedAt === "number" ? formatPostedLabel(selectedJob.postedAt) : "Not provided"}
                          </span>
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
                      <div className="p-5 space-y-4">
                        <div className="flex gap-2">
                          {selectedJob.url && (
                            <button
                              onClick={() => { void handleApply(selectedJob._id, "manual", selectedJob.url); }}
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

                        {selectedJobDetails.find(item => item.label === "Job URL") && (
                        <div className="rounded-lg border border-slate-800/70 bg-slate-900/50 px-3 py-2 flex flex-col gap-1">
                          <div className="text-[10px] uppercase tracking-wider font-semibold text-slate-500">
                            Job URL
                          </div>
                          <div className="text-sm font-medium text-slate-100 flex items-center gap-2 break-words">
                            <a
                              href={selectedJobDetails.find(item => item.label === "Job URL")?.value}
                              target="_blank"
                              rel="noreferrer"
                              className="text-blue-300 hover:text-blue-200 underline-offset-2 break-all"
                            >
                              {selectedJobDetails.find(item => item.label === "Job URL")?.value}
                            </a>
                          </div>
                        </div>
                      )}

                        <div className="grid grid-cols-2 gap-2">
                          {selectedJobDetails.filter(item => item.label !== "Job URL").map((item) => (
                            <div
                              key={item.label}
                              className="rounded-lg border border-slate-800/70 bg-slate-900/50 px-3 py-2 flex flex-col gap-1"
                            >
                              <div className="text-[10px] uppercase tracking-wider font-semibold text-slate-500">
                                {item.label}
                              </div>
                              <div className="text-sm font-medium text-slate-100 flex items-center gap-2 break-words">
                                {item.badge && (
                                  <span className="px-1.5 py-0.5 text-[10px] font-semibold rounded bg-emerald-500/10 text-emerald-300 border border-emerald-500/30">
                                    {item.badge}
                                  </span>
                                )}
                                {item.type === "link" ? (
                                  <a
                                    href={item.value}
                                    target="_blank"
                                    rel="noreferrer"
                                    className="text-blue-300 hover:text-blue-200 underline-offset-2"
                                  >
                                    {item.value}
                                  </a>
                                ) : (
                                  <span className="truncate text-slate-200">{item.value}</span>
                                )}
                              </div>
                            </div>
                          ))}
                        </div>

                        <div className="rounded-lg border border-slate-800/70 bg-slate-900/40 p-3">
                          <div className="flex items-center justify-between mb-2">
                            <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider">Description</h3>
                            <span className="text-[11px] text-slate-500">
                              {descriptionWordCount !== null ? `${descriptionWordCount} words` : ""}
                            </span>
                          </div>
                          <div className="text-sm leading-relaxed text-slate-300 whitespace-pre-wrap font-sans max-h-72 overflow-y-auto pr-1">
                            {descriptionText}
                          </div>
                        </div>

                        <div className="rounded-lg border border-slate-800/70 bg-slate-900/40 p-3 space-y-2">
                          <div className="text-[10px] uppercase tracking-wider font-semibold text-slate-500">
                            Scrape Info
                          </div>
                          <div className="flex items-start gap-2 text-sm text-slate-200">
                            <span className="w-28 text-slate-500">Scraped</span>
                            <span className="font-semibold text-slate-100 break-words">
                              {typeof selectedJob?.scrapedAt === "number"
                                ? new Date(selectedJob.scrapedAt).toLocaleString(undefined, {
                                    month: "short",
                                    day: "numeric",
                                    hour: "2-digit",
                                    minute: "2-digit",
                                  })
                                : "None"}
                              {selectedJob?.scrapedWith ? ` • ${selectedJob.scrapedWith}` : ""}
                            </span>
                          </div>
                          <div className="flex items-start gap-2 text-sm text-slate-200">
                            <span className="w-28 text-slate-500">Workflow</span>
                            <span className="font-semibold text-slate-100 break-words">
                              {selectedJob?.workflowName || "None"}
                            </span>
                          </div>
                          <div className="flex items-start gap-2 text-sm text-slate-200">
                            <span className="w-28 text-slate-500">Scrape Cost</span>
                            <span className="font-semibold text-slate-100 break-words">
                              {typeof selectedJob?.scrapedCostMilliCents === "number"
                                ? (() => {
                                    const mc = selectedJob.scrapedCostMilliCents;
                                    const renderFraction = (numerator: number, denominator: number) => (
                            <span className="inline-flex items-center text-[12px] font-semibold text-amber-400/90">
                                <span className="flex flex-col leading-tight items-center mr-0.5">
                                    <span className="px-0.5">{numerator}</span>
                                    <span className="px-0.5">{denominator}</span>
                                </span>
                                <span className="text-[10px] text-amber-300 mx-0.5">/</span>
                                <span className="text-[10px] text-amber-300 ml-0.5">¢</span>
                            </span>
                                    );

                                    if (mc >= 1000) return `${(mc / 1000).toFixed(2)} ¢`;
                                    if (mc === 100) return renderFraction(1, 10);
                                    if (mc === 10) return renderFraction(1, 100);
                                    if (mc === 1) return renderFraction(1, 1000);
                                    if (mc > 0) return `${(mc / 1000).toFixed(3)} ¢`;
                                    return "0 ¢";
                                  })()
                                : "None"}
                            </span>
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

        {activeTab === "applied" && (
          <div className="flex-1 flex flex-col bg-slate-950 overflow-hidden">
            {/* Header Row */}
            <div className="flex items-center gap-4 px-4 py-2 border-b border-slate-800 bg-slate-900/50 text-xs font-semibold text-slate-500 uppercase tracking-wider">
              <div className="w-1" /> {/* Spacer for alignment with selection indicator */}
              <div className="flex-1 grid grid-cols-12 gap-4">
                <div className="col-span-4">Job</div>
                <div className="col-span-2">Location</div>
                <div className="col-span-2 text-right">Applied</div>
                <div className="col-span-4 text-right">Status</div>
              </div>
            </div>

            <div className="flex-1 overflow-y-auto">
              <div className="min-h-full">
                {appliedList.map(job => (
                  <AppliedJobRow
                    key={job._id}
                    job={job}
                    isSelected={selectedJobId === job._id}
                    onSelect={() => setSelectedJobId(job._id)}
                  />
                ))}
                {appliedList.length === 0 && (
                  <div className="flex flex-col items-center justify-center h-64 text-slate-500">
                    <p>No applied jobs yet.</p>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {activeTab === "rejected" && (
          <div className="flex-1 flex flex-col bg-slate-950 overflow-hidden">
            <div className="flex items-center gap-4 px-4 py-2 border-b border-slate-800 bg-slate-900/50 text-xs font-semibold text-slate-500 uppercase tracking-wider">
              <div className="w-1" />
              <div className="flex-1 grid grid-cols-12 gap-4">
                <div className="col-span-5">Job</div>
                <div className="col-span-3">Location</div>
                <div className="col-span-2 text-right">Rejected</div>
                <div className="col-span-2 text-right">Level</div>
              </div>
            </div>

            <div className="flex-1 overflow-y-auto">
              <div className="min-h-full">
                {rejectedList.map(job => (
                  <RejectedJobRow
                    key={job._id}
                    job={job}
                    isSelected={selectedJobId === job._id}
                    onSelect={() => setSelectedJobId(job._id)}
                  />
                ))}
                {rejectedList.length === 0 && (
                  <div className="flex flex-col items-center justify-center h-64 text-slate-500">
                    <p>No rejected jobs.</p>
                  </div>
                )}
              </div>
            </div>
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
