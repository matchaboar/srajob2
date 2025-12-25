// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { getFunctionName } from "convex/server";
import { api } from "../convex/_generated/api";
import { JobBoard } from "./JobBoard";
import { toast } from "sonner";

let paginatedResults: any[] = [];
let appliedJobsFixture: any[] = [];
let rejectedJobsFixture: any[] = [];
let jobDetailsById: Record<string, any> = {};
let savedFiltersFixture: any[] = [];
let recentJobsFixture: any[] = [];

const PROD_CONVEX_HTTP_URL = "https://affable-kiwi-46.convex.site";
const PROD_CONVEX_URL = "https://affable-kiwi-46.convex.cloud";

const resetFixtures = () => {
  paginatedResults = [];
  appliedJobsFixture = [];
  rejectedJobsFixture = [];
  jobDetailsById = {};
  savedFiltersFixture = [];
  recentJobsFixture = [];
};

const queryNames = {
  applied: getFunctionName(api.jobs.getAppliedJobs),
  rejected: getFunctionName(api.jobs.getRejectedJobs),
  recent: getFunctionName(api.jobs.getRecentJobs),
  savedFilters: getFunctionName(api.filters.getSavedFilters),
  jobDetails: getFunctionName(api.jobs.getJobDetails),
  searchCompanies: getFunctionName(api.jobs.searchCompanies),
  ignored: getFunctionName(api.router.listIgnoredJobs),
};

const safeQueryName = (queryFn: any) => {
  try {
    return getFunctionName(queryFn);
  } catch {
    return null;
  }
};

vi.mock("convex/react", () => {
  const usePaginatedQuery = vi.fn(() => ({
    results: paginatedResults,
    status: "Complete",
    loadMore: vi.fn(),
  }));

  const useQuery = vi.fn((queryFn: any, args?: any) => {
    if (args === "skip") return undefined;
    const queryName = safeQueryName(queryFn);
    if (queryName === queryNames.applied) return appliedJobsFixture;
    if (queryName === queryNames.rejected) return rejectedJobsFixture;
    if (queryName === queryNames.recent) return recentJobsFixture;
    if (queryName === queryNames.savedFilters) return savedFiltersFixture;
    if (queryName === queryNames.jobDetails) {
      const jobId = args?.jobId as string | undefined;
      return (jobId && jobDetailsById[jobId]) ?? {};
    }
    if (queryName === queryNames.searchCompanies) return [];
    if (queryName === queryNames.ignored) return [];
    return [];
  });

  const useMutation = vi.fn(() => vi.fn(async () => ({})));

  return {
    usePaginatedQuery,
    useQuery,
    useMutation,
  };
});

vi.mock("sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}));

vi.mock("framer-motion", () => {
  const passthrough = ({ children, ...rest }: any) => <div {...rest}>{children}</div>;
  const motion = new Proxy(
    {},
    {
      get: () => passthrough,
    },
  );

  return {
    AnimatePresence: ({ children }: any) => <>{children}</>,
    motion,
  };
});

vi.mock("./components/JobRow", () => ({
  JobRow: ({ job, onSelect }: any) => (
    <button type="button" onClick={onSelect}>
      {job.title}
    </button>
  ),
}));

const buildJob = (overrides: Record<string, any>) => ({
  _id: "job-1",
  title: "Test Role",
  company: "Example Co",
  location: "Remote",
  postedAt: Date.now(),
  scrapedAt: Date.now(),
  ...overrides,
});

const mockClipboard = () => {
  const writeText = vi.fn().mockResolvedValue(undefined);
  const clipboard = { writeText };
  Object.defineProperty(window.navigator, "clipboard", {
    value: clipboard,
    configurable: true,
  });
  Object.defineProperty(globalThis, "navigator", {
    value: window.navigator,
    configurable: true,
  });
  return writeText;
};

const expectShareLink = (writeText: ReturnType<typeof vi.fn>, jobId: string) => {
  const called = writeText.mock.calls[0]?.[0];
  expect(typeof called).toBe("string");
  const url = new URL(called);
  expect(url.origin).toBe(new URL(PROD_CONVEX_HTTP_URL).origin);
  expect(url.pathname).toBe("/share/job");
  expect(url.searchParams.get("id")).toBe(jobId);
  expect(url.searchParams.get("app")).toBe(window.location.origin);
};

afterEach(() => {
  cleanup();
  resetFixtures();
  window.location.hash = "";
  vi.clearAllMocks();
});

beforeEach(() => {
  const meta = import.meta as any;
  if (!meta.env || typeof meta.env !== "object") {
    meta.env = {};
  }
  Object.assign(meta.env, {
    VITE_CONVEX_HTTP_URL: PROD_CONVEX_HTTP_URL,
    VITE_CONVEX_URL: PROD_CONVEX_URL,
    MODE: "production",
    DEV: false,
    PROD: true,
  });
  window.location.hash = "#jobs";
});

describe("JobBoard copy link button", () => {
  it("copies the job details link from the jobs tab", async () => {
    const job = buildJob({ _id: "job-live" });
    paginatedResults = [job];
    jobDetailsById = {
      [job._id]: { description: "Job description" },
    };

    const writeText = mockClipboard();
    render(<JobBoard />);

    fireEvent.click(await screen.findByRole("button", { name: job.title }));
    const copyButton = await screen.findByRole("button", { name: /copy job link/i });
    fireEvent.click(copyButton);

    await waitFor(() => {
      expectShareLink(writeText, job._id);
    });
    expect(toast.success).toHaveBeenCalledWith("Job link copied");
  });

  it("copies the job details link from the applied tab", async () => {
    window.location.hash = "#applied";
    const job = buildJob({ _id: "job-applied", userStatus: "applied", appliedAt: Date.now() });
    appliedJobsFixture = [job];
    jobDetailsById = {
      [job._id]: { description: "Applied description" },
    };

    const writeText = mockClipboard();
    render(<JobBoard />);

    fireEvent.click(await screen.findByRole("button", { name: job.title }));
    const copyButton = await screen.findByRole("button", { name: /copy job link/i });
    fireEvent.click(copyButton);

    await waitFor(() => {
      expectShareLink(writeText, job._id);
    });
    expect(toast.success).toHaveBeenCalledWith("Job link copied");
  });

  it("copies the job details link from the rejected tab", async () => {
    window.location.hash = "#rejected";
    const job = buildJob({
      _id: "job-rejected",
      userStatus: "rejected",
      rejectedAt: Date.now(),
      description: "Rejected description",
    });
    rejectedJobsFixture = [job];

    const writeText = mockClipboard();
    render(<JobBoard />);

    fireEvent.click(await screen.findByRole("button", { name: job.title }));
    const copyButton = await screen.findByRole("button", { name: /copy job link/i });
    fireEvent.click(copyButton);

    await waitFor(() => {
      expectShareLink(writeText, job._id);
    });
    expect(toast.success).toHaveBeenCalledWith("Job link copied");
  });
});
