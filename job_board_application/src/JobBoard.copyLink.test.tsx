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

afterEach(() => {
  cleanup();
  resetFixtures();
  window.location.hash = "";
  vi.clearAllMocks();
});

beforeEach(() => {
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

    const expected = new URL(window.location.href);
    expected.hash = `job-details-${job._id}`;

    await waitFor(() => {
      expect(writeText).toHaveBeenCalledWith(expected.toString());
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

    const expected = new URL(window.location.href);
    expected.hash = `job-details-${job._id}`;

    await waitFor(() => {
      expect(writeText).toHaveBeenCalledWith(expected.toString());
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

    const expected = new URL(window.location.href);
    expected.hash = `job-details-${job._id}`;

    await waitFor(() => {
      expect(writeText).toHaveBeenCalledWith(expected.toString());
    });
    expect(toast.success).toHaveBeenCalledWith("Job link copied");
  });
});
