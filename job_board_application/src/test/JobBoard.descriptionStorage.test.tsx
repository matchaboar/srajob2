// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { getFunctionName } from "convex/server";
import { api } from "../../convex/_generated/api";
import { JobBoard } from "../pages/JobBoard";

let paginatedResults: any[] = [];
let jobDetailsById: Record<string, any> = {};
let descriptionUrlByJobId: Record<string, string | null> = {};

const resetFixtures = () => {
  paginatedResults = [];
  jobDetailsById = {};
  descriptionUrlByJobId = {};
};

const queryNames = {
  jobDetails: getFunctionName(api.jobs.getJobDetails),
  jobDescriptionUrl: getFunctionName(api.jobs.getJobDescriptionUrl),
  savedFilters: getFunctionName(api.filters.getSavedFilters),
  isAdmin: getFunctionName(api.auth.isAdmin),
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
    if (queryName === queryNames.isAdmin) return false;
    if (queryName === queryNames.savedFilters) return [];
    if (queryName === queryNames.jobDetails) {
      const jobId = args?.jobId as string | undefined;
      return (jobId && jobDetailsById[jobId]) ?? {};
    }
    if (queryName === queryNames.jobDescriptionUrl) {
      const jobId = args?.jobId as string | undefined;
      return jobId ? descriptionUrlByJobId[jobId] ?? null : null;
    }
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

vi.mock("../components/JobRow", () => ({
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

beforeEach(() => {
  window.location.hash = "#jobs";
});

afterEach(() => {
  cleanup();
  resetFixtures();
  vi.restoreAllMocks();
  window.location.hash = "";
});

describe("JobBoard description storage", () => {
  it("loads the full description from storage when read more is clicked", async () => {
    const job = buildJob({ _id: "job-full" });
    paginatedResults = [job];
    jobDetailsById = {
      [job._id]: { description: "Short description", descriptionStorageAvailable: true },
    };
    descriptionUrlByJobId = {
      [job._id]: "https://example.com/desc.txt",
    };
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      text: async () => "Full description from storage.",
    });
    (globalThis as any).fetch = fetchMock;

    render(<JobBoard />);

    fireEvent.click(await screen.findByRole("button", { name: job.title }));
    const readMore = await screen.findByRole("button", { name: /read more/i });
    fireEvent.click(readMore);

    expect(await screen.findByText("Full description from storage.")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith("https://example.com/desc.txt");
  });

  it("shows an error when the storage url is missing", async () => {
    const job = buildJob({ _id: "job-missing" });
    paginatedResults = [job];
    jobDetailsById = {
      [job._id]: { description: "Short description", descriptionStorageAvailable: true },
    };
    descriptionUrlByJobId = {
      [job._id]: null,
    };
    const fetchMock = vi.fn();
    (globalThis as any).fetch = fetchMock;

    render(<JobBoard />);

    fireEvent.click(await screen.findByRole("button", { name: job.title }));
    const readMore = await screen.findByRole("button", { name: /read more/i });
    fireEvent.click(readMore);

    expect(await screen.findByText("Full description unavailable.")).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
