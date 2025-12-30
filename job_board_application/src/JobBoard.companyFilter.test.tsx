// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as convexReact from "convex/react";
import { getFunctionName } from "convex/server";
import { api } from "../convex/_generated/api";
import { JobBoard } from "./JobBoard";

const fixturePath = resolve(process.cwd(), "src/test/fixtures/convex_coupang_jobs.json");
const fixture = JSON.parse(readFileSync(fixturePath, "utf-8")) as {
  jobs: Array<{
    id: string;
    title?: string;
    company?: string;
    location?: string;
    locations?: string[];
    url?: string;
    postedAt?: number;
    remote?: boolean;
    level?: string;
  }>;
};

const fixtureJobs = fixture.jobs.map((job) => ({
  _id: job.id,
  title: job.title ?? "Unknown",
  company: job.company ?? "Unknown",
  location: job.location ?? "Unknown",
  locations: job.locations ?? [],
  url: job.url ?? "https://example.com",
  groupedJobIds: [job.id],
  alternateUrls: job.url ? [job.url] : [],
  postedAt: job.postedAt ?? Date.now(),
  remote: job.remote ?? false,
  level: job.level ?? null,
  applicationCount: 0,
}));

const emptyPaginatedResponse = {
  results: [] as any[],
  status: "Complete",
  loadMore: vi.fn(),
};

const getQueryName = (queryFn: any) => {
  try {
    return getFunctionName(queryFn);
  } catch {
    return null;
  }
};

const matchesQuery = (queryFn: any, target: any) => getQueryName(queryFn) === getQueryName(target);

vi.mock("convex/react", () => {
  const usePaginatedQuery = vi.fn((queryFn: any, args?: any) => {
    if (args === "skip") return emptyPaginatedResponse;

    const companies = Array.isArray(args?.companies)
      ? (args.companies as string[]).map((name) => name.toLowerCase())
      : [];
    const results = companies.length === 0
      ? []
      : fixtureJobs.filter((job) => companies.includes(job.company.toLowerCase()));

    return {
      results,
      status: "Complete",
      loadMore: vi.fn(),
    };
  });

  const useQuery = vi.fn((queryFn: any, args?: any) => {
    if (args === "skip") return undefined;
    if (matchesQuery(queryFn, api.auth.isAdmin)) return false;
    if (matchesQuery(queryFn, api.filters.getSavedFilters)) return [];
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
  JobRow: () => <div data-testid="job-row" />,
}));

vi.mock("./components/AppliedJobRow", () => ({
  AppliedJobRow: () => <div data-testid="applied-job-row" />,
}));

vi.mock("./components/RejectedJobRow", () => ({
  RejectedJobRow: () => <div data-testid="rejected-job-row" />,
}));

afterEach(() => {
  cleanup();
  (convexReact as any).useQuery.mockClear();
  (convexReact as any).usePaginatedQuery.mockClear();
  (convexReact as any).useMutation.mockClear();
  window.history.pushState({}, "", "/");
});

describe("JobBoard company filter via URL", () => {
  it("requests and renders matching jobs when the company query param is set", async () => {
    window.history.pushState({}, "", "?company=Coupang");

    render(<JobBoard />);

    const rows = await screen.findAllByTestId("job-row");
    expect(rows).toHaveLength(fixtureJobs.length);

    await waitFor(() => {
      const lastArgs = (convexReact as any).usePaginatedQuery.mock.calls.at(-1)?.[1];
      expect(lastArgs?.companies).toEqual(["Coupang"]);
    });
  });
});
