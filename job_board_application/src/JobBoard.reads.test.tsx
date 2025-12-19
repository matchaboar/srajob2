// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as convexReact from "convex/react";
import { api } from "../convex/_generated/api";
import { JobBoard } from "./JobBoard";

vi.mock("convex/react", () => {
  const usePaginatedQuery = vi.fn(() => ({
    results: [],
    status: "Complete",
    loadMore: vi.fn(),
  }));

  const useQuery = vi.fn((queryFn: any, args?: any) => {
    if (args === "skip") return undefined;
    if (queryFn === api.filters.getSavedFilters) return [];
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

const findQueryCall = (queryFn: any) =>
  (convexReact as any).useQuery.mock.calls.find((call: any[]) => call[0] === queryFn);

const findQueryCalls = (queryFn: any) =>
  (convexReact as any).useQuery.mock.calls.filter((call: any[]) => call[0] === queryFn);

afterEach(() => {
  cleanup();
  (convexReact as any).useQuery.mockClear();
  (convexReact as any).usePaginatedQuery.mockClear();
  (convexReact as any).useMutation.mockClear();
  window.location.hash = "";
});

beforeEach(() => {
  window.location.hash = "";
});

describe("JobBoard query skipping", () => {
  it("skips non-active tab queries on the jobs tab", () => {
    render(<JobBoard />);

    expect(findQueryCall(api.router.listIgnoredJobs)?.[1]).toBe("skip");
    expect(findQueryCall(api.jobs.getAppliedJobs)?.[1]).toBe("skip");
    expect(findQueryCall(api.jobs.getRejectedJobs)?.[1]).toBe("skip");
    expect(findQueryCall(api.jobs.searchCompanies)?.[1]).toBe("skip");
    expect(findQueryCall(api.jobs.getRecentJobs)?.[1]).toEqual({});
    expect(findQueryCall(api.filters.getSavedFilters)?.[1]).toEqual({});

    const detailsCalls = findQueryCalls(api.jobs.getJobDetails);
    expect(detailsCalls.length).toBeGreaterThan(0);
    expect(detailsCalls.every((call: any[]) => call[1] === "skip")).toBe(true);
  });

  it("fetches only applied jobs on the applied tab", () => {
    window.location.hash = "#applied";
    render(<JobBoard />);

    expect(findQueryCall(api.jobs.getAppliedJobs)?.[1]).toEqual({});
    expect(findQueryCall(api.jobs.getRejectedJobs)?.[1]).toBe("skip");
    expect(findQueryCall(api.router.listIgnoredJobs)?.[1]).toBe("skip");
    expect(findQueryCall(api.jobs.getRecentJobs)?.[1]).toBe("skip");
    expect(findQueryCall(api.filters.getSavedFilters)?.[1]).toBe("skip");

    const paginatedArgs = (convexReact as any).usePaginatedQuery.mock.calls[0]?.[1];
    expect(paginatedArgs).toBe("skip");
  });

  it("fetches recent jobs on the live tab and skips job list", () => {
    window.location.hash = "#live";
    render(<JobBoard />);

    expect(findQueryCall(api.jobs.getRecentJobs)?.[1]).toEqual({});
    expect(findQueryCall(api.jobs.getAppliedJobs)?.[1]).toBe("skip");
    expect(findQueryCall(api.jobs.getRejectedJobs)?.[1]).toBe("skip");
    expect(findQueryCall(api.router.listIgnoredJobs)?.[1]).toBe("skip");

    const paginatedArgs = (convexReact as any).usePaginatedQuery.mock.calls[0]?.[1];
    expect(paginatedArgs).toBe("skip");
  });

  it("fetches ignored jobs only on the ignored tab", () => {
    window.location.hash = "#ignored";
    render(<JobBoard />);

    expect(findQueryCall(api.router.listIgnoredJobs)?.[1]).toEqual({ limit: 200 });
    expect(findQueryCall(api.jobs.getAppliedJobs)?.[1]).toBe("skip");
    expect(findQueryCall(api.jobs.getRejectedJobs)?.[1]).toBe("skip");
    expect(findQueryCall(api.jobs.getRecentJobs)?.[1]).toBe("skip");

    const paginatedArgs = (convexReact as any).usePaginatedQuery.mock.calls[0]?.[1];
    expect(paginatedArgs).toBe("skip");
  });
});
