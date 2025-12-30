// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, render } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as convexReact from "convex/react";
import { getFunctionName } from "convex/server";
import { api } from "../convex/_generated/api";
import { JobBoard } from "./JobBoard";

const defaultPaginatedResponse = {
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
  const usePaginatedQuery = vi.fn(() => defaultPaginatedResponse);

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

const findQueryCall = (queryFn: any) => {
  const targetName = getQueryName(queryFn);
  return (convexReact as any).useQuery.mock.calls.find((call: any[]) => getQueryName(call[0]) === targetName);
};

const findQueryCalls = (queryFn: any) => {
  const targetName = getQueryName(queryFn);
  return (convexReact as any).useQuery.mock.calls.filter((call: any[]) => getQueryName(call[0]) === targetName);
};

const captureTimeouts = () => {
  const timeouts: Array<{ cb: () => void; delay: number }> = [];
  const timeoutSpy = vi.spyOn(window, "setTimeout").mockImplementation((cb, delay) => {
    if (typeof cb === "function") {
      timeouts.push({ cb, delay: typeof delay === "number" ? delay : 0 });
    }
    return 0 as unknown as number;
  });
  return {
    timeouts,
    restore: () => timeoutSpy.mockRestore(),
  };
};

const findAutoLoadTimeout = (timeouts: Array<{ cb: () => void; delay: number }>) =>
  timeouts.findLast((timeout) => timeout.delay === 1000);

afterEach(() => {
  cleanup();
  (convexReact as any).useQuery.mockClear();
  (convexReact as any).usePaginatedQuery.mockReset();
  (convexReact as any).usePaginatedQuery.mockImplementation(() => defaultPaginatedResponse);
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

describe("JobBoard auto-load behavior", () => {
  const buildJobs = (count: number) =>
    Array.from({ length: count }, (_, index) => ({
      _id: `job-${index + 1}`,
      title: "Engineer",
      company: "Acme",
      location: "Remote",
      url: "https://example.com",
      groupedJobIds: [`job-${index + 1}`],
      alternateUrls: ["https://example.com"],
      postedAt: Date.now(),
      remote: true,
      level: "mid",
      applicationCount: 0,
    }));

  it("auto-loads another chunk when the jobs list is short and more data is available", () => {
    const loadMore = vi.fn();
    const jobs = buildJobs(10);
    (convexReact as any).usePaginatedQuery.mockImplementation(() => ({
      results: jobs,
      status: "CanLoadMore",
      loadMore,
    }));
    const { timeouts, restore } = captureTimeouts();

    render(<JobBoard />);

    expect(loadMore).not.toHaveBeenCalled();
    const autoLoad = findAutoLoadTimeout(timeouts);
    expect(autoLoad).toBeDefined();
    act(() => {
      autoLoad?.cb();
    });
    expect(loadMore).toHaveBeenCalledWith(10);
    restore();
  });

  it("does not auto-load when the list is complete", () => {
    const loadMore = vi.fn();
    const jobs = buildJobs(10);
    (convexReact as any).usePaginatedQuery.mockImplementation(() => ({
      results: jobs,
      status: "Complete",
      loadMore,
    }));
    const { timeouts, restore } = captureTimeouts();

    render(<JobBoard />);

    expect(loadMore).not.toHaveBeenCalled();
    const autoLoad = findAutoLoadTimeout(timeouts);
    expect(autoLoad).toBeUndefined();
    restore();
  });

  it("does not auto-load on non-jobs tabs", () => {
    const loadMore = vi.fn();
    const jobs = buildJobs(10);
    (convexReact as any).usePaginatedQuery.mockImplementation(() => ({
      results: jobs,
      status: "CanLoadMore",
      loadMore,
    }));
    const { timeouts, restore } = captureTimeouts();

    window.location.hash = "#live";
    render(<JobBoard />);

    expect(loadMore).not.toHaveBeenCalled();
    const autoLoad = findAutoLoadTimeout(timeouts);
    expect(autoLoad).toBeUndefined();
    restore();
  });

  it("tops up when new jobs arrive after reaching the auto-fill target", () => {
    const loadMore = vi.fn();
    const initialJobs = buildJobs(30);
    let response = {
      results: initialJobs,
      status: "CanLoadMore",
      loadMore,
    };
    (convexReact as any).usePaginatedQuery.mockImplementation(() => response);
    const { timeouts, restore } = captureTimeouts();

    const { rerender } = render(<JobBoard />);

    response = {
      results: [
        {
          _id: "job-new",
          title: "Engineer",
          company: "Acme",
          location: "Remote",
          url: "https://example.com/new",
          groupedJobIds: ["job-new"],
          alternateUrls: ["https://example.com/new"],
          postedAt: Date.now(),
          remote: true,
          level: "mid",
          applicationCount: 0,
        },
        ...buildJobs(29),
      ],
      status: "CanLoadMore",
      loadMore,
    };

    rerender(<JobBoard />);

    const autoLoad = findAutoLoadTimeout(timeouts);
    expect(autoLoad).toBeDefined();
    act(() => {
      autoLoad?.cb();
    });
    expect(loadMore).toHaveBeenCalledWith(10);
    restore();
  });
});
