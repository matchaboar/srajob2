// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as convexReact from "convex/react";
import { api } from "../convex/_generated/api";
import { JobBoard } from "./JobBoard";

vi.mock("convex/react", () => {
  let savedFiltersFixture: any[] = [];
  let paginatedResults: any[] = [];
  const paginatedResponse = {
    results: paginatedResults,
    status: "Complete",
    loadMore: vi.fn(),
  };
  const emptyResults: any[] = [];

  const usePaginatedQuery = vi.fn(() => paginatedResponse);

  const useQuery = vi.fn((queryFn: any, args?: any) => {
    if (args === "skip") return undefined;
    if (queryFn === api.filters.getSavedFilters) return savedFiltersFixture;
    return emptyResults;
  });

  const useMutation = vi.fn(() => vi.fn(async () => ({})));

  return {
    usePaginatedQuery,
    useQuery,
    useMutation,
    __setSavedFilters: (filters: any[]) => {
      savedFiltersFixture = filters;
    },
    __setPaginatedResults: (results: any[]) => {
      paginatedResults = results;
      paginatedResponse.results = results;
    },
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

const setSavedFilters = (filters: any[]) => {
  (convexReact as any).__setSavedFilters(filters);
};

afterEach(() => {
  cleanup();
  (convexReact as any).useQuery.mockClear();
  (convexReact as any).usePaginatedQuery.mockClear();
  (convexReact as any).useMutation.mockClear();
  window.history.pushState({}, "", "/");
});

beforeEach(() => {
  setSavedFilters([
    {
      _id: "f1",
      name: "Default",
      search: "",
      includeRemote: true,
      state: null,
      level: null,
      minCompensation: null,
      maxCompensation: null,
      isSelected: true,
    },
  ]);
});

describe("JobBoard filters panel", () => {
  it("closes filters when clicking outside the panel", async () => {
    const user = userEvent.setup();
    render(<JobBoard />);

    const toggle = screen.getByRole("button", { name: /toggle filters/i });
    await user.click(toggle);

    expect(toggle).toHaveAttribute("aria-expanded", "true");
    const overlay = screen.getByTestId("filters-overlay");
    await user.click(overlay);

    expect(toggle).toHaveAttribute("aria-expanded", "false");
  });

  it("keeps the close button visible via sticky header", async () => {
    const user = userEvent.setup();
    render(<JobBoard />);

    const toggle = screen.getByRole("button", { name: /toggle filters/i });
    await user.click(toggle);

    const header = screen.getByTestId("filters-header");
    expect(header.className).toContain("sticky");
    const closeButton = screen.getByTestId("filters-close");
    expect(closeButton).toBeVisible();
  });

  it("applies the engineer filter on the main job list", async () => {
    const user = userEvent.setup();
    render(<JobBoard />);

    const toggle = screen.getByRole("button", { name: /toggle filters/i });
    await user.click(toggle);

    const engineerCheckbox = await screen.findByLabelText(/engineer titles only/i);
    await user.click(engineerCheckbox);

    const lastArgs = (convexReact as any).usePaginatedQuery.mock.calls.at(-1)?.[1];
    expect(lastArgs?.engineer).toBe(true);
  });

  it("keeps the engineer filter when a company is selected via URL", async () => {
    const user = userEvent.setup();
    window.history.pushState({}, "", "?company=Acme");
    render(<JobBoard />);

    const toggle = screen.getByRole("button", { name: /toggle filters/i });
    await user.click(toggle);

    const engineerCheckbox = await screen.findByLabelText(/engineer titles only/i);
    await user.click(engineerCheckbox);

    const lastArgs = (convexReact as any).usePaginatedQuery.mock.calls.at(-1)?.[1];
    expect(lastArgs?.engineer).toBe(true);
    expect(lastArgs?.companies).toEqual(["Acme"]);
  });
});
