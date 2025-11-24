// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, vi, it } from "vitest";
import * as convexReact from "convex/react";
import { api } from "../convex/_generated/api";
import { JobBoard } from "./JobBoard";

const listJobsRef = api.jobs.listJobs;
function defaultUsePaginatedQueryImpl(queryFn: any, _args: any, _opts: any) {
  if (queryFn === listJobsRef) {
    return { results: [], status: "Complete", loadMore: vi.fn() };
  }
  return { results: [], status: "Complete", loadMore: vi.fn() };
}

vi.mock("convex/react", () => {
  let savedFiltersFixture: any[] = [];

  const usePaginatedQuery = vi.fn(defaultUsePaginatedQueryImpl);

  let queryCallCount = 0;
  const useQuery = vi.fn(() => {
    const callIndex = queryCallCount % 3;
    queryCallCount += 1;
    if (callIndex === 0) return savedFiltersFixture;
    return [];
  });

  const useMutation = vi.fn(() => vi.fn(async () => ({})));

  return {
    usePaginatedQuery,
    useQuery,
    useMutation,
    __setSavedFilters: (filters: any[]) => {
      savedFiltersFixture = filters;
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

const setSavedFilters = (filters: any[]) => {
  (convexReact as any).__setSavedFilters(filters);
};

afterEach(() => {
  cleanup();
  (convexReact as any).usePaginatedQuery.mockImplementation(defaultUsePaginatedQueryImpl);
});

describe("JobBoard min salary input", () => {
  beforeEach(() => {
    setSavedFilters([
      {
        _id: "f1",
        name: "Saved",
        search: "",
        includeRemote: true,
        state: null,
        level: null,
        minCompensation: 10000,
        maxCompensation: null,
        isSelected: true,
      },
    ]);
  });

  it("lets the user expand 10k to 100k with typing", async () => {
    const user = userEvent.setup();
    render(<JobBoard />);

    const input = await screen.findByPlaceholderText("$50k");
    await user.click(input);
    await user.keyboard("{backspace}{backspace}00k");
    await user.tab();

    expect(input).toHaveValue("$100k");
    const lastArgs = (convexReact as any).usePaginatedQuery.mock.calls.at(-1)?.[1];
    expect(lastArgs?.minCompensation).toBe(100000);
  });

  it("replaces the value when clearing and typing a new amount", async () => {
    const user = userEvent.setup();
    setSavedFilters([
      {
        _id: "f1",
        name: "Saved",
        search: "",
        includeRemote: true,
        state: null,
        level: null,
        minCompensation: 100000,
        maxCompensation: null,
        isSelected: true,
      },
    ]);

    render(<JobBoard />);

    const input = await screen.findByPlaceholderText("$50k");
    await user.click(input);
    await user.keyboard("{Control>}{A}{/Control}{Delete}30k");
    await user.tab();

    expect(input).toHaveValue("$30k");
    const lastArgs = (convexReact as any).usePaginatedQuery.mock.calls.at(-1)?.[1];
    expect(lastArgs?.minCompensation).toBe(30000);
  });

  it("lets the user type k in the min salary even with list navigation shortcuts", async () => {
    const user = userEvent.setup();
    const jobResults = [{ _id: "j1" }];
    (convexReact as any).usePaginatedQuery.mockImplementation(() => ({
      results: jobResults,
      status: "Complete",
      loadMore: vi.fn(),
    }));
    setSavedFilters([
      {
        _id: "f1",
        name: "Saved",
        search: "",
        includeRemote: true,
        state: null,
        level: null,
        minCompensation: 100000,
        maxCompensation: null,
        isSelected: true,
      },
    ]);

    render(<JobBoard />);

    const input = await screen.findByPlaceholderText("$50k");
    expect(input).toHaveValue("$100k");

    await screen.findByTestId("job-row");
    await user.dblClick(input);
    await user.keyboard("{Delete}10k");
    await user.tab();

    expect(input).toHaveValue("$10k");
    const lastArgs = (convexReact as any).usePaginatedQuery.mock.calls.at(-1)?.[1];
    expect(lastArgs?.minCompensation).toBe(10000);
  });

  it("updates min salary when pressing Tab after editing", async () => {
    const user = userEvent.setup();
    setSavedFilters([
      {
        _id: "f1",
        name: "Saved",
        search: "",
        includeRemote: true,
        state: null,
        level: null,
        minCompensation: 120000,
        maxCompensation: null,
        isSelected: true,
      },
    ]);

    render(<JobBoard />);

    const input = await screen.findByPlaceholderText("$50k");
    expect(input).toHaveValue("$120k");

    await user.click(input);
    await user.keyboard("{Control>}{A}{/Control}60k");
    await user.tab();

    expect(input).toHaveValue("$60k");
    const lastArgs = (convexReact as any).usePaginatedQuery.mock.calls.at(-1)?.[1];
    expect(lastArgs?.minCompensation).toBe(60000);
  });

  it("updates min salary when pressing Enter after editing", async () => {
    const user = userEvent.setup();
    setSavedFilters([
      {
        _id: "f1",
        name: "Saved",
        search: "",
        includeRemote: true,
        state: null,
        level: null,
        minCompensation: 150000,
        maxCompensation: null,
        isSelected: true,
      },
    ]);

    render(<JobBoard />);

    const input = await screen.findByPlaceholderText("$50k");
    expect(input).toHaveValue("$150k");

    await user.click(input);
    await user.keyboard("{Control>}{A}{/Control}80k{Enter}");

    expect(input).toHaveValue("$80k");
    const lastArgs = (convexReact as any).usePaginatedQuery.mock.calls.at(-1)?.[1];
    expect(lastArgs?.minCompensation).toBe(80000);
  });
});

describe("JobBoard dropdown styling", () => {
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
        isSelected: false,
      },
    ]);
  });

  it("keeps the location and level dropdowns on a dark surface when opened", async () => {
    render(<JobBoard />);

    const locationSelect = await screen.findByLabelText("Location");
    const levelSelect = await screen.findByLabelText("Level");

    const expectStyleContains = (style: string, fragment: string) => {
      expect(style.replace(/\s+/g, " ")).toContain(fragment);
    };

    const expectDarkSelect = (selectEl: HTMLSelectElement) => {
      const styleAttr = selectEl.getAttribute("style") ?? "";
      expectStyleContains(styleAttr, "color-scheme: dark");
      expectStyleContains(styleAttr, "background-color: rgb(15, 23, 42)");

      const options = within(selectEl).getAllByRole("option");
      options.forEach((option) => {
        const optionStyle = option.getAttribute("style") ?? "";
        expectStyleContains(optionStyle, "background-color: rgb(15, 23, 42)");
        expectStyleContains(optionStyle, "color: rgb(226, 232, 240)");
      });
    };

    expectDarkSelect(locationSelect as HTMLSelectElement);
    expectDarkSelect(levelSelect as HTMLSelectElement);
  });
});
