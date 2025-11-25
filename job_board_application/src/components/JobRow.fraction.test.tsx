// @vitest-environment jsdom

import React from "react";
import { describe, it, expect, vi, afterEach, beforeAll, afterAll } from "vitest";
import { render, cleanup } from "@testing-library/react";
import { JobRow } from "./JobRow";

// Simplify animation/rendering for tests
vi.mock("framer-motion", () => ({
  motion: {
    div: ({ children, className }: any) => <div className={className}>{children}</div>,
  },
}));

vi.mock("./LiveTimer", () => ({
  LiveTimer: () => <span data-testid="live-timer">timer</span>,
}));

beforeAll(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2025-11-24T15:00:00Z"));
});

afterAll(() => {
  vi.useRealTimers();
});

const baseJob = {
  _id: "job-1",
  title: "Senior Engineer",
  company: "Example Co",
  location: "Remote",
  remote: true,
  level: "senior",
  totalCompensation: 200000,
  postedAt: Date.now() - 1000 * 60 * 60,
  scrapedAt: Date.now() - 1000 * 60 * 5,
  scrapedWith: "firecrawl",
};

const renderRow = (scrapedCostMilliCents: number | null) =>
  render(
    <JobRow
      job={{ ...baseJob, scrapedCostMilliCents }}
      isSelected={false}
      onSelect={() => {}}
      onApply={() => {}}
      onReject={() => {}}
    />,
  );

describe("JobRow scrape cost fraction rendering", () => {
  afterEach(() => cleanup());

  it("renders 1/10 cent fraction styling", () => {
    const { container, getByText } = renderRow(100);
    expect(getByText("cost", { exact: false })).toBeTruthy();
    expect(container).toMatchSnapshot();
  });

  it("renders 1/100 cent fraction styling", () => {
    const { container } = renderRow(10);
    expect(container).toMatchSnapshot();
  });

  it("renders 1/1000 cent fraction styling", () => {
    const { container } = renderRow(1);
    expect(container).toMatchSnapshot();
  });

  it("renders whole-cent amounts normally", () => {
    const { container, getByText } = renderRow(15000);
    expect(getByText(/15\.00 ¢/i)).toBeTruthy();
    expect(container).toMatchSnapshot();
  });

  it("renders zero cost as 0 ¢", () => {
    const { container, getAllByText } = renderRow(0);
    expect(getAllByText(/0 ¢/i).length).toBeGreaterThan(0);
    expect(container).toMatchSnapshot();
  });
});
