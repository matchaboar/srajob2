import React from "react";
import { describe, it, expect, vi, beforeAll, afterAll } from "vitest";
import { JobRow } from "./JobRow";
import { render } from "@testing-library/react";

beforeAll(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2025-11-24T15:00:00Z"));
});

afterAll(() => {
  vi.useRealTimers();
});

const job = {
  _id: "job-1",
  title: "Senior Engineer",
  company: "Example Co",
  location: "Remote",
  remote: true,
  level: "senior",
  totalCompensation: 200000,
  postedAt: Date.now() - 3600_000,
  scrapedAt: Date.now() - 60_000,
  scrapedWith: "firecrawl",
  scrapedCostMilliCents: 10,
};

describe("JobRow scrape cost screenshot", () => {
  it("renders fraction styling", async () => {
    const { baseElement } = await render(
      <JobRow job={job} isSelected onSelect={() => {}} onApply={() => {}} onReject={() => {}} />,
    );
    expect(baseElement.outerHTML).toMatchSnapshot();
  }, 60000);
});
