// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

const fixturePath = resolve(
  process.cwd(),
  "..",
  "tests",
  "job_scrape_application",
  "fixtures",
  "convex_voltage-park_jobs.json",
);
const fixture = JSON.parse(readFileSync(fixturePath, "utf-8")) as {
  jobs: Array<{
    id: string;
    title?: string;
    company?: string;
    location?: string;
    url?: string;
    postedAt?: number;
    remote?: boolean;
    level?: string;
    totalCompensation?: number;
    compensationUnknown?: boolean;
    currencyCode?: string;
  }>;
};

const companyIconSpy = vi.fn();

vi.mock("../components/CompanyIcon", () => ({
  CompanyIcon: (props: any) => {
    companyIconSpy(props);
    return (
      <div
        data-testid="company-icon"
        data-company={props.company}
        data-url={props.url}
      />
    );
  },
}));

vi.mock("framer-motion", () => {
  const passthrough = ({ children, layout, initial, animate, exit, ...rest }: any) => (
    <div {...rest}>{children}</div>
  );
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

import { JobRow } from "../components/JobRow";

afterEach(() => {
  companyIconSpy.mockClear();
});

describe("JobRow logo props from prod fixture", () => {
  it("passes the fixture job url to CompanyIcon", () => {
    const fixtureJob = fixture.jobs[0];
    if (!fixtureJob) {
      throw new Error("Expected Voltage Park fixture to include at least one job.");
    }

    const job = {
      _id: fixtureJob.id,
      title: fixtureJob.title ?? "Unknown",
      company: fixtureJob.company ?? "Unknown",
      location: fixtureJob.location ?? "Unknown",
      url: fixtureJob.url ?? "https://example.com",
      postedAt: fixtureJob.postedAt ?? Date.now(),
      remote: fixtureJob.remote ?? false,
      level: fixtureJob.level ?? null,
      totalCompensation: fixtureJob.totalCompensation ?? 0,
      compensationUnknown: fixtureJob.compensationUnknown ?? true,
      currencyCode: fixtureJob.currencyCode ?? "USD",
    };

    render(<JobRow job={job} isSelected={false} onSelect={() => {}} />);

    const icon = screen.getByTestId("company-icon");
    expect(icon).toHaveAttribute("data-company", job.company);
    expect(icon).toHaveAttribute("data-url", job.url);
    expect(companyIconSpy).toHaveBeenCalledWith(expect.objectContaining({ url: job.url, company: job.company }));
  });
});
