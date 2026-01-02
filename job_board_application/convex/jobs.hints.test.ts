import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { buildUpdatesFromHints, deriveCompanyFromUrl, parseMarkdownHints } from "./jobs";

const OFFSEC_MARKDOWN = `
Job Application for Senior Offensive Security Engineer at Robinhood
# Senior Offensive Security Engineer
Menlo Park, CA
Base Pay Range:
Zone 1 (Menlo Park, CA; New York, NY; Bellevue, WA; Washington, DC)
$187,000-$220,000USD
`;

const RELIABILITY_MARKDOWN = readFileSync(
  path.resolve(process.cwd(), "convex/__fixtures__/robinhood_staff_reliability_full.md"),
  "utf8"
);

const CITY_IN_BODY_MARKDOWN = `
Job Application for Data Engineer at DemoCo
# Data Engineer
This team works closely with partners across the business.
You'll collaborate from our Seattle office with periodic travel to Austin.
`;

const BELLEVUE_ADDRESS_MARKDOWN = `
Job Application for Software Engineer II at The Trade Desk
# Software Engineer II
Location: 10770 NE 8th Street, Suite 700, Bellevue, WA 98004
*Telecommuting permitted: work may be performed within normal commuting distance from The Trade Desk, Inc. office in Bellevue, WA
CO, CA, IL, NY, WA, and Washington DC residents only: In accordance with CO, CA, IL, NY, WA, and Washington DC law.
`;

describe("markdown hint parsing", () => {
  it("parses title/location/level/compensation from markdown", () => {
    const hints = parseMarkdownHints(OFFSEC_MARKDOWN);
    expect(hints.title).toBe("Senior Offensive Security Engineer");
    expect(hints.location).toBe("Menlo Park, California");
    expect(hints.level).toBe("senior");
    expect(hints.compensation).toBeGreaterThanOrEqual(187000);
  });

  it("builds updates to strip job application prefix and fill fields", () => {
    const job = {
      _id: "job1",
      title: "Job Application for Senior Offensive Security Engineer at Robinhood",
      location: "Unknown",
      city: null,
      state: null,
      level: "staff",
      totalCompensation: 0,
      remote: false,
      description: OFFSEC_MARKDOWN,
    };

    const hints = parseMarkdownHints(job.description);
    const updates = buildUpdatesFromHints(job, hints);

    expect(updates.title).toBe("Senior Offensive Security Engineer");
    expect(updates.location).toBe("Menlo Park, California");
    expect(updates.city).toBe("Menlo Park");
    expect(updates.state).toBe("California");
    expect(updates.level).toBe("senior");
    expect(updates.totalCompensation).toBeGreaterThanOrEqual(187000);
    expect(updates.compensationReason).toBe("parsed from description");
  });

  it("parses robinhood reliability markdown with links and fills location when re-parsed", () => {
    const hints = parseMarkdownHints(RELIABILITY_MARKDOWN);

    expect(hints.location).toBe("Menlo Park, California");
    expect(hints.title).toBe("Staff Software Engineer, Reliability");

    const job = {
      _id: "job2",
      title: "Job Application for Staff Software Engineer, Reliability at Robinhood",
      location: "Unknown",
      city: null,
      state: null,
      level: "mid",
      totalCompensation: 0,
      remote: false,
      description: RELIABILITY_MARKDOWN,
    };

    const updates = buildUpdatesFromHints(job, hints);

    expect(updates.location).toBe("Menlo Park, California");
    expect(updates.city).toBe("Menlo Park");
    expect(updates.state).toBe("California");
    expect(updates.title).toBe("Staff Software Engineer, Reliability");
  });

  it("falls back to mapped common tech cities when no explicit location line", () => {
    const hints = parseMarkdownHints(CITY_IN_BODY_MARKDOWN);
    expect(hints.location).toBe("Seattle, Washington");
  });

  it("prefers explicit address city/state over later state mentions", () => {
    const hints = parseMarkdownHints(BELLEVUE_ADDRESS_MARKDOWN);
    expect(hints.location).toBe("Bellevue, Washington");
  });

  it("prefers United States location when mixed with international lines", () => {
    const markdown = `
Job Application for Engineer at DemoCo
# Engineer
Madrid, Spain
New York, NY
`;
    const hints = parseMarkdownHints(markdown);
    expect(hints.location).toBe("New York, New York");
    expect(hints.locations?.[0]).toBe("New York, New York");
    expect(hints.locations?.[1]).toBe("Madrid, Spain");
  });

  it("extracts remote country from remote-only lines", () => {
    const markdown = `
Job Application for Engineer at DemoCo
# Engineer
In this role you can work from Remote, United Kingdom
`;
    const job = {
      _id: "job4",
      title: "Engineer",
      location: "Remote",
      locations: ["Remote"],
      locationStates: ["Remote"],
      locationSearch: "Remote United States",
      countries: ["United States"],
      country: "United States",
      city: "Remote",
      state: "Remote",
      remote: true,
      totalCompensation: 0,
      description: markdown,
    };
    const hints = parseMarkdownHints(markdown);
    const updates = buildUpdatesFromHints(job, hints);

    expect(updates.location ?? job.location).toBe("Remote");
    expect(updates.countries).toEqual(["United Kingdom"]);
    expect(updates.country).toBe("United Kingdom");
    expect(updates.locationSearch).toBe("Remote United Kingdom");
  });

  it("overrides existing unknown city/state values when re-parsed", () => {
    const job = {
      _id: "job3",
      title: "Job Application for Senior Offensive Security Engineer at Robinhood",
      location: "Unknown",
      city: "Unknown",
      state: "Unknown",
      level: "staff",
      totalCompensation: 0,
      remote: false,
      description: OFFSEC_MARKDOWN,
    };

    const hints = parseMarkdownHints(OFFSEC_MARKDOWN);
    const updates = buildUpdatesFromHints(job, hints);

    expect(updates.city).toBe("Menlo Park");
    expect(updates.state).toBe("California");
    expect(updates.location).toBe("Menlo Park, California");
  });

  it("extracts compensation from multi-zone reliability posting and averages min/max", () => {
    const hints = parseMarkdownHints(RELIABILITY_MARKDOWN);

    expect(hints.compensation).toBe(212000);

    const job = {
      _id: "job3",
      title: "Job Application for Staff Software Engineer, Reliability at Robinhood",
      location: "Unknown",
      city: null,
      state: null,
      level: "mid",
      totalCompensation: 0,
      compensationUnknown: true,
      remote: false,
      description: RELIABILITY_MARKDOWN,
    };

    const updates = buildUpdatesFromHints(job, hints);

    expect(updates.totalCompensation).toBe(212000);
    expect(updates.compensationUnknown).toBe(false);
    expect(updates.compensationReason).toBe("parsed from description");
  });

  it("derives company from greenhouse and ashby URL slugs", () => {
    expect(deriveCompanyFromUrl("https://boards.greenhouse.io/robinhood/jobs/123")).toBe("Robinhood");
    expect(deriveCompanyFromUrl("https://boards-api.greenhouse.io/v1/boards/mithril/jobs/4419565007")).toBe("Mithril");
    expect(deriveCompanyFromUrl("https://api.greenhouse.io/v1/boards/mithril/jobs")).toBe("Mithril");
    expect(deriveCompanyFromUrl("https://careers.databricks.com/open-roles")).toBe("Databricks");
    expect(deriveCompanyFromUrl("https://jobs.ashbyhq.com/notion/5703a1d4-e1a2-4286-af10-a48c65fd4114")).toBe("Notion");
    expect(deriveCompanyFromUrl("https://api.ashbyhq.com/posting-api/job-board/Serval")).toBe("Serval");
  });
});
