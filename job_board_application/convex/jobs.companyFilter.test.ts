import { describe, expect, it } from "vitest";
import { matchesCompanyFilters } from "./jobs";

describe("matchesCompanyFilters", () => {
  it("matches when company name is selected", () => {
    const filters = new Set(["stubhub"]);
    const job = { company: "StubHub", url: "https://boards.greenhouse.io/stubhubinc/jobs/123" };

    expect(matchesCompanyFilters(job, filters, null)).toBe(true);
  });

  it("matches by domain alias when company differs", () => {
    const filters = new Set(["stubhub"]);
    const job = { company: "stubhubinc", url: "https://boards.greenhouse.io/stubhubinc/jobs/123" };
    const aliasMap = new Map([["stubhubinc.greenhouse.io", "StubHub"]]);

    expect(matchesCompanyFilters(job, filters, aliasMap)).toBe(true);
  });

  it("returns false when neither company nor alias match", () => {
    const filters = new Set(["coupang"]);
    const job = { company: "stubhubinc", url: "https://boards.greenhouse.io/stubhubinc/jobs/123" };
    const aliasMap = new Map([["stubhubinc.greenhouse.io", "StubHub"]]);

    expect(matchesCompanyFilters(job, filters, aliasMap)).toBe(false);
  });

  it("matches by ashby slug alias", () => {
    const filters = new Set(["serval"]);
    const job = { company: "ashbyhq", url: "https://jobs.ashbyhq.com/Serval/2bfaede4-22b2-43b2-a14c-f45e5f398624" };
    const aliasMap = new Map([["serval.ashbyhq.com", "Serval"]]);

    expect(matchesCompanyFilters(job, filters, aliasMap)).toBe(true);
  });
});
