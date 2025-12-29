import { describe, expect, it } from "vitest";
import { matchesCompanyFilters, normalizeCompanyFilterKey } from "./jobs";

describe("company filter matching", () => {
  it("matches company filters ignoring punctuation and legal suffixes", () => {
    const filters = new Set([normalizeCompanyFilterKey("Airbnb")]);
    expect(matchesCompanyFilters({ company: "Airbnb, Inc.", url: null }, filters)).toBe(true);
  });

  it("matches when filter includes suffix and job does not", () => {
    const filters = new Set([normalizeCompanyFilterKey("Stripe, Inc.")]);
    expect(matchesCompanyFilters({ company: "Stripe", url: null }, filters)).toBe(true);
  });

  it("does not match partial tokens", () => {
    const filters = new Set([normalizeCompanyFilterKey("Air")]);
    expect(matchesCompanyFilters({ company: "Airbnb", url: null }, filters)).toBe(false);
  });

  it("matches via domain alias with normalized suffix", () => {
    const filters = new Set([normalizeCompanyFilterKey("Airbnb")]);
    const domainAliases = new Map([["airbnb.greenhouse.io", "Airbnb, Inc."]]);
    expect(
      matchesCompanyFilters(
        { company: "OtherCo", url: "https://boards.greenhouse.io/airbnb/jobs/123" },
        filters,
        domainAliases
      )
    ).toBe(true);
  });
});
