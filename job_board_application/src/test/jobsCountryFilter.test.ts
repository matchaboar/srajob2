import { describe, it, expect } from "vitest";
import { computeJobCountry, matchesCountryFilter } from "../../convex/jobs";

const buildJob = (overrides: Partial<any>) => ({
  _id: "job1" as any,
  _creationTime: Date.now(),
  title: "Engineer",
  company: "Example",
  location: overrides.location ?? "Remote",
  locations: overrides.locations,
  countries: overrides.countries,
  country: overrides.country,
  locationStates: overrides.locationStates,
  locationSearch: "remote",
  city: overrides.city,
  state: overrides.state,
  remote: overrides.remote ?? true,
  level: "senior" as const,
  totalCompensation: 100000,
  postedAt: Date.now(),
  url: "https://example.com",
  compensationReason: "",
  compensationUnknown: false,
  ...overrides,
});

describe("computeJobCountry", () => {
  it("defaults unknown locations to United States", () => {
    const job = buildJob({ location: "Unknown", locationStates: ["Unknown"] });
    expect(computeJobCountry(job)).toBe("United States");
  });

  it("keeps explicit country when provided", () => {
    const job = buildJob({ country: "Canada", location: "Remote" });
    expect(computeJobCountry(job)).toBe("Canada");
  });

  it("treats US state info as United States when country is missing", () => {
    const job = buildJob({ location: "Remote", locationStates: ["California"] });
    expect(computeJobCountry(job)).toBe("United States");
  });

  it("infers Canada from province abbreviation", () => {
    const job = buildJob({ location: "Toronto, ON", locationStates: ["ON"], remote: false });
    expect(computeJobCountry(job)).toBe("Canada");
  });

  it("maps remote-only listings to United States", () => {
    const job = buildJob({ location: "Remote", locationStates: ["Remote"], country: undefined });
    expect(computeJobCountry(job)).toBe("United States");
  });
});

describe("matchesCountryFilter", () => {
  it("allows everything when no country filter is provided", () => {
    expect(matchesCountryFilter("Canada", "", false)).toBe(true);
    expect(matchesCountryFilter("Unknown", "", false)).toBe(true);
  });

  it("allows Unknown when filtering for United States", () => {
    expect(matchesCountryFilter("Unknown", "United States", false)).toBe(true);
  });

  it("excludes explicit non-US when filtering for United States", () => {
    expect(matchesCountryFilter("Canada", "United States", false)).toBe(false);
  });

  it("keeps Unknown when filtering for Other", () => {
    expect(matchesCountryFilter("Unknown", "Other", true)).toBe(true);
  });

  it("excludes United States when filtering for Other", () => {
    expect(matchesCountryFilter("United States", "Other", true)).toBe(false);
  });
});
