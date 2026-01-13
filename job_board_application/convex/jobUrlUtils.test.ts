import { describe, expect, it } from "vitest";
import { canonicalizeGreenhouseUrl, normalizeJobUrlKey } from "./jobUrlUtils";

describe("canonicalizeGreenhouseUrl", () => {
  const canonicalUrl = "https://boards.greenhouse.io/axon/jobs/7449723003";

  it("returns web format URL unchanged (canonical format)", () => {
    const webUrl = "https://boards.greenhouse.io/axon/jobs/7449723003";
    expect(canonicalizeGreenhouseUrl(webUrl)).toBe(canonicalUrl);
  });

  it("converts API format to canonical web format", () => {
    const apiUrl = "https://boards-api.greenhouse.io/v1/boards/axon/jobs/7449723003";
    expect(canonicalizeGreenhouseUrl(apiUrl)).toBe(canonicalUrl);
  });

  it("converts job-boards format to canonical web format", () => {
    const jobBoardsUrl = "https://job-boards.greenhouse.io/axon/jobs/7449723003";
    expect(canonicalizeGreenhouseUrl(jobBoardsUrl)).toBe(canonicalUrl);
  });

  it("handles trailing slashes", () => {
    expect(canonicalizeGreenhouseUrl("https://boards.greenhouse.io/axon/jobs/7449723003/"))
      .toBe(canonicalUrl);
    expect(canonicalizeGreenhouseUrl("https://boards-api.greenhouse.io/v1/boards/axon/jobs/7449723003/"))
      .toBe(canonicalUrl);
    expect(canonicalizeGreenhouseUrl("https://job-boards.greenhouse.io/axon/jobs/7449723003/"))
      .toBe(canonicalUrl);
  });

  it("preserves case of slug", () => {
    expect(canonicalizeGreenhouseUrl("https://boards.greenhouse.io/Axon/jobs/123"))
      .toBe("https://boards.greenhouse.io/Axon/jobs/123");
    expect(canonicalizeGreenhouseUrl("https://boards-api.greenhouse.io/v1/boards/Axon/jobs/123"))
      .toBe("https://boards.greenhouse.io/Axon/jobs/123");
  });

  it("returns non-Greenhouse URLs unchanged", () => {
    const otherUrls = [
      "https://jobs.lever.co/company/123",
      "https://jobs.ashbyhq.com/company/456",
      "https://example.com/careers/789",
    ];
    for (const url of otherUrls) {
      expect(canonicalizeGreenhouseUrl(url)).toBe(url);
    }
  });
});

describe("normalizeJobUrlKey", () => {
  it("canonicalizes Greenhouse URLs when normalizing", () => {
    const apiUrl = "https://boards-api.greenhouse.io/v1/boards/axon/jobs/7449723003";
    const canonical = "https://boards.greenhouse.io/axon/jobs/7449723003";
    expect(normalizeJobUrlKey(apiUrl)).toBe(canonical);
  });

  it("canonicalizes job-boards format URLs when normalizing", () => {
    const jobBoardsUrl = "https://job-boards.greenhouse.io/axon/jobs/7449723003";
    const canonical = "https://boards.greenhouse.io/axon/jobs/7449723003";
    expect(normalizeJobUrlKey(jobBoardsUrl)).toBe(canonical);
  });

  it("preserves non-Greenhouse URLs after normalization", () => {
    const leverUrl = "https://jobs.lever.co/company/123";
    expect(normalizeJobUrlKey(leverUrl)).toBe(leverUrl);
  });
});
