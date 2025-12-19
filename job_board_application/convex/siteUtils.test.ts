import { describe, expect, it } from "vitest";
import { greenhouseSlugFromUrl, normalizeSiteUrl, siteCanonicalKey } from "./siteUtils";

describe("siteUtils", () => {
  it("extracts greenhouse slug from boards-api URLs", () => {
    expect(greenhouseSlugFromUrl("https://boards-api.greenhouse.io/v1/boards/StubhubInc/jobs")).toBe("stubhubinc");
    expect(greenhouseSlugFromUrl("https://api.greenhouse.io/v1/boards/robinhood/jobs")).toBe("robinhood");
    expect(greenhouseSlugFromUrl("https://job-boards.eu.greenhouse.io/stubhubinc/jobs/4648156101")).toBe("stubhubinc");
  });

  it("normalizes greenhouse URLs to api.greenhouse slug form", () => {
    expect(normalizeSiteUrl("https://boards.greenhouse.io/v1/boards/Coupang/jobs", "greenhouse")).toBe(
      "https://api.greenhouse.io/v1/boards/coupang/jobs",
    );
    expect(normalizeSiteUrl("https://api.greenhouse.io/v1/boards/robinhood/jobs?foo=bar", "greenhouse")).toBe(
      "https://api.greenhouse.io/v1/boards/robinhood/jobs",
    );
    expect(normalizeSiteUrl("https://job-boards.eu.greenhouse.io/stubhubinc/jobs/4648156101", "greenhouse")).toBe(
      "https://api.greenhouse.io/v1/boards/stubhubinc/jobs",
    );
  });

  it("builds canonical keys that include type", () => {
    const keyA = siteCanonicalKey("https://api.greenhouse.io/v1/boards/robinhood/jobs", "greenhouse");
    const keyB = siteCanonicalKey("https://boards.greenhouse.io/v1/boards/robinhood/jobs", "greenhouse");
    expect(keyA).toBe(keyB);

    const generalKey = siteCanonicalKey("https://robinhood.com/jobs", "general");
    expect(generalKey).not.toBe(keyA);
  });

  it("preserves query strings for general site URLs", () => {
    const url =
      "https://www.github.careers/careers-home/jobs?keywords=engineer&sortBy=relevance&limit=100";
    expect(normalizeSiteUrl(url, "general")).toBe(url);
  });
});
