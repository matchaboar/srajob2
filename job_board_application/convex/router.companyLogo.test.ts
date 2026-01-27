import { describe, expect, it } from "vitest";
import { resolveCompanyLogoUrl } from "./router";

describe("resolveCompanyLogoUrl", () => {
  it("falls back to the company domain when the host is an opaque UUID", () => {
    const url = "https://c93c2f7d-f00d-409f-b288-8956f84976dd.com/jobs/role/123";
    const result = resolveCompanyLogoUrl("Voltage Park", url, "https://example.com/fallback.svg");
    expect(result).toContain("cdn.brandfetch.io/voltagepark.com");
  });

  it("uses the hosted path domain when the path includes a full domain slug", () => {
    const url = "https://jobs.ashbyhq.com/voltagepark.com/5b6e2a55-3f19-437f-ba4c-284d5b7b7724";
    const result = resolveCompanyLogoUrl("Voltage Park", url, "https://example.com/fallback.svg");
    expect(result).toContain("cdn.brandfetch.io/voltagepark.com");
    expect(result).not.toContain("voltagepark.com.com");
  });

  it("uses moov.io for moov company on rippling domain", () => {
    const url = "https://ats.rippling.com/moov/jobs/123";
    const result = resolveCompanyLogoUrl("moov", url, "https://example.com/fallback.svg");
    expect(result).toContain("cdn.brandfetch.io/moov.io");
    expect(result).not.toContain("rippling");
  });

  it("uses sentinelone.com for sentinelone company", () => {
    const url = "https://api.greenhouse.io/v1/boards/sentinellabs/jobs/123";
    const result = resolveCompanyLogoUrl("sentinelone", url, "https://example.com/fallback.svg");
    expect(result).toContain("cdn.brandfetch.io/sentinelone.com");
  });

  it("uses sentinelone.com for sentinellabs company", () => {
    const url = "https://api.greenhouse.io/v1/boards/sentinellabs/jobs/123";
    const result = resolveCompanyLogoUrl("sentinellabs", url, "https://example.com/fallback.svg");
    expect(result).toContain("cdn.brandfetch.io/sentinelone.com");
  });
});
