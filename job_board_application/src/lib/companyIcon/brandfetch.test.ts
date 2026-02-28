import { describe, expect, it } from "vitest";
import {
  deriveBrandfetchDomain,
  BRANDFETCH_DOMAIN_OVERRIDES,
} from "./brandfetch";

describe("deriveBrandfetchDomain", () => {
  it("falls back to the company domain when the host is an opaque UUID", () => {
    const result = deriveBrandfetchDomain(
      "Voltage Park",
      "https://c93c2f7d-f00d-409f-b288-8956f84976dd.com/jobs/role/123",
    );
    expect(result).toBe("voltagepark.com");
  });

  it("uses the hosted path domain when the path includes a full domain slug", () => {
    const result = deriveBrandfetchDomain(
      "Voltage Park",
      "https://jobs.ashbyhq.com/voltagepark.com/5b6e2a55-3f19-437f-ba4c-284d5b7b7724",
    );
    expect(result).toBe("voltagepark.com");
  });

  it("uses moov.io for moov company on rippling domain", () => {
    const result = deriveBrandfetchDomain(
      "moov",
      "https://ats.rippling.com/moov/jobs/123",
    );
    expect(result).toBe("moov.io");
  });

  it("uses sentinelone.com for sentinelone company", () => {
    const result = deriveBrandfetchDomain(
      "sentinelone",
      "https://api.greenhouse.io/v1/boards/sentinellabs/jobs/123",
    );
    expect(result).toBe("sentinelone.com");
  });

  it("uses sentinelone.com for sentinellabs company", () => {
    const result = deriveBrandfetchDomain(
      "sentinellabs",
      "https://api.greenhouse.io/v1/boards/sentinellabs/jobs/123",
    );
    expect(result).toBe("sentinelone.com");
  });

  it("uses hioscar.com for oscar company", () => {
    const result = deriveBrandfetchDomain(
      "oscar",
      "https://careers.hioscar.com/jobs/123",
    );
    expect(result).toBe("hioscar.com");
  });

  it("uses serval.com for serval company", () => {
    const result = deriveBrandfetchDomain("serval", "https://serval.ai/careers");
    expect(result).toBe("serval.com");
  });
});

describe("BRANDFETCH_DOMAIN_OVERRIDES consistency", () => {
  it("includes all required domain overrides", () => {
    // These overrides must exist to prevent incorrect logos
    const requiredOverrides: Record<string, string> = {
      moov: "moov.io",
      oscar: "hioscar.com",
      sentinelone: "sentinelone.com",
      sentinellabs: "sentinelone.com",
      serval: "serval.com",
    };

    for (const [slug, expectedDomain] of Object.entries(requiredOverrides)) {
      expect(BRANDFETCH_DOMAIN_OVERRIDES[slug]).toBe(expectedDomain);
    }
  });
});
