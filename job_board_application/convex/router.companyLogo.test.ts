import { describe, expect, it } from "vitest";
import { resolveCompanyLogoUrl } from "./router";

describe("resolveCompanyLogoUrl", () => {
  it("falls back to the company domain when the host is an opaque UUID", () => {
    const url = "https://c93c2f7d-f00d-409f-b288-8956f84976dd.com/jobs/role/123";
    const result = resolveCompanyLogoUrl("Voltage Park", url, "https://example.com/fallback.svg");
    expect(result).toContain("cdn.brandfetch.io/voltagepark.com");
  });
});
