import { describe, expect, it } from "vitest";
import { SITE_TYPES, SPIDER_CLOUD_DEFAULT_SITE_TYPES } from "./siteTypes";

describe("siteTypes", () => {
  it("includes ashby in allowed site types", () => {
    expect(SITE_TYPES).toContain("ashby");
  });

  it("defaults ashby sites to spidercloud", () => {
    expect(SPIDER_CLOUD_DEFAULT_SITE_TYPES.has("ashby")).toBe(true);
  });
});
