import { describe, expect, it } from "vitest";
import { formatCompensationDisplay, parseCompensationInput } from "./compensation";

describe("parseCompensationInput", () => {
  it("parses k-suffixed values", () => {
    expect(parseCompensationInput("10k")).toBe(10000);
    expect(parseCompensationInput("$10k")).toBe(10000);
    expect(parseCompensationInput("$100k")).toBe(100000);
  });

  it("parses plain numeric values", () => {
    expect(parseCompensationInput("120000")).toBe(120000);
  });

  it("clamps to max when provided", () => {
    expect(parseCompensationInput("$900k", { max: 800000 })).toBe(800000);
  });

  it("returns null for invalid inputs", () => {
    expect(parseCompensationInput("")).toBeNull();
    expect(parseCompensationInput("abc")).toBeNull();
  });
});

describe("formatCompensationDisplay", () => {
  it("formats values to $Xk", () => {
    expect(formatCompensationDisplay(100000)).toBe("$100k");
    expect(formatCompensationDisplay(null)).toBe("");
  });
});
