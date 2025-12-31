import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
  findCityInText,
  formatLocationLabel,
  normalizeLocations,
  deriveLocationFields,
  resolveLocationFromDictionary,
  splitLocation,
} from "./location";

const readFixture = (filename: string) =>
  readFileSync(path.resolve(process.cwd(), "..", "tests/fixtures", filename), "utf8");
const readLocationDictionary = () =>
  JSON.parse(readFileSync(path.resolve(process.cwd(), "convex/locationDictionary.json"), "utf8"));

type LocationDictionaryEntry = {
  city: string;
  state: string;
  country: string;
  aliases?: string[];
  remoteOnly?: boolean;
  population?: number;
};

type LocationDictionaryValue = LocationDictionaryEntry | LocationDictionaryEntry[];

describe("location dictionary coverage", () => {
  it("stores city-keyed dictionary entries with at least 1,000 rows", () => {
    const dictionary = readLocationDictionary() as Record<string, LocationDictionaryValue>;
    const totalEntries = Object.values(dictionary).reduce((count, value) => {
      if (Array.isArray(value)) return count + value.length;
      return count + 1;
    }, 0);
    expect(totalEntries).toBeGreaterThanOrEqual(1000);
    expect(dictionary["Boston"]).toBeTruthy();
  });

  it("resolves international cities with country labels", () => {
    const madrid = splitLocation("Madrid, Spain");
    expect(madrid.city).toBe("Madrid");
    expect(madrid.state).toBe("Spain");
    expect(formatLocationLabel(madrid.city, madrid.state, null, madrid.country)).toBe("Madrid, Spain");

    const saoPaulo = splitLocation("Sao Paulo, Brazil");
    expect(saoPaulo.city).toBe("Sao Paulo");
    expect(saoPaulo.state).toBe("Brazil");
    expect(formatLocationLabel(saoPaulo.city, saoPaulo.state, null, saoPaulo.country)).toBe("Sao Paulo, Brazil");
  });

  it("finds common cities inside markdown bodies", () => {
    const markdown = readFixture("datadog-commonmark-spidercloud.md");
    const match = findCityInText(markdown);
    expect(match?.city).toBe("Madrid");
    expect(match?.state).toBe("Spain");
  });

  it("detects South Korea entries explicitly", () => {
    const markdown = "Senior Engineer\nSeoul, South Korea\nResponsibilities follow.";
    const match = findCityInText(markdown);
    expect(match?.city).toBe("Seoul");
    expect(match?.state).toBe("South Korea");
  });

  it("handles cities that share the same name across countries", () => {
    const cambridgeUs = splitLocation("Cambridge, MA");
    expect(cambridgeUs.city).toBe("Cambridge");
    expect(cambridgeUs.state).toBe("Massachusetts");

    const cambridgeUk = splitLocation("Cambridge, United Kingdom");
    expect(cambridgeUk.city).toBe("Cambridge");
    expect(cambridgeUk.state).toBe("United Kingdom");
  });

  it("only treats explicit remote-only phrases as remote locations", () => {
    expect(resolveLocationFromDictionary("remote friendly across the us")).toBeNull();
    const remoteOnly = resolveLocationFromDictionary("Remote, US");
    expect(remoteOnly?.city).toBe("Remote");
    expect(remoteOnly?.state).toBe("Remote");
  });

  it("normalizes multiple comma-separated dictionary locations", () => {
    const normalized = normalizeLocations(["Madrid, Spain; Paris, France"]);
    expect(normalized).toEqual(["Madrid, Spain", "Paris, France"]);
  });

  it("prioritizes United States location as primary when present", () => {
    const normalized = normalizeLocations(["Madrid, Spain; Boston, MA"]);
    expect(normalized[0]).toBe("Boston, Massachusetts");

    const fields = deriveLocationFields({ locations: normalized });
    expect(fields.primaryLocation).toBe("Boston, Massachusetts");
    expect(fields.locations[0]).toBe("Boston, Massachusetts");
  });

  it("keeps non-US primary when no US location exists", () => {
    const normalized = normalizeLocations(["Madrid, Spain; Paris, France"]);
    const fields = deriveLocationFields({ locations: normalized });
    expect(fields.primaryLocation).toBe("Madrid, Spain");
  });
});
