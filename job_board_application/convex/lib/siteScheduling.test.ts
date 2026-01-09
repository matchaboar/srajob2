import { describe, expect, it } from "vitest";
import { deriveNextEligibleAt } from "./siteScheduling";

describe("deriveNextEligibleAt", () => {
  it("returns the latest eligible slot when behind the current window", () => {
    const now = Date.UTC(2024, 0, 1, 12, 30, 0); // Monday
    const schedule = {
      days: ["mon"],
      startTime: "08:00",
      intervalMinutes: 60,
      timezone: "UTC",
    };

    const nextEligibleAt = deriveNextEligibleAt({
      hasSchedule: true,
      schedule,
      lastRunAt: Date.UTC(2024, 0, 1, 10, 0, 0),
      completed: false,
      nowMs: now,
    });

    expect(nextEligibleAt).toBe(Date.UTC(2024, 0, 1, 12, 0, 0));
  });

  it("returns the next day slot when today is not scheduled", () => {
    const now = Date.UTC(2024, 0, 1, 12, 0, 0); // Monday
    const schedule = {
      days: ["tue"],
      startTime: "09:00",
      intervalMinutes: 1440,
      timezone: "UTC",
    };

    const nextEligibleAt = deriveNextEligibleAt({
      hasSchedule: true,
      schedule,
      lastRunAt: 0,
      completed: false,
      nowMs: now,
    });

    const expected = Date.UTC(2024, 0, 2, 9, 0, 0);
    expect(nextEligibleAt).toBeGreaterThanOrEqual(expected);
    expect(nextEligibleAt).toBeLessThanOrEqual(expected + 1);
  });

  it("returns undefined for completed unscheduled sites", () => {
    const nextEligibleAt = deriveNextEligibleAt({
      hasSchedule: false,
      schedule: null,
      lastRunAt: 123,
      completed: true,
      nowMs: Date.UTC(2024, 0, 1, 12, 0, 0),
    });

    expect(nextEligibleAt).toBeUndefined();
  });
});
