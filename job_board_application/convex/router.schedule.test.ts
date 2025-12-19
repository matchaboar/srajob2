import { afterEach, describe, expect, it, vi } from "vitest";
import { updateSiteSchedule } from "./router";
import { getHandler } from "./__tests__/getHandler";

type FakeCtx = {
  db: {
    get: (id: string) => Promise<any>;
    patch: (id: string, updates: Record<string, any>) => Promise<void>;
  };
};

const dayStartUtc = (date: Date) =>
  Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 0, 0, 0);

const scheduleFor = (overrides: Partial<any> = {}) => ({
  _id: "sched-1",
  days: ["mon"],
  startTime: "09:30",
  intervalMinutes: 24 * 60,
  timezone: "UTC",
  ...overrides,
});

const siteFor = (overrides: Partial<any> = {}) => ({
  _id: "site-1",
  lastRunAt: 0,
  scheduleId: undefined,
  ...overrides,
});

describe("updateSiteSchedule", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("backdates lastRunAt when attaching a schedule whose window already passed today", async () => {
    const now = new Date("2024-01-01T15:00:00Z"); // Monday, 3:00pm UTC
    vi.useFakeTimers();
    vi.setSystemTime(now);

    const schedule = scheduleFor({ days: ["mon"], startTime: "09:30" });
    const site = siteFor({ lastRunAt: Date.now() });
    const patches: Array<{ id: string; updates: Record<string, any> }> = [];
    const ctx: FakeCtx = {
      db: {
        get: async (id) => {
          if (id === site._id) return site;
          if (id === schedule._id) return schedule;
          return null;
        },
        patch: async (id, updates) => {
          patches.push({ id, updates });
        },
      },
    };

    const handler = getHandler(updateSiteSchedule);
    await handler(ctx as any, { id: site._id, scheduleId: schedule._id });

    expect(patches).toHaveLength(1);
    const patch = patches[0].updates;
    expect(patch.scheduleId).toBe(schedule._id);

    const dayStart = dayStartUtc(now);
    const startMinutes = 9 * 60 + 30;
    const expectedEligibleAt = dayStart + startMinutes * 60 * 1000;
    expect(patch.lastRunAt).toBe(expectedEligibleAt - 1);
    expect(patch.lastRunAt).toBeLessThan(site.lastRunAt);
  });

  it("does not change lastRunAt if schedule window has not started yet", async () => {
    const now = new Date("2024-01-01T07:00:00Z"); // Before 09:30 start (Monday)
    vi.useFakeTimers();
    vi.setSystemTime(now);

    const schedule = scheduleFor({ days: ["mon"], startTime: "09:30" });
    const site = siteFor({ lastRunAt: 0 });
    const patches: Array<{ id: string; updates: Record<string, any> }> = [];
    const ctx: FakeCtx = {
      db: {
        get: async (id) => {
          if (id === site._id) return site;
          if (id === schedule._id) return schedule;
          return null;
        },
        patch: async (id, updates) => {
          patches.push({ id, updates });
        },
      },
    };

    const handler = getHandler(updateSiteSchedule);
    await handler(ctx as any, { id: site._id, scheduleId: schedule._id });

    expect(patches).toHaveLength(1);
    const patch = patches[0].updates;
    expect(patch.scheduleId).toBe(schedule._id);
    expect(patch.lastRunAt).toBeUndefined();
  });

  it("gracefully handles missing schedule records", async () => {
    const scheduleId = "sched-missing";
    const site = siteFor({ lastRunAt: 10, scheduleId: undefined });
    const patches: Array<{ id: string; updates: Record<string, any> }> = [];
    const ctx: FakeCtx = {
      db: {
        get: async (id) => {
          if (id === site._id) return site;
          return null;
        },
        patch: async (id, updates) => {
          patches.push({ id, updates });
        },
      },
    };

    const handler = getHandler(updateSiteSchedule);
    await handler(ctx as any, { id: site._id, scheduleId });

    expect(patches).toHaveLength(1);
    const patch = patches[0].updates;
    expect(patch.scheduleId).toBe(scheduleId);
    expect(patch.lastRunAt).toBeUndefined();
  });
});
