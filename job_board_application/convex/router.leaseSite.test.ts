import { afterEach, describe, expect, it, vi } from "vitest";
import { completeSite, leaseSite } from "./router";
import { getHandler } from "./__tests__/getHandler";

class FakeSitesQuery {
  constructor(private readonly rows: any[]) {}

  withIndex(_name: string, cb: (q: any) => any) {
    cb({ eq: (_field: string, val: any) => val });
    return this;
  }

  collect() {
    return this.rows;
  }
}

class FakeRunRequestsQuery {
  withIndex(_name: string, cb: (q: any) => any) {
    const eq = (_field: string, _val: any) => ({ eq });
    cb({ eq });
    return this;
  }

  order(_dir: string) {
    return this;
  }

  first() {
    return null;
  }
}

describe("leaseSite", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("leases scheduled sites even if they were previously completed", async () => {
    const now = new Date("2024-01-01T12:00:00Z"); // Monday
    vi.useFakeTimers();
    vi.setSystemTime(now);

    const schedule = {
      _id: "sched-1",
      days: ["mon"],
      startTime: "08:00",
      intervalMinutes: 60,
      timezone: "UTC",
    };
    const site = {
      _id: "site-1",
      name: "Snap",
      url: "https://careers.snap.com/jobs",
      enabled: true,
      completed: true,
      failed: false,
      lockExpiresAt: 0,
      lastRunAt: Date.UTC(2023, 11, 31, 12, 0, 0), // previous day
      scheduleId: schedule._id,
      type: "general",
    };

    const patches: Array<{ id: string; updates: Record<string, any> }> = [];
    const ctx: any = {
      db: {
        query: (table: string) => {
          if (table === "sites") {
            return new FakeSitesQuery([site]);
          }
          if (table === "run_requests") {
            return new FakeRunRequestsQuery();
          }
          throw new Error(`Unexpected table ${table}`);
        },
        get: async (id: string) => {
          if (id === site._id) return site;
          if (id === schedule._id) return schedule;
          return null;
        },
        patch: async (id: string, updates: Record<string, any>) => {
          patches.push({ id, updates });
          if (id === site._id) Object.assign(site, updates);
        },
      },
    };

    const handler = getHandler(leaseSite);
    const leased = await handler(ctx, { workerId: "worker-1", lockSeconds: 60 });

    expect(leased?._id).toBe(site._id);
    expect(leased?.url).toBe(site.url);
    expect(patches.some((p) => p.id === site._id && p.updates.lockedBy === "worker-1")).toBe(true);
  });

  it("still skips completed sites without a schedule", async () => {
    const site = {
      _id: "site-2",
      name: "Example",
      url: "https://example.com/jobs",
      enabled: true,
      completed: true,
      failed: false,
      lockExpiresAt: 0,
      lastRunAt: 0,
      scheduleId: undefined,
      type: "general",
    };

    const ctx: any = {
      db: {
        query: (table: string) => {
          if (table === "sites") {
            return new FakeSitesQuery([site]);
          }
          if (table === "run_requests") {
            return new FakeRunRequestsQuery();
          }
          throw new Error(`Unexpected table ${table}`);
        },
        get: async (_id: string) => site,
        patch: async () => {
          throw new Error("patch should not be called for skipped sites");
        },
      },
    };

    const handler = getHandler(leaseSite);
    const leased = await handler(ctx, { workerId: "worker-2", lockSeconds: 60 });

    expect(leased).toBeNull();
  });

  it("clears manual triggers after completion to avoid repeat leases", async () => {
    const now = new Date("2024-01-01T12:00:00Z"); // Monday
    vi.useFakeTimers();
    vi.setSystemTime(now);

    const schedule = {
      _id: "sched-1",
      days: ["mon"],
      startTime: "08:00",
      intervalMinutes: 60,
      timezone: "UTC",
    };
    const site = {
      _id: "site-manual",
      name: "Robinhood",
      url: "https://api.greenhouse.io/v1/boards/robinhood/jobs",
      enabled: true,
      completed: false,
      failed: false,
      lockExpiresAt: 0,
      lastRunAt: 0,
      manualTriggerAt: now.getTime(),
      scheduleId: schedule._id,
      type: "greenhouse",
      scrapeProvider: "spidercloud",
    };

    const patches: Array<{ id: string; updates: Record<string, any> }> = [];
    const ctx: any = {
      db: {
        query: (table: string) => {
          if (table === "sites") {
            return new FakeSitesQuery([site]);
          }
          if (table === "run_requests") {
            return new FakeRunRequestsQuery();
          }
          throw new Error(`Unexpected table ${table}`);
        },
        get: async (id: string) => {
          if (id === site._id) return site;
          if (id === schedule._id) return schedule;
          return null;
        },
        patch: async (id: string, updates: Record<string, any>) => {
          patches.push({ id, updates });
          if (id === site._id) Object.assign(site, updates);
        },
      },
    };

    const leaseHandler = getHandler(leaseSite);
    const completeHandler = getHandler(completeSite);

    const leased = await leaseHandler(ctx, { workerId: "worker-1", lockSeconds: 60, scrapeProvider: "spidercloud" });
    expect(leased?._id).toBe(site._id);

    await completeHandler(ctx, { id: site._id });

    expect(site.manualTriggerAt).toBe(0);
    expect(patches.some((p) => p.updates.manualTriggerAt === 0)).toBe(true);

    const leasedAgain = await leaseHandler(ctx, { workerId: "worker-1", lockSeconds: 60, scrapeProvider: "spidercloud" });
    expect(leasedAgain).toBeNull();
  });

  it("ignores stale manual triggers once a run has already completed", async () => {
    const now = new Date("2024-01-01T12:00:00Z"); // Monday
    vi.useFakeTimers();
    vi.setSystemTime(now);

    const schedule = {
      _id: "sched-2",
      days: ["mon"],
      startTime: "12:00",
      intervalMinutes: 60,
      timezone: "UTC",
    };
    const triggerAt = now.getTime() - 5_000;
    const site = {
      _id: "site-loop",
      name: "Robinhood",
      url: "https://api.greenhouse.io/v1/boards/robinhood/jobs",
      enabled: true,
      completed: true,
      failed: false,
      lockExpiresAt: 0,
      lastRunAt: now.getTime(), // run already happened after the manual trigger
      manualTriggerAt: triggerAt,
      scheduleId: schedule._id,
      type: "greenhouse",
      scrapeProvider: "spidercloud",
    };

    const patches: Array<{ id: string; updates: Record<string, any> }> = [];
    const ctx: any = {
      db: {
        query: (table: string) => {
          if (table === "sites") {
            return new FakeSitesQuery([site]);
          }
          if (table === "run_requests") {
            return new FakeRunRequestsQuery();
          }
          throw new Error(`Unexpected table ${table}`);
        },
        get: async (id: string) => {
          if (id === site._id) return site;
          if (id === schedule._id) return schedule;
          return null;
        },
        patch: async (id: string, updates: Record<string, any>) => {
          patches.push({ id, updates });
          if (id === site._id) Object.assign(site, updates);
        },
      },
    };

    const leaseHandler = getHandler(leaseSite);
    const leased = await leaseHandler(ctx, { workerId: "worker-3", lockSeconds: 60, scrapeProvider: "spidercloud" });

    expect(leased).toBeNull();
    expect(patches).toHaveLength(0);
  });
});
