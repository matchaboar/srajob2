import { afterEach, describe, expect, it, vi } from "vitest";
import { completeSite, leaseSite } from "./router";
import { getHandler } from "./__tests__/getHandler";

class FakeSitesQuery {
  private readonly filters: Array<{ field: string; op: "eq" | "gte" | "lte"; value: any }> = [];
  private orderDir: "asc" | "desc" | null = null;

  constructor(private readonly rows: any[]) {}

  withIndex(_name: string, cb: (q: any) => any) {
    const q = {
      eq: (field: string, val: any) => {
        this.filters.push({ field, op: "eq", value: val });
        return q;
      },
      gte: (field: string, val: any) => {
        this.filters.push({ field, op: "gte", value: val });
        return q;
      },
      lte: (field: string, val: any) => {
        this.filters.push({ field, op: "lte", value: val });
        return q;
      },
    };
    cb(q);
    return this;
  }

  order(dir: "asc" | "desc") {
    this.orderDir = dir;
    return this;
  }

  private applyFilters(rows: any[]) {
    return rows.filter((row) =>
      this.filters.every((filter) => {
        const value = (row as any)[filter.field];
        if (filter.op === "eq") return value === filter.value;
        if (filter.op === "gte") return typeof value === "number" && value >= filter.value;
        if (filter.op === "lte") return typeof value === "number" && value <= filter.value;
        return false;
      })
    );
  }

  private applyOrder(rows: any[]) {
    if (!this.orderDir) return rows;
    const dir = this.orderDir === "asc" ? 1 : -1;
    return rows.slice().sort((a, b) => {
      const aVal = (a as any).nextEligibleAt ?? 0;
      const bVal = (b as any).nextEligibleAt ?? 0;
      return dir * (aVal - bVal);
    });
  }

  collect() {
    return this.applyOrder(this.applyFilters(this.rows));
  }

  paginate({ cursor, numItems }: { cursor: number | null; numItems: number }) {
    const ordered = this.applyOrder(this.applyFilters(this.rows));
    const start = typeof cursor === "number" ? cursor : 0;
    const page = ordered.slice(start, start + numItems);
    const nextCursor = start + page.length;
    return {
      page,
      isDone: nextCursor >= ordered.length,
      continueCursor: nextCursor >= ordered.length ? null : nextCursor,
    };
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
      nextEligibleAt: Date.UTC(2023, 11, 31, 13, 0, 0),
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
      nextEligibleAt: undefined,
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
      nextEligibleAt: now.getTime() - 60_000,
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
      nextEligibleAt: now.getTime() + 60 * 60 * 1000,
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

  it("skips scheduled sites outside the schedule window even if nextEligibleAt is stale", async () => {
    const now = new Date("2024-01-01T12:00:00Z"); // Monday
    vi.useFakeTimers();
    vi.setSystemTime(now);

    const schedule = {
      _id: "sched-offday",
      days: ["tue"],
      startTime: "08:00",
      intervalMinutes: 60,
      timezone: "UTC",
    };
    const site = {
      _id: "site-offday",
      name: "Offday Co",
      url: "https://example.com/jobs",
      enabled: true,
      completed: false,
      failed: false,
      lockExpiresAt: 0,
      lastRunAt: 0,
      nextEligibleAt: now.getTime() - 60_000,
      scheduleId: schedule._id,
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
        get: async (id: string) => {
          if (id === site._id) return site;
          if (id === schedule._id) return schedule;
          return null;
        },
        patch: async () => {
          throw new Error("patch should not be called for skipped sites");
        },
      },
    };

    const handler = getHandler(leaseSite);
    const leased = await handler(ctx, { workerId: "worker-4", lockSeconds: 60 });

    expect(leased).toBeNull();
  });

  it("leases manual triggers even when schedule is not due", async () => {
    const now = new Date("2024-01-01T12:00:00Z"); // Monday
    vi.useFakeTimers();
    vi.setSystemTime(now);

    const schedule = {
      _id: "sched-manual",
      days: ["tue"],
      startTime: "08:00",
      intervalMinutes: 60,
      timezone: "UTC",
    };
    const manualTriggerAt = now.getTime() - 5 * 60 * 1000;
    const site = {
      _id: "site-manual-window",
      name: "Manual Co",
      url: "https://example.com/manual",
      enabled: true,
      completed: false,
      failed: false,
      lockExpiresAt: 0,
      lastRunAt: 0,
      manualTriggerAt,
      nextEligibleAt: now.getTime() + 60 * 60 * 1000,
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
    const leased = await handler(ctx, { workerId: "worker-5", lockSeconds: 60 });

    expect(leased?._id).toBe(site._id);
    expect(patches.some((p) => p.id === site._id && p.updates.lockedBy === "worker-5")).toBe(true);
  });
});
