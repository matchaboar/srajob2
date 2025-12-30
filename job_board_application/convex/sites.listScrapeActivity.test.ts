import { describe, expect, it, vi } from "vitest";
import { listScrapeActivity } from "./sites";
import { getHandler } from "./__tests__/getHandler";

type Scrape = { _id: string; siteId?: string; sourceUrl: string; completedAt?: number; jobCount?: number };
type Run = { _id: string; siteUrls: string[]; startedAt: number; completedAt?: number; status?: string };

type QueryTracker = { take: number; collect: number; paginate: number };

class FakeQuery<T extends { [key: string]: any }> {
  constructor(private table: string, private rows: T[], private tracker?: QueryTracker) {}

  withIndex(_name: string, cb: (q: any) => any) {
    const filters: { eq?: { field: string; value: any }; gte?: { field: string; value: any } } = {};
    const builder = {
      eq: (field: string, value: any) => {
        filters.eq = { field, value };
        return builder;
      },
      gte: (field: string, value: any) => {
        filters.gte = { field, value };
        return builder;
      },
    };
    cb(builder);

    if (this.table === "scrape_activity") {
      let filtered = this.rows;
      if (filters.eq) {
        filtered = filtered.filter((r: any) => r[filters.eq!.field] === filters.eq!.value);
      }
      if (filters.gte) {
        filtered = filtered.filter((r: any) => (r[filters.gte!.field] ?? 0) >= filters.gte!.value);
      }
      return new FakeQuery<T>(this.table, filtered, this.tracker);
    }
    if (this.table === "workflow_runs") {
      const cutoff = filters.gte?.value ?? 0;
      const filtered = this.rows.filter((r: any) => (r.startedAt ?? 0) >= cutoff);
      return new FakeQuery<T>(this.table, filtered, this.tracker);
    }
    return this;
  }

  order(_dir: string) {
    return this;
  }

  take(n: number) {
    if (this.tracker) this.tracker.take += 1;
    return this.rows.slice(0, n);
  }

  collect() {
    if (this.tracker) this.tracker.collect += 1;
    return this.rows;
  }

  paginate() {
    if (this.tracker) this.tracker.paginate += 1;
    throw new Error("paginate should not be used in listScrapeActivity");
  }
}

describe("listScrapeActivity", () => {
  it("ignores scrapes outside the lookback window when computing totals", async () => {
    const now = Date.now();
    vi.setSystemTime(now);

    const recentScrape: Scrape = {
      _id: "scrape-new",
      siteId: "site-1",
      sourceUrl: "https://example.com",
      completedAt: now - 5 * 24 * 60 * 60 * 1000,
      jobCount: 2,
    };
    const oldScrape: Scrape = {
      _id: "scrape-old",
      siteId: "site-1",
      sourceUrl: "https://example.com",
      completedAt: now - 90 * 24 * 60 * 60 * 1000,
      jobCount: 3,
    };
    const recentRun: Run = {
      _id: "run-1",
      siteUrls: ["https://example.com"],
      startedAt: now - 2 * 24 * 60 * 60 * 1000,
      completedAt: now - 2 * 24 * 60 * 60 * 1000,
      status: "completed",
    };
    const ctx: any = {
      db: {
        query: (table: string) => {
          if (table === "sites") {
            return new FakeQuery<any>("sites", [
              { _id: "site-1", url: "https://example.com", name: "Site", enabled: true, _creationTime: now },
            ]);
          }
          if (table === "scrape_activity") {
            return new FakeQuery<Scrape>("scrape_activity", [recentScrape, oldScrape]);
          }
          if (table === "workflow_runs") {
            return new FakeQuery<Run>("workflow_runs", [recentRun]);
          }
          throw new Error(`Unexpected table ${table}`);
        },
      },
    };

    const handler = getHandler(listScrapeActivity);
    const rows = await handler(ctx, {});
    vi.useRealTimers();

    expect(rows).toHaveLength(1);
    const row = rows[0];
    expect(row.totalScrapes).toBe(1); // only recent scrapes counted
    expect(row.totalJobsScraped).toBe(2); // only recent counted
    expect(row.lastScrapeEnd).toBe(recentScrape.completedAt);
  });

  it("avoids paginate helpers when collect/take are available", async () => {
    const now = Date.now();
    vi.setSystemTime(now);
    const tracker: QueryTracker = { take: 0, collect: 0, paginate: 0 };

    const ctx: any = {
      db: {
        query: (table: string) => {
          if (table === "sites") {
            return new FakeQuery<any>("sites", [
              { _id: "site-1", url: "https://example.com", name: "Site", enabled: true, _creationTime: now },
            ], tracker);
          }
          if (table === "scrape_activity") {
            return new FakeQuery<Scrape>("scrape_activity", [
              { _id: "scrape-1", siteId: "site-1", sourceUrl: "https://example.com", completedAt: now, jobCount: 0 },
            ], tracker);
          }
          if (table === "workflow_runs") {
            return new FakeQuery<Run>("workflow_runs", [
              { _id: "run-1", siteUrls: ["https://example.com"], startedAt: now, status: "completed" },
            ], tracker);
          }
          throw new Error(`Unexpected table ${table}`);
        },
      },
    };

    const handler = getHandler(listScrapeActivity);
    const rows = await handler(ctx, {});
    vi.useRealTimers();

    expect(rows).toHaveLength(1);
    expect(tracker.paginate).toBe(0);
    expect(tracker.collect).toBe(0);
    expect(tracker.take).toBeGreaterThan(0);
  });
});
