import { describe, expect, it, vi } from "vitest";
import { listScrapeActivity } from "./sites";
import { getHandler } from "./__tests__/getHandler";

type Scrape = { _id: string; siteId?: string; sourceUrl: string; completedAt?: number; items?: any };
type Run = { _id: string; siteUrls: string[]; startedAt: number; completedAt?: number; status?: string };

class FakeQuery<T extends { [key: string]: any }> {
  constructor(private table: string, private rows: T[]) {}

  withIndex(_name: string, cb: (q: any) => any) {
    if (this.table === "scrapes") {
      const value = cb({ eq: (_field: string, val: any) => val });
      const filtered = this.rows.filter(
        (r) => r.siteId === value || r.sourceUrl === value
      );
      return new FakeQuery<T>(this.table, filtered);
    }
    if (this.table === "workflow_runs") {
      const cutoff = cb({
        gte: (_field: string, val: any) => val,
      });
      const filtered = this.rows.filter((r: any) => (r.startedAt ?? 0) >= cutoff);
      return new FakeQuery<T>(this.table, filtered);
    }
    return this;
  }

  order(_dir: string) {
    return this;
  }

  take(n: number) {
    return this.rows.slice(0, n);
  }

  collect() {
    return this.rows;
  }

  paginate() {
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
      items: { items: [1, 2] },
    };
    const oldScrape: Scrape = {
      _id: "scrape-old",
      siteId: "site-1",
      sourceUrl: "https://example.com",
      completedAt: now - 90 * 24 * 60 * 60 * 1000,
      items: { items: [1, 2, 3] },
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
          if (table === "scrapes") {
            return new FakeQuery<Scrape>("scrapes", [recentScrape, oldScrape]);
          }
          if (table === "workflow_runs") {
            return new FakeQuery<Run>("workflow_runs", [recentRun]);
          }
          throw new Error(`Unexpected table ${table}`);
        },
      },
    };

    const handler = getHandler(listScrapeActivity) as any;
    const rows = await handler(ctx, {});
    vi.useRealTimers();

    expect(rows).toHaveLength(1);
    const row = rows[0];
    expect(row.totalScrapes).toBe(2); // both fetched
    expect(row.totalJobsScraped).toBe(2); // only recent counted
    expect(row.lastScrapeEnd).toBe(recentScrape.completedAt);
  });

  it("avoids paginate helpers when collect/take are available", async () => {
    const now = Date.now();
    vi.setSystemTime(now);

    const ctx: any = {
      db: {
        query: (table: string) => {
          if (table === "sites") {
            return new FakeQuery<any>("sites", [
              { _id: "site-1", url: "https://example.com", name: "Site", enabled: true, _creationTime: now },
            ]);
          }
          if (table === "scrapes") {
            return new FakeQuery<Scrape>("scrapes", [
              { _id: "scrape-1", siteId: "site-1", sourceUrl: "https://example.com", completedAt: now, items: [] },
            ]);
          }
          if (table === "workflow_runs") {
            return new FakeQuery<Run>("workflow_runs", [
              { _id: "run-1", siteUrls: ["https://example.com"], startedAt: now, status: "completed" },
            ]);
          }
          throw new Error(`Unexpected table ${table}`);
        },
      },
    };

    const handler = getHandler(listScrapeActivity) as any;
    const rows = await handler(ctx, {});
    vi.useRealTimers();

    expect(rows).toHaveLength(1);
  });
});
