import { describe, expect, it, vi } from "vitest";
import {
  completeScrapeUrls,
  enqueueScrapeUrls,
  ingestJobsFromScrape,
  leaseScrapeUrlBatch,
  listSeenJobUrlsForSite,
} from "./router";
import { getHandler } from "./__tests__/getHandler";

type Row = Record<string, any>;

type Tables = {
  scrape_url_queue: Row[];
  ignored_jobs: Row[];
  seen_job_urls: Row[];
  jobs: Row[];
  job_details: Row[];
  domain_aliases: Row[];
  sites: Row[];
};

class FakeQuery {
  constructor(private rows: Row[], private filters: Record<string, any> = {}) {}
  withIndex(_name: string, cb: (q: any) => any) {
    const nextFilters = { ...this.filters };
    const builder = {
      eq: (field: string, val: any) => {
        nextFilters[field] = val;
        return builder;
      },
      lte: (field: string, val: any) => {
        nextFilters[field] = { lte: val };
        return builder;
      },
    };
    cb(builder);
    return new FakeQuery(this.rows, nextFilters);
  }
  order() {
    return this;
  }
  take(n: number) {
    return this.collect().slice(0, n);
  }
  collect() {
    return this.rows.filter((row) =>
      Object.entries(this.filters).every(([key, val]) => {
        if (val && typeof val === "object" && "lte" in val) {
          return (row as any)[key] <= (val as any).lte;
        }
        return (row as any)[key] === val;
      })
    );
  }
  first() {
    return this.collect()[0] ?? null;
  }
}

class FakeDb {
  tables: Tables;
  constructor(seed?: Partial<Tables>) {
    this.tables = {
      scrape_url_queue: seed?.scrape_url_queue ?? [],
      ignored_jobs: seed?.ignored_jobs ?? [],
      seen_job_urls: seed?.seen_job_urls ?? [],
      jobs: seed?.jobs ?? [],
      job_details: seed?.job_details ?? [],
      domain_aliases: seed?.domain_aliases ?? [],
      sites: seed?.sites ?? [],
    };
  }
  query = (table: keyof Tables) => {
    const rows = this.tables[table];
    if (!rows) throw new Error(`Unexpected table ${table}`);
    return new FakeQuery(rows);
  };
  insert = async (table: keyof Tables, payload: Row) => {
    const rows = this.tables[table];
    if (!rows) throw new Error(`Unexpected insert table ${table}`);
    const _id = `${table}-${rows.length + 1}`;
    rows.push({ _id, ...payload });
    return _id;
  };
  get = async (id: string) => {
    return this.tables.sites.find((row) => row._id === id) ?? null;
  };
  patch = async (id: string, updates: Row) => {
    for (const rows of Object.values(this.tables)) {
      const row = rows.find((r) => r._id === id);
      if (row) {
        Object.assign(row, updates);
        return;
      }
    }
    throw new Error(`Unknown id ${id}`);
  };
  delete = async (id: string) => {
    for (const key of Object.keys(this.tables) as Array<keyof Tables>) {
      const rows = this.tables[key];
      const index = rows.findIndex((row) => row._id === id);
      if (index >= 0) {
        rows.splice(index, 1);
        return;
      }
    }
  };
}

describe("scrape queue end-to-end", () => {
  it("records seen URLs after completion so future leases can skip them", async () => {
    const sourceUrl = "https://example.com/jobs";
    const site = { _id: "site-1", url: sourceUrl, name: "Example" };
    const db = new FakeDb({ sites: [site] });
    const ctx: any = { db };

    const jobUrl = "https://example.com/jobs/123";

    const enqueueHandler = getHandler(enqueueScrapeUrls);
    await enqueueHandler(ctx, {
      urls: [jobUrl],
      sourceUrl,
      provider: "spidercloud",
    });

    expect(db.tables.scrape_url_queue).toHaveLength(1);

    const leaseHandler = getHandler(leaseScrapeUrlBatch);
    const leased = await leaseHandler(ctx, { provider: "spidercloud", limit: 1 });
    expect(leased.urls.map((entry: any) => entry.url)).toEqual([jobUrl]);

    const ingestHandler = getHandler(ingestJobsFromScrape);
    const ingestRes = await ingestHandler(ctx, {
      siteId: site._id,
      jobs: [
        {
          title: "Software Engineer",
          company: "ExampleCo",
          description: "Build things",
          location: "Remote",
          remote: true,
          level: "mid",
          totalCompensation: 123,
          url: jobUrl,
          postedAt: Date.now(),
        },
      ],
    });
    expect(ingestRes.inserted).toBe(1);
    expect(db.tables.jobs).toHaveLength(1);

    const completeHandler = getHandler(completeScrapeUrls);
    await completeHandler(ctx, { urls: [jobUrl], status: "completed" });

    expect(db.tables.seen_job_urls).toEqual([
      expect.objectContaining({ sourceUrl, url: jobUrl }),
    ]);

    const listHandler = getHandler(listSeenJobUrlsForSite);
    const seen = await listHandler(ctx, { sourceUrl });
    expect(seen.urls).toContain(jobUrl);
  });

  it("stores and returns postedAt when enqueuing Greenhouse listing jobs", async () => {
    const sourceUrl = "https://api.greenhouse.io/v1/boards/acme/jobs";
    const site = { _id: "site-1", url: sourceUrl, name: "Acme" };
    const db = new FakeDb({ sites: [site] });
    const ctx: any = { db };

    const jobUrlA = "https://boards-api.greenhouse.io/v1/boards/acme/jobs/123";
    const jobUrlB = "https://boards-api.greenhouse.io/v1/boards/acme/jobs/456";
    const postedAtA = Date.now() - 5 * 60 * 1000;

    const enqueueHandler = getHandler(enqueueScrapeUrls);
    await enqueueHandler(ctx, {
      urls: [jobUrlA, jobUrlB],
      sourceUrl,
      provider: "spidercloud",
      siteId: site._id,
      pattern: null,
      postedAts: [postedAtA, null],
    });

    const queuedRows = db.tables.scrape_url_queue;
    expect(queuedRows).toHaveLength(2);
    const rowA = queuedRows.find((row) => row.url === jobUrlA);
    const rowB = queuedRows.find((row) => row.url === jobUrlB);
    expect(rowA?.postedAt).toBe(postedAtA);
    expect(rowB?.postedAt).toBeUndefined();

    const leaseHandler = getHandler(leaseScrapeUrlBatch);
    const leaseRes = await leaseHandler(ctx, { provider: "spidercloud", limit: 2 });
    const leasedA = leaseRes.urls.find((row: any) => row.url === jobUrlA);
    expect(leasedA?.postedAt).toBe(postedAtA);
  });

  it("respects scheduledAt, updates queue state, and records seen URLs across batches", async () => {
    const now = new Date("2024-01-01T00:00:00Z");
    vi.useFakeTimers();
    vi.setSystemTime(now);

    try {
      const sourceUrl = "https://example.com/jobs";
      const site = { _id: "site-2", url: sourceUrl, name: "Example" };
      const db = new FakeDb({ sites: [site] });
      const ctx: any = { db };

      const jobUrlA = "https://example.com/jobs/aaa";
      const jobUrlB = "https://example.com/jobs/bbb";

      const enqueueHandler = getHandler(enqueueScrapeUrls);
      await enqueueHandler(ctx, {
        urls: [jobUrlA, jobUrlB],
        sourceUrl,
        provider: "spidercloud",
        delaysMs: [0, 60_000],
      });

      expect(db.tables.scrape_url_queue).toHaveLength(2);

      const leaseHandler = getHandler(leaseScrapeUrlBatch);
      const firstLease = await leaseHandler(ctx, { provider: "spidercloud", limit: 5 });
      expect(firstLease.urls.map((entry: any) => entry.url)).toEqual([jobUrlA]);

      const leasedRow = db.tables.scrape_url_queue.find((row) => row.url === jobUrlA);
      expect(leasedRow?.status).toBe("processing");
      expect(leasedRow?.attempts).toBe(1);

      const ingestHandler = getHandler(ingestJobsFromScrape);
      await ingestHandler(ctx, {
        siteId: site._id,
        jobs: [
          {
            title: "Role A",
            company: "ExampleCo",
            description: "A",
            location: "Remote",
            remote: true,
            level: "mid",
            totalCompensation: 100,
            url: jobUrlA,
            postedAt: Date.now(),
          },
        ],
      });

      const completeHandler = getHandler(completeScrapeUrls);
      await completeHandler(ctx, { urls: [jobUrlA], status: "completed" });

      const completedRow = db.tables.scrape_url_queue.find((row) => row.url === jobUrlA);
      expect(completedRow?.status).toBe("completed");
      expect(typeof completedRow?.completedAt).toBe("number");

      vi.advanceTimersByTime(60_000);

      const secondLease = await leaseHandler(ctx, { provider: "spidercloud", limit: 5 });
      expect(secondLease.urls.map((entry: any) => entry.url)).toEqual([jobUrlB]);

      await ingestHandler(ctx, {
        siteId: site._id,
        jobs: [
          {
            title: "Role B",
            company: "ExampleCo",
            description: "B",
            location: "Remote",
            remote: true,
            level: "mid",
            totalCompensation: 120,
            url: jobUrlB,
            postedAt: Date.now(),
          },
        ],
      });

      await completeHandler(ctx, { urls: [jobUrlB], status: "completed" });

      const thirdLease = await leaseHandler(ctx, { provider: "spidercloud", limit: 5 });
      expect(thirdLease.urls).toEqual([]);

      const listHandler = getHandler(listSeenJobUrlsForSite);
      const seen = await listHandler(ctx, { sourceUrl });
      expect(seen.urls).toEqual(expect.arrayContaining([jobUrlA, jobUrlB]));
    } finally {
      vi.useRealTimers();
    }
  });
});
