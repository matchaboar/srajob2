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
  scrape_url_bucket_leases: Row[];
  scrape_worker_heartbeats: Row[];
  ignored_jobs: Row[];
  seen_job_urls: Row[];
  seen_job_url_index: Row[];
  jobs: Row[];
  job_url_keys: Row[];
  job_details: Row[];
  domain_aliases: Row[];
  sites: Row[];
};

class FakeQuery {
  constructor(
    private rows: Row[],
    private filters: Record<string, any> = {},
    private predicates: Array<(row: Row) => boolean> = []
  ) {}
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
      gt: (field: string, val: any) => {
        nextFilters[field] = { gt: val };
        return builder;
      },
    };
    cb(builder);
    return new FakeQuery(this.rows, nextFilters, this.predicates);
  }
  filter(cb: (q: any) => any) {
    const predicate = cb({
      field: (name: string) => name,
      eq: (field: string, val: any) => (row: Row) => (row as any)[field] === val,
      lte: (field: string, val: number) => (row: Row) => (row as any)[field] <= val,
      or: (...tests: Array<(row: Row) => boolean>) => (row: Row) =>
        tests.some((test) => test(row)),
      and: (...tests: Array<(row: Row) => boolean>) => (row: Row) =>
        tests.every((test) => test(row)),
    });
    return new FakeQuery(
      this.rows,
      this.filters,
      predicate ? [...this.predicates, predicate] : this.predicates
    );
  }
  order() {
    return this;
  }
  take(n: number) {
    return this.collect().slice(0, n);
  }
  collect() {
    const filtered = this.rows.filter((row) =>
      Object.entries(this.filters).every(([key, val]) => {
        if (val && typeof val === "object" && "lte" in val) {
          return (row as any)[key] <= (val as any).lte;
        }
        if (val && typeof val === "object" && "gt" in val) {
          return (row as any)[key] > (val as any).gt;
        }
        return (row as any)[key] === val;
      })
    );
    if (this.predicates.length === 0) return filtered;
    return filtered.filter((row) => this.predicates.every((predicate) => predicate(row)));
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
      scrape_url_bucket_leases: seed?.scrape_url_bucket_leases ?? [],
      scrape_worker_heartbeats: seed?.scrape_worker_heartbeats ?? [],
      ignored_jobs: seed?.ignored_jobs ?? [],
      seen_job_urls: seed?.seen_job_urls ?? [],
      seen_job_url_index: seed?.seen_job_url_index ?? [],
      jobs: seed?.jobs ?? [],
      job_url_keys: seed?.job_url_keys ?? [],
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

const buildCtx = (db: FakeDb) => ({
  db,
  storage: {
    store: async () => "storage-1",
    delete: async () => {},
  },
});

describe("scrape queue end-to-end", () => {
  it("records seen URLs after completion so future leases can skip them", async () => {
    const sourceUrl = "https://example.com/jobs";
    const site = { _id: "site-1", url: sourceUrl, name: "Example" };
    const db = new FakeDb({ sites: [site] });
    const ctx: any = buildCtx(db);

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
    const ctx: any = buildCtx(db);

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

  it("dedupes normalized URLs before enqueueing", async () => {
    const sourceUrl = "https://example.com/jobs";
    const db = new FakeDb();
    const ctx: any = buildCtx(db);
    const enqueueHandler = getHandler(enqueueScrapeUrls);

    await enqueueHandler(ctx, {
      urls: ["https://example.com/jobs?b=1&a=2", "https://example.com/jobs?a=2&b=1"],
      sourceUrl,
      provider: "spidercloud",
    });

    expect(db.tables.scrape_url_queue).toHaveLength(1);
    expect(db.tables.scrape_url_queue[0]?.url).toBe("https://example.com/jobs?a=2&b=1");
  });

  it("skips enqueueing URLs that are already ignored", async () => {
    const sourceUrl = "https://example.com/jobs";
    const ignoredUrl = "https://example.com/jobs/ignored";
    const allowedUrl = "https://example.com/jobs/allowed";
    const db = new FakeDb({
      ignored_jobs: [
        {
          _id: "ignored-1",
          url: ignoredUrl,
          sourceUrl,
          reason: "missing_required_keyword",
          createdAt: Date.now(),
        },
      ],
    });
    const ctx: any = buildCtx(db);

    const enqueueHandler = getHandler(enqueueScrapeUrls);
    const res = await enqueueHandler(ctx, {
      urls: [ignoredUrl, allowedUrl],
      sourceUrl,
      provider: "spidercloud",
    });

    expect(res.queued).toEqual([allowedUrl]);
    expect(db.tables.scrape_url_queue.map((row) => row.url)).toEqual([allowedUrl]);
  });

  it("leases only listing URLs when urlType filter is set", async () => {
    const sourceUrl = "https://example.com/jobs";
    const db = new FakeDb({
      scrape_url_queue: [
        {
          _id: "queue-1",
          url: "https://example.com/jobs?page=1",
          sourceUrl,
          provider: "spidercloud",
          urlType: "listing",
          status: "pending",
          createdAt: Date.now(),
          updatedAt: Date.now(),
          scheduledAt: Date.now(),
        },
        {
          _id: "queue-2",
          url: "https://example.com/jobs/123",
          sourceUrl,
          provider: "spidercloud",
          urlType: "detail",
          status: "pending",
          createdAt: Date.now(),
          updatedAt: Date.now(),
          scheduledAt: Date.now(),
        },
      ],
    });
    const ctx: any = buildCtx(db);

    const leaseHandler = getHandler(leaseScrapeUrlBatch);
    const leased = await leaseHandler(ctx, { provider: "spidercloud", limit: 5, urlType: "listing" });
    expect(leased.urls.map((entry: any) => entry.url)).toEqual([
      "https://example.com/jobs?page=1",
    ]);
  });

  it("does not requeue fail_expired job detail URLs", async () => {
    const sourceUrl = "https://example.com/jobs";
    const expiredUrl = "https://example.com/jobs/expired";
    const db = new FakeDb({
      scrape_url_queue: [
        {
          _id: "queue-1",
          url: expiredUrl,
          sourceUrl,
          provider: "spidercloud",
          status: "failed",
          lastError: "fail_expired",
          createdAt: Date.now() - 60 * 60 * 1000,
          updatedAt: Date.now() - 60 * 60 * 1000,
          urlType: "detail",
        },
      ],
    });
    const ctx: any = buildCtx(db);

    const enqueueHandler = getHandler(enqueueScrapeUrls);
    const res = await enqueueHandler(ctx, {
      urls: [expiredUrl],
      sourceUrl,
      provider: "spidercloud",
    });

    expect(res.queued).toEqual([]);
    expect(db.tables.scrape_url_queue).toHaveLength(1);
    expect(db.tables.scrape_url_queue[0]?.status).toBe("failed");
    expect(db.tables.scrape_url_queue[0]?.lastError).toBe("fail_expired");
  });

  it("skips enqueueing URLs that already exist as jobs", async () => {
    const sourceUrl = "https://example.com/jobs";
    const storedUrl = "https://example.com/jobs/123";
    const incomingUrl = "https://example.com/jobs/123/";
    const db = new FakeDb({
      jobs: [
        {
          _id: "job-1",
          url: storedUrl,
          title: "Engineer",
          company: "ExampleCo",
          description: "Role",
          location: "Remote",
          remote: true,
          level: "mid",
          totalCompensation: 123,
          postedAt: Date.now(),
        },
      ],
    });
    const ctx: any = buildCtx(db);

    const enqueueHandler = getHandler(enqueueScrapeUrls);
    const res = await enqueueHandler(ctx, {
      urls: [incomingUrl],
      sourceUrl,
      provider: "spidercloud",
    });

    expect(res.queued).toEqual([]);
    expect(db.tables.scrape_url_queue).toHaveLength(0);
    expect(db.tables.seen_job_urls).toEqual([
      expect.objectContaining({ sourceUrl, url: storedUrl }),
    ]);
  });

  it("respects scheduledAt, updates queue state, and records seen URLs across batches", async () => {
    const now = new Date("2024-01-01T00:00:00Z");
    vi.useFakeTimers();
    vi.setSystemTime(now);

    try {
      const sourceUrl = "https://example.com/jobs";
      const site = { _id: "site-2", url: sourceUrl, name: "Example" };
      const db = new FakeDb({ sites: [site] });
      const ctx: any = buildCtx(db);

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
