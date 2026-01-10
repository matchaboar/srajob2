import { describe, expect, it, vi } from "vitest";
import {
  completeScrapeUrls,
  leaseScrapeUrlBuckets,
  leaseScrapeUrlBatch,
  requeueStaleScrapeUrls,
  resetScrapeUrlsByStatus,
} from "./router";
import { getHandler } from "./__tests__/getHandler";

type QueueRow = {
  _id: string;
  url: string;
  status: string;
  updatedAt: number;
  createdAt?: number;
  scheduledAt?: number;
  provider?: string;
  attempts?: number;
  sourceUrl?: string;
  siteId?: string;
  lastError?: string;
  completedAt?: number;
  urlType?: "listing" | "detail";
  bucket?: number;
};

type IndexCall = {
  indexName: string | null;
  scheduledAtMax: number | null;
  filterFields: Record<string, any>;
};

const SCRAPE_URL_QUEUE_BUCKETS = 128;

const hashStringToBucket = (value: string, bucketCount: number) => {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0) % bucketCount;
};

const deriveScrapeQueueBucket = (params: { url: string; sourceUrl?: string | null; siteId?: string | null }) => {
  if (params.siteId) return hashStringToBucket(`site:${params.siteId}`, SCRAPE_URL_QUEUE_BUCKETS);
  if (params.sourceUrl) return hashStringToBucket(`source:${params.sourceUrl}`, SCRAPE_URL_QUEUE_BUCKETS);
  try {
    const host = new URL(params.url).hostname.toLowerCase();
    if (host) return hashStringToBucket(`domain:${host}`, SCRAPE_URL_QUEUE_BUCKETS);
  } catch {
    // fall through to url hash
  }
  return hashStringToBucket(`url:${params.url}`, SCRAPE_URL_QUEUE_BUCKETS);
};

class FakeQuery {
  constructor(
    private getRows: () => QueueRow[],
    private filterFields: Record<string, any> = {},
    private scheduledAtMax: number | null = null,
    private indexName: string | null = null,
    private ordered: boolean = false,
    private tracker?: { indexCalls: IndexCall[] },
    private predicates: Array<(row: QueueRow) => boolean> = [],
    private expiresAtMin: number | null = null
  ) {}
  withIndex(name: string, cb: (q: any) => any) {
    const filterFields = { ...this.filterFields };
    let scheduledAtMax = this.scheduledAtMax;
    let expiresAtMin = this.expiresAtMin;
    const indexName = name;
    const builder = {
      eq: (field: string, val: string) => {
        filterFields[field] = val;
        return builder;
      },
      lte: (field: string, val: number) => {
        if (field === "scheduledAt") {
          if (indexName === "by_status_attempts_scheduled_at" && filterFields.attempts === undefined) {
            throw new Error("Index order violation: attempts must be constrained before scheduledAt");
          }
          scheduledAtMax = val;
        }
        return builder;
      },
      gt: (field: string, val: number) => {
        if (field === "expiresAt") {
          expiresAtMin = val;
        }
        return builder;
      },
    };
    cb(builder);
    if (this.tracker) {
      this.tracker.indexCalls.push({
        indexName,
        scheduledAtMax,
        filterFields: { ...filterFields },
      });
    }
    return new FakeQuery(
      this.getRows,
      filterFields,
      scheduledAtMax,
      indexName,
      this.ordered,
      this.tracker,
      this.predicates,
      expiresAtMin
    );
  }
  order() {
    return new FakeQuery(
      this.getRows,
      this.filterFields,
      this.scheduledAtMax,
      this.indexName,
      true,
      this.tracker,
      this.predicates,
      this.expiresAtMin
    );
  }
  filter(cb: (q: any) => any) {
    const predicate = cb({
      field: (name: string) => name,
      eq: (field: string, val: any) => (row: QueueRow) => {
        const fieldVal = (row as any)[field];
        if (val === null) {
          return fieldVal === null || fieldVal === undefined;
        }
        return fieldVal === val;
      },
      lte: (field: string, val: number) => (row: QueueRow) => (row as any)[field] <= val,
      gt: (field: string, val: number) => (row: QueueRow) => (row as any)[field] > val,
      or: (...tests: Array<(row: QueueRow) => boolean>) => (row: QueueRow) =>
        tests.some((test) => test(row)),
      and: (...tests: Array<(row: QueueRow) => boolean>) => (row: QueueRow) =>
        tests.every((test) => test(row)),
    });
    return new FakeQuery(
      this.getRows,
      this.filterFields,
      this.scheduledAtMax,
      this.indexName,
      this.ordered,
      this.tracker,
      predicate ? [...this.predicates, predicate] : this.predicates,
      this.expiresAtMin
    );
  }
  private _filterRows(rows: QueueRow[]) {
    let filtered = rows;
    for (const [field, val] of Object.entries(this.filterFields)) {
      filtered = filtered.filter((row) => (row as any)[field] === val);
    }
    if (this.predicates.length > 0) {
      filtered = filtered.filter((row) => this.predicates.every((predicate) => predicate(row)));
    }
    if (this.scheduledAtMax !== null) {
      const scheduledAtMax = this.scheduledAtMax;
      filtered = filtered.filter((row) => {
        const scheduledAt = row.scheduledAt ?? 0;
        return scheduledAt <= scheduledAtMax;
      });
    }
    if (this.expiresAtMin !== null) {
      const expiresAtMin = this.expiresAtMin;
      filtered = filtered.filter((row) => (row as any).expiresAt > expiresAtMin);
    }
    if (
      this.ordered &&
      (this.indexName === "by_status_attempts_created_at" ||
        this.indexName === "by_status_bucket_attempts_created_at" ||
        this.indexName === "by_status_url_type_attempts_created_at")
    ) {
      filtered = filtered.slice().sort((a, b) => {
        const attemptsA = a.attempts ?? 0;
        const attemptsB = b.attempts ?? 0;
        if (attemptsA !== attemptsB) return attemptsA - attemptsB;
        const createdA = a.createdAt ?? 0;
        const createdB = b.createdAt ?? 0;
        return createdA - createdB;
      });
    } else if (this.ordered && this.indexName === "by_status_attempts_scheduled_at") {
      filtered = filtered.slice().sort((a, b) => {
        const attemptsA = a.attempts ?? 0;
        const attemptsB = b.attempts ?? 0;
        if (attemptsA !== attemptsB) return attemptsA - attemptsB;
        const scheduledA = a.scheduledAt ?? 0;
        const scheduledB = b.scheduledAt ?? 0;
        if (scheduledA !== scheduledB) return scheduledA - scheduledB;
        const createdA = a.createdAt ?? 0;
        const createdB = b.createdAt ?? 0;
        return createdA - createdB;
      });
    } else if (this.ordered && this.indexName === "by_status_and_scheduled_at") {
      filtered = filtered.slice().sort((a, b) => {
        const scheduledA = a.scheduledAt ?? 0;
        const scheduledB = b.scheduledAt ?? 0;
        if (scheduledA !== scheduledB) return scheduledA - scheduledB;
        const createdA = a.createdAt ?? 0;
        const createdB = b.createdAt ?? 0;
        return createdA - createdB;
      });
    }
    return filtered;
  }
  take(n: number) {
    return this._filterRows(this.getRows()).slice(0, n);
  }
  first() {
    return this._filterRows(this.getRows())[0];
  }
  collect() {
    return this._filterRows(this.getRows());
  }
}

class FakeDb {
  constructor(
    private queueRows: QueueRow[],
    private ignoredRows: Array<any> = [],
    private seenRows: Array<any> = [],
    private seenIndexRows: Array<any> = [],
    private bucketLeaseRows: Array<any> = [],
    private heartbeatRows: Array<any> = [],
    private tracker?: { indexCalls: IndexCall[] }
  ) {}
  query = (table: string) => {
    if (table === "scrape_url_queue") {
      return new FakeQuery(() => this.queueRows, {}, null, null, false, this.tracker);
    }
    if (table === "seen_job_urls") {
      return new FakeQuery(() => this.seenRows, {}, null, null, false, this.tracker);
    }
    if (table === "seen_job_url_index") {
      return new FakeQuery(() => this.seenIndexRows, {}, null, null, false, this.tracker);
    }
    if (table === "domain_aliases") {
      return new FakeQuery(() => [] as QueueRow[], {}, null, null, false, this.tracker);
    }
    if (table === "scrape_url_bucket_leases") {
      return new FakeQuery(() => this.bucketLeaseRows as QueueRow[], {}, null, null, false, this.tracker);
    }
    if (table === "scrape_worker_heartbeats") {
      return new FakeQuery(() => this.heartbeatRows as QueueRow[], {}, null, null, false, this.tracker);
    }
    throw new Error(`Unexpected table ${table}`);
  };
  insert = vi.fn((table: string, payload: any) => {
    if (table === "ignored_jobs") {
      this.ignoredRows.push(payload);
      return "ignored-id";
    }
    if (table === "seen_job_urls") {
      this.seenRows.push(payload);
      return `seen-${this.seenRows.length}`;
    }
    if (table === "seen_job_url_index") {
      this.seenIndexRows.push(payload);
      return `seen-index-${this.seenIndexRows.length}`;
    }
    if (table === "scrape_url_bucket_leases") {
      const _id = `bucket-${this.bucketLeaseRows.length + 1}`;
      this.bucketLeaseRows.push({ _id, ...payload });
      return _id;
    }
    if (table === "scrape_worker_heartbeats") {
      const _id = `heartbeat-${this.heartbeatRows.length + 1}`;
      this.heartbeatRows.push({ _id, ...payload });
      return _id;
    }
    throw new Error(`Unexpected insert table ${table}`);
  });
  patch = vi.fn((id: string, updates: any) => {
    const row = this.queueRows.find((r) => r._id === id);
    if (row) {
      Object.assign(row, updates);
      return;
    }
    const bucketRow = this.bucketLeaseRows.find((r) => r._id === id);
    if (bucketRow) {
      Object.assign(bucketRow, updates);
      return;
    }
    const heartbeatRow = this.heartbeatRows.find((r) => r._id === id);
    if (heartbeatRow) {
      Object.assign(heartbeatRow, updates);
      return;
    }
    throw new Error(`Unknown id ${id}`);
  });
  delete = vi.fn((id: string) => {
    const idx = this.queueRows.findIndex((row) => row._id === id);
    if (idx >= 0) {
      this.queueRows.splice(idx, 1);
      return;
    }
    const bucketIdx = this.bucketLeaseRows.findIndex((row) => row._id === id);
    if (bucketIdx >= 0) {
      this.bucketLeaseRows.splice(bucketIdx, 1);
      return;
    }
    const heartbeatIdx = this.heartbeatRows.findIndex((row) => row._id === id);
    if (heartbeatIdx >= 0) {
      this.heartbeatRows.splice(heartbeatIdx, 1);
      return;
    }
    throw new Error(`Unknown id ${id}`);
  });
  getIgnored() {
    return this.ignoredRows;
  }
  getSeen() {
    return this.seenRows;
  }
  getBucketLeases() {
    return this.bucketLeaseRows;
  }
  getHeartbeats() {
    return this.heartbeatRows;
  }
}

describe("leaseScrapeUrlBatch", () => {
  it("avoids querying scheduledAt against the attempts index out of order", async () => {
    const now = Date.now();
    const rows: QueueRow[] = [
      {
        _id: "row-1",
        url: "https://example.com/job/1",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 10_000,
        scheduledAt: now - 500,
        provider: "spidercloud",
        attempts: 0,
      },
    ];
    const tracker = { indexCalls: [] as IndexCall[] };
    const db = new FakeDb(rows, [], [], [], [], [], tracker);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBatch);

    await handler(ctx, {
      provider: "spidercloud",
      limit: 1,
      processingExpiryMs: 15 * 60 * 1000,
    });

    const invalidIndexCall = tracker.indexCalls.some(
      (call) =>
        call.indexName === "by_status_attempts_scheduled_at" &&
        call.scheduledAtMax !== null &&
        call.filterFields.attempts === undefined
    );
    expect(invalidIndexCall).toBe(false);
  });

  it("releases stale processing rows before leasing pending ones", async () => {
    const now = Date.now();
    const stale: QueueRow = {
      _id: "stale-1",
      url: "https://example.com/stale",
      status: "pending",
      updatedAt: now - 25 * 60 * 1000,
      createdAt: now - 8 * 24 * 60 * 60 * 1000, // older than 7d to trigger ignore
      provider: "spidercloud",
      attempts: 1,
    };
    const pending: QueueRow = {
      _id: "pend-1",
      url: "https://example.com/pending",
      status: "pending",
      updatedAt: now - 1_000,
      createdAt: now - 30 * 60 * 1000,
      provider: "spidercloud",
      attempts: 0,
    };

    const db = new FakeDb([stale, pending]);
    const ctx: any = { db };

    const handler = getHandler(leaseScrapeUrlBatch);
    const res = await handler(ctx, {
      provider: "spidercloud",
      limit: 2,
      processingExpiryMs: 15 * 60 * 1000,
    });

    const leasedUrls = res.urls.map((u: any) => u.url);
    expect(leasedUrls).toContain("https://example.com/pending");
    expect(db.getIgnored().some((row) => row?.reason === "stale_scrape_queue_entry")).toBe(true);
  });

  it("marks stale listing rows with a listing reason", async () => {
    const now = Date.now();
    const listing: QueueRow = {
      _id: "listing-1",
      url: "https://example.com/jobs?page=3",
      status: "pending",
      updatedAt: now - 1_000,
      createdAt: now - 8 * 24 * 60 * 60 * 1000,
      provider: "spidercloud",
      attempts: 0,
      urlType: "listing",
    };
    const pending: QueueRow = {
      _id: "pend-1",
      url: "https://example.com/job/123",
      status: "pending",
      updatedAt: now - 1_000,
      createdAt: now - 30 * 60 * 1000,
      provider: "spidercloud",
      attempts: 0,
      urlType: "detail",
    };

    const db = new FakeDb([listing, pending]);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBatch);

    await handler(ctx, {
      provider: "spidercloud",
      limit: 2,
      processingExpiryMs: 15 * 60 * 1000,
    });

    const reasons = db.getIgnored().map((row) => row?.reason);
    expect(reasons).toContain("listing_stale_scrape_queue_entry");
  });

  it("leases unique rows across consecutive calls (multi-worker safety)", async () => {
    const now = Date.now();
    const rows: QueueRow[] = [
      {
        _id: "row-1",
        url: "https://example.com/job/1",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 10_000,
        provider: "spidercloud",
        attempts: 0,
      },
      {
        _id: "row-2",
        url: "https://example.com/job/2",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 10_000,
        provider: "spidercloud",
        attempts: 0,
      },
      {
        _id: "row-3",
        url: "https://example.com/job/3",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 10_000,
        provider: "spidercloud",
        attempts: 0,
      },
    ];
    const db = new FakeDb(rows);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBatch);

    const first = await handler(ctx, {
      provider: "spidercloud",
      limit: 2,
      processingExpiryMs: 15 * 60 * 1000,
    });
    const second = await handler(ctx, {
      provider: "spidercloud",
      limit: 2,
      processingExpiryMs: 15 * 60 * 1000,
    });

    const firstUrls = first.urls.map((u: any) => u.url);
    const secondUrls = second.urls.map((u: any) => u.url);

    expect(firstUrls.length).toBeGreaterThan(0);
    expect(new Set([...firstUrls, ...secondUrls]).size).toBe(firstUrls.length + secondUrls.length);
  });

  it("expires stale job detail rows after 48 hours", async () => {
    const now = Date.now();
    const expired: QueueRow = {
      _id: "expired-1",
      url: "https://example.com/job/expired",
      status: "pending",
      updatedAt: now - 1_000,
      createdAt: now - 49 * 60 * 60 * 1000,
      provider: "spidercloud",
      attempts: 0,
      urlType: "detail",
    };
    const pending: QueueRow = {
      _id: "pend-1",
      url: "https://example.com/job/fresh",
      status: "pending",
      updatedAt: now - 1_000,
      createdAt: now - 60 * 60 * 1000,
      provider: "spidercloud",
      attempts: 0,
      urlType: "detail",
    };

    const db = new FakeDb([expired, pending]);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBatch);

    const res = await handler(ctx, {
      provider: "spidercloud",
      limit: 2,
      processingExpiryMs: 15 * 60 * 1000,
    });

    const leasedUrls = res.urls.map((u: any) => u.url);
    expect(leasedUrls).toContain("https://example.com/job/fresh");
    expect(leasedUrls).not.toContain("https://example.com/job/expired");
    expect(expired.status).toBe("failed");
    expect(expired.lastError).toBe("fail_expired");
  });

  it("does not expire listing rows after 48 hours", async () => {
    const now = Date.now();
    const listing: QueueRow = {
      _id: "listing-1",
      url: "https://example.com/jobs?page=2",
      status: "pending",
      updatedAt: now - 1_000,
      createdAt: now - 49 * 60 * 60 * 1000,
      provider: "spidercloud",
      attempts: 0,
      urlType: "listing",
    };

    const db = new FakeDb([listing]);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBatch);

    const res = await handler(ctx, {
      provider: "spidercloud",
      limit: 1,
      processingExpiryMs: 15 * 60 * 1000,
    });

    const leasedUrls = res.urls.map((u: any) => u.url);
    expect(leasedUrls).toContain("https://example.com/jobs?page=2");
    expect(listing.status).toBe("processing");
  });

  it("prioritizes lowest attempts when leasing", async () => {
    const now = Date.now();
    const rows: QueueRow[] = [
      {
        _id: "row-high",
        url: "https://example.com/job/high",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 10_000,
        scheduledAt: now - 1_000,
        provider: "spidercloud",
        attempts: 2,
        siteId: "site-1",
      },
      {
        _id: "row-low",
        url: "https://example.com/job/low",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 10_000,
        scheduledAt: now - 1_000,
        provider: "spidercloud",
        attempts: 0,
        siteId: "site-1",
      },
      {
        _id: "row-mid",
        url: "https://example.com/job/mid",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 10_000,
        scheduledAt: now - 1_000,
        provider: "spidercloud",
        attempts: 1,
        siteId: "site-1",
      },
    ];

    const db = new FakeDb(rows);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBatch);

    const res = await handler(ctx, {
      provider: "spidercloud",
      limit: 2,
      processingExpiryMs: 15 * 60 * 1000,
    });

    const leasedUrls = res.urls.map((u: any) => u.url);
    expect(leasedUrls).toEqual([
      "https://example.com/job/low",
      "https://example.com/job/mid",
    ]);
  });

  it("prioritizes oldest createdAt when attempts match", async () => {
    const now = Date.now();
    const rows: QueueRow[] = [
      {
        _id: "row-newer",
        url: "https://example.com/job/newer",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 5_000,
        scheduledAt: now - 1_000,
        provider: "spidercloud",
        attempts: 0,
        siteId: "site-1",
      },
      {
        _id: "row-older",
        url: "https://example.com/job/older",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 20_000,
        scheduledAt: now - 1_000,
        provider: "spidercloud",
        attempts: 0,
        siteId: "site-1",
      },
    ];

    const db = new FakeDb(rows);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBatch);

    const res = await handler(ctx, {
      provider: "spidercloud",
      limit: 2,
      processingExpiryMs: 15 * 60 * 1000,
    });

    const leasedUrls = res.urls.map((u: any) => u.url);
    expect(leasedUrls).toEqual([
      "https://example.com/job/older",
      "https://example.com/job/newer",
    ]);
  });

  it("skips active processing rows to prevent double-leasing", async () => {
    const now = Date.now();
    const rows: QueueRow[] = [
      {
        _id: "processing-1",
        url: "https://example.com/job/processing",
        status: "processing",
        updatedAt: now - 1_000,
        createdAt: now - 10_000,
        provider: "spidercloud",
        attempts: 1,
      },
      {
        _id: "pending-1",
        url: "https://example.com/job/pending",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 10_000,
        provider: "spidercloud",
        attempts: 0,
      },
    ];
    const db = new FakeDb(rows);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBatch);

    const res = await handler(ctx, {
      provider: "spidercloud",
      limit: 2,
      processingExpiryMs: 15 * 60 * 1000,
    });

    const leasedUrls = res.urls.map((u: any) => u.url);
    expect(leasedUrls).toEqual(["https://example.com/job/pending"]);
    expect(rows.find((r) => r._id === "processing-1")?.status).toBe("processing");
  });

  it("leases distinct rows across six worker calls", async () => {
    const now = Date.now();
    const rows: QueueRow[] = Array.from({ length: 6 }, (_, idx) => ({
      _id: `row-${idx + 1}`,
      url: `https://example.com/job/${idx + 1}`,
      status: "pending",
      updatedAt: now - 1_000,
      createdAt: now - 10_000,
      provider: "spidercloud",
      attempts: 0,
    }));
    const db = new FakeDb(rows);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBatch);

    const leased: string[] = [];
    for (let i = 0; i < 6; i += 1) {
      const res = await handler(ctx, {
        provider: "spidercloud",
        limit: 1,
        processingExpiryMs: 15 * 60 * 1000,
      });
      const url = res.urls[0]?.url;
      if (url) leased.push(url);
    }

    expect(leased).toHaveLength(6);
    expect(new Set(leased).size).toBe(6);
    for (const row of rows) {
      expect(row.status).toBe("processing");
      expect(row.attempts).toBe(1);
    }
  });

  it("leases pending spidercloud rows for lambda-style URLs", async () => {
    const now = Date.now();
    const rows: QueueRow[] = [
      {
        _id: "lambda-1",
        url: "https://jobs.ashbyhq.com/lambda/2d656d6c-733f-4072-8bee-847f142c0938",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 5_000,
        provider: "spidercloud",
        attempts: 0,
      },
      {
        _id: "lambda-2",
        url: "https://jobs.ashbyhq.com/lambda/2d656d6c-733f-4072-8bee-847f142c0938/application",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 5_000,
        provider: "spidercloud",
        attempts: 0,
      },
    ];
    const db = new FakeDb(rows);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBatch);

    const res = await handler(ctx, {
      provider: "spidercloud",
      limit: 2,
      processingExpiryMs: 15 * 60 * 1000,
    });

    const leasedUrls = res.urls.map((u: any) => u.url);
    expect(leasedUrls).toEqual(rows.map((r) => r.url));
    expect(rows.every((r) => r.status === "processing")).toBe(true);
    expect(rows.every((r) => (r.attempts ?? 0) === 1)).toBe(true);
  });

  it("skips scheduled rows until their scheduledAt time", async () => {
    const now = Date.now();
    const rows: QueueRow[] = [
      {
        _id: "future-1",
        url: "https://example.com/job/future",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 10_000,
        scheduledAt: now + 60_000,
        provider: "spidercloud",
        attempts: 0,
      },
      {
        _id: "ready-1",
        url: "https://example.com/job/ready",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 10_000,
        scheduledAt: now - 1_000,
        provider: "spidercloud",
        attempts: 0,
      },
    ];
    const db = new FakeDb(rows);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBatch);

    const res = await handler(ctx, {
      provider: "spidercloud",
      limit: 2,
      processingExpiryMs: 15 * 60 * 1000,
    });

    const urls = res.urls.map((u: any) => u.url);
    expect(urls).toContain("https://example.com/job/ready");
    expect(urls).not.toContain("https://example.com/job/future");
  });

  it("skips rows when provider does not match the lease filter", async () => {
    const now = Date.now();
    const rows: QueueRow[] = [
      {
        _id: "row-1",
        url: "https://jobs.ashbyhq.com/lambda/abc",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 5_000,
        provider: "fetchfox",
        attempts: 0,
      },
      {
        _id: "row-2",
        url: "https://jobs.ashbyhq.com/lambda/def",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 5_000,
        provider: undefined,
        attempts: 0,
      },
    ];
    const db = new FakeDb(rows);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBatch);

    const res = await handler(ctx, {
      provider: "spidercloud",
      limit: 5,
      processingExpiryMs: 15 * 60 * 1000,
    });

    expect(res.urls).toEqual([]);
    expect(rows.every((r) => r.status === "pending")).toBe(true);
    expect(rows.every((r) => (r.attempts ?? 0) === 0)).toBe(true);
  });

  it("requeues stale processing rows and leases them for retry", async () => {
    const now = Date.now();
    const rows: QueueRow[] = [
      {
        _id: "netflix-1",
        url: "https://explore.jobs.netflix.net/careers/job/790313345439",
        sourceUrl:
          "https://explore.jobs.netflix.net/careers?query=engineer&pid=790313345439&Region=ucan&domain=netflix.com&sort_by=date",
        status: "processing",
        updatedAt: now - 31 * 60 * 1000,
        createdAt: now - 31 * 60 * 1000,
        scheduledAt: now - 1_000,
        provider: "spidercloud",
        attempts: 0,
      },
    ];
    const db = new FakeDb(rows);
    const ctx: any = { db };
    const requeueHandler = getHandler(requeueStaleScrapeUrls);
    const leaseHandler = getHandler(leaseScrapeUrlBatch);

    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(now);
    const requeueRes = await requeueHandler(ctx, {
      provider: "spidercloud",
      processingExpiryMs: 15 * 60 * 1000,
    });
    const res = await leaseHandler(ctx, {
      provider: "spidercloud",
      limit: 1,
    });
    nowSpy.mockRestore();

    expect(requeueRes.requeued).toBe(1);
    expect(res.urls.map((u: any) => u.url)).toEqual([rows[0].url]);
    expect(rows[0].status).toBe("processing");
    expect(rows[0].attempts).toBe(1);
  });

  it("requeues stale processing rows with bucket and scheduledAt populated", async () => {
    const now = Date.now();
    const rows: QueueRow[] = [
      {
        _id: "stale-2",
        url: "https://example.com/job/stale",
        status: "processing",
        updatedAt: now - 31 * 60 * 1000,
        createdAt: now - 31 * 60 * 1000,
        provider: "spidercloud",
        attempts: 1,
      },
    ];
    const db = new FakeDb(rows);
    const ctx: any = { db };
    const handler = getHandler(requeueStaleScrapeUrls);

    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(now);
    const res = await handler(ctx, {
      provider: "spidercloud",
      processingExpiryMs: 15 * 60 * 1000,
    });
    nowSpy.mockRestore();

    expect(res.requeued).toBe(1);
    expect(rows[0].status).toBe("pending");
    expect(typeof rows[0].bucket).toBe("number");
    expect(rows[0].scheduledAt).toBe(now);
  });

  it("round-robins across site buckets when leasing", async () => {
    const now = Date.now();
    const rows: QueueRow[] = [
      {
        _id: "site-a-1",
        url: "https://site-a.example/job/1",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 5_000,
        provider: "spidercloud",
        attempts: 0,
        siteId: "site-a",
      },
      {
        _id: "site-a-2",
        url: "https://site-a.example/job/2",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 5_000,
        provider: "spidercloud",
        attempts: 0,
        siteId: "site-a",
      },
      {
        _id: "site-b-1",
        url: "https://site-b.example/job/1",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 5_000,
        provider: "spidercloud",
        attempts: 0,
        siteId: "site-b",
      },
    ];
    const db = new FakeDb(rows);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBatch);

    const res = await handler(ctx, {
      provider: "spidercloud",
      limit: 3,
      processingExpiryMs: 15 * 60 * 1000,
    });

    const leasedSiteIds = res.urls.map((u: any) => u.siteId);
    expect(leasedSiteIds).toContain("site-a");
    expect(leasedSiteIds).toContain("site-b");
  });

  it("leases only from provided buckets", async () => {
    const now = Date.now();
    const rows: QueueRow[] = [
      {
        _id: "row-1",
        url: "https://example.com/job/1",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 5_000,
        provider: "spidercloud",
        attempts: 0,
        bucket: 0,
      },
      {
        _id: "row-2",
        url: "https://example.com/job/2",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 5_000,
        provider: "spidercloud",
        attempts: 0,
        bucket: 1,
      },
      {
        _id: "row-3",
        url: "https://example.com/job/3",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 5_000,
        provider: "spidercloud",
        attempts: 0,
        bucket: 2,
      },
      {
        _id: "row-4",
        url: "https://example.com/job/4",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 5_000,
        provider: "spidercloud",
        attempts: 0,
        bucket: 3,
      },
    ];
    const db = new FakeDb(rows);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBatch);

    const res = await handler(ctx, {
      provider: "spidercloud",
      limit: 2,
      buckets: [1, 3],
    });

    const ownedBuckets = new Set([1, 3]);
    expect(res.urls.length).toBeGreaterThan(0);
    for (const leased of res.urls) {
      const row = rows.find((r) => r.url === leased.url);
      expect(row?.bucket).toBeDefined();
      expect(ownedBuckets.has(row?.bucket ?? -1)).toBe(true);
    }
  });

  it("leases rows missing bucket when explicit buckets are provided", async () => {
    const now = Date.now();
    const row: QueueRow = {
      _id: "row-missing-bucket",
      url: "https://example.com/job/1",
      sourceUrl: "https://example.com/jobs",
      status: "pending",
      updatedAt: now - 1_000,
      createdAt: now - 5_000,
      scheduledAt: now - 500,
      provider: "spidercloud",
      attempts: 0,
    };
    const derivedBucket = deriveScrapeQueueBucket({
      url: row.url,
      sourceUrl: row.sourceUrl ?? null,
      siteId: row.siteId ?? null,
    });
    const db = new FakeDb([row]);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBatch);

    const res = await handler(ctx, {
      provider: "spidercloud",
      limit: 1,
      buckets: [derivedBucket],
    });

    expect(res.urls).toHaveLength(1);
  });

  it("leases rows missing scheduledAt when explicit buckets are provided", async () => {
    const now = Date.now();
    const row: QueueRow = {
      _id: "row-missing-scheduled",
      url: "https://example.com/job/2",
      sourceUrl: "https://example.com/jobs",
      status: "pending",
      updatedAt: now - 1_000,
      createdAt: now - 5_000,
      provider: "spidercloud",
      attempts: 0,
    };
    const derivedBucket = deriveScrapeQueueBucket({
      url: row.url,
      sourceUrl: row.sourceUrl ?? null,
      siteId: row.siteId ?? null,
    });
    row.bucket = derivedBucket;
    const db = new FakeDb([row]);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBatch);

    const res = await handler(ctx, {
      provider: "spidercloud",
      limit: 1,
      buckets: [derivedBucket],
    });

    expect(res.urls).toHaveLength(1);
  });

  it("backfills bucket when missing", async () => {
    const now = Date.now();
    const rows: QueueRow[] = [
      {
        _id: "row-1",
        url: "https://example.com/job/1",
        status: "pending",
        updatedAt: now - 1_000,
        createdAt: now - 5_000,
        provider: "spidercloud",
        attempts: 0,
      },
    ];
    const db = new FakeDb(rows);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBatch);

    await handler(ctx, {
      provider: "spidercloud",
      limit: 1,
    });

    expect(typeof rows[0].bucket).toBe("number");
  });
});

describe("leaseScrapeUrlBuckets", () => {
  it("claims more buckets when other workers expire", async () => {
    const now = Date.now();
    const heartbeats = [
      { _id: "hb-1", workerId: "worker-1", updatedAt: now, expiresAt: now + 60_000 },
      { _id: "hb-2", workerId: "worker-2", updatedAt: now, expiresAt: now + 60_000 },
      { _id: "hb-3", workerId: "worker-3", updatedAt: now, expiresAt: now + 60_000 },
      { _id: "hb-4", workerId: "worker-4", updatedAt: now, expiresAt: now + 60_000 },
    ];
    const db = new FakeDb([], [], [], [], [], heartbeats);
    const ctx: any = { db };
    const handler = getHandler(leaseScrapeUrlBuckets);

    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(now);
    await handler(ctx, { workerId: "worker-1" });
    const ownedBefore = db
      .getBucketLeases()
      .filter((row: any) => row.workerId === "worker-1" && row.expiresAt > now).length;

    const nowLater = now + 2_000;
    for (const heartbeat of db.getHeartbeats()) {
      if (heartbeat.workerId !== "worker-1") {
        heartbeat.expiresAt = nowLater - 1;
      }
    }
    nowSpy.mockReturnValue(nowLater);
    await handler(ctx, { workerId: "worker-1" });
    nowSpy.mockRestore();

    const ownedAfter = db
      .getBucketLeases()
      .filter((row: any) => row.workerId === "worker-1" && row.expiresAt > nowLater).length;
    expect(ownedAfter).toBeGreaterThan(ownedBefore);
  });
});

describe("completeScrapeUrls", () => {
  it("marks failed rows for retry without ejecting", async () => {
    const now = Date.now();
    const rows: QueueRow[] = [
      {
        _id: "netflix-1",
        url: "https://explore.jobs.netflix.net/careers/job/790313345439",
        sourceUrl:
          "https://explore.jobs.netflix.net/careers?query=engineer&pid=790313345439&Region=ucan&domain=netflix.com&sort_by=date",
        status: "processing",
        updatedAt: now - 1_000,
        createdAt: now - 5_000,
        provider: "spidercloud",
        attempts: 1,
      },
    ];
    const db = new FakeDb(rows);
    const ctx: any = { db };
    const handler = getHandler(completeScrapeUrls);

    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(now);
    const res = await handler(ctx, {
      urls: [rows[0].url],
      status: "failed",
      error: "timeout",
    });
    nowSpy.mockRestore();

    expect(res.updated).toBe(1);
    expect(rows[0].status).toBe("failed");
    expect(rows[0].attempts).toBe(1);
    expect(rows[0].lastError).toBe("timeout");
    expect(rows[0].completedAt).toBeUndefined();
    expect(db.getIgnored()).toHaveLength(0);
  });

  it("retries failed rows after reset and leases again", async () => {
    const now = Date.now();
    const row: QueueRow = {
      _id: "netflix-2",
      url: "https://explore.jobs.netflix.net/careers/job/790313323421",
      sourceUrl:
        "https://explore.jobs.netflix.net/careers?query=engineer&pid=790313345439&Region=ucan&domain=netflix.com&sort_by=date",
      status: "failed",
      updatedAt: now - 1_000,
      createdAt: now - 5_000,
      provider: "spidercloud",
      attempts: 1,
    };
    const db = new FakeDb([row]);
    const ctx: any = { db };
    const resetHandler = getHandler(resetScrapeUrlsByStatus);
    const leaseHandler = getHandler(leaseScrapeUrlBatch);

    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(now);
    await resetHandler(ctx, {
      provider: "spidercloud",
      status: "failed",
      limit: 50,
    });
    expect(typeof row.bucket).toBe("number");
    expect(row.scheduledAt).toBe(now);
    const res = await leaseHandler(ctx, {
      provider: "spidercloud",
      limit: 1,
      processingExpiryMs: 15 * 60 * 1000,
    });
    nowSpy.mockRestore();

    expect(res.urls.map((u: any) => u.url)).toEqual([row.url]);
    expect(row.status).toBe("processing");
    expect(row.attempts).toBe(2);
  });

  it("ejects rows after max attempts", async () => {
    const now = Date.now();
    const rows: QueueRow[] = [
      {
        _id: "netflix-3",
        url: "https://explore.jobs.netflix.net/careers/job/790313310792",
        sourceUrl:
          "https://explore.jobs.netflix.net/careers?query=engineer&pid=790313345439&Region=ucan&domain=netflix.com&sort_by=date",
        status: "processing",
        updatedAt: now - 1_000,
        createdAt: now - 5_000,
        provider: "spidercloud",
        attempts: 3,
      },
    ];
    const db = new FakeDb(rows);
    const ctx: any = { db };
    const handler = getHandler(completeScrapeUrls);

    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(now);
    const res = await handler(ctx, {
      urls: [rows[0].url],
      status: "failed",
      error: "timeout",
    });
    nowSpy.mockRestore();

    expect(res.updated).toBe(1);
    expect(rows).toHaveLength(0);
    expect(db.getIgnored()[0]?.reason).toBe("max_attempts");
    expect(db.getSeen()).toHaveLength(1);
  });
});

describe("requeueStaleScrapeUrls", () => {
  it("requeues stale processing rows and skips fresh ones", async () => {
    const now = Date.now();
    const rows: QueueRow[] = [
      {
        _id: "stale-1",
        url: "https://example.com/stale",
        status: "processing",
        updatedAt: now - 30 * 60 * 1000,
        createdAt: now - 2 * 60 * 60 * 1000,
        provider: "spidercloud",
      },
      {
        _id: "fresh-1",
        url: "https://example.com/fresh",
        status: "processing",
        updatedAt: now - 2 * 60 * 1000,
        createdAt: now - 10 * 60 * 1000,
        provider: "spidercloud",
      },
    ];
    const db = new FakeDb(rows);
    const ctx: any = { db };
    const handler = getHandler(requeueStaleScrapeUrls);

    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(now);
    const res = await handler(ctx, {
      provider: "spidercloud",
      processingExpiryMs: 15 * 60 * 1000,
    });
    nowSpy.mockRestore();

    expect(res.requeued).toBe(1);
    expect(rows.find((r) => r._id === "stale-1")?.status).toBe("pending");
    expect(rows.find((r) => r._id === "fresh-1")?.status).toBe("processing");
  });

  it("skips rows when provider does not match", async () => {
    const now = Date.now();
    const rows: QueueRow[] = [
      {
        _id: "stale-1",
        url: "https://example.com/stale",
        status: "processing",
        updatedAt: now - 30 * 60 * 1000,
        createdAt: now - 2 * 60 * 60 * 1000,
        provider: "fetchfox",
      },
    ];
    const db = new FakeDb(rows);
    const ctx: any = { db };
    const handler = getHandler(requeueStaleScrapeUrls);

    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(now);
    const res = await handler(ctx, {
      provider: "spidercloud",
      processingExpiryMs: 15 * 60 * 1000,
    });
    nowSpy.mockRestore();

    expect(res.requeued).toBe(0);
    expect(rows[0].status).toBe("processing");
  });
});
