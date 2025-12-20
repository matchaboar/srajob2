import { describe, expect, it, vi } from "vitest";
import { leaseScrapeUrlBatch } from "./router";
import { getHandler } from "./__tests__/getHandler";

type QueueRow = {
  _id: string;
  url: string;
  status: string;
  updatedAt: number;
  createdAt?: number;
  provider?: string;
  attempts?: number;
};

class FakeQuery {
  constructor(
    private getRows: () => QueueRow[],
    private filterStatus: string | null = null
  ) {}
  withIndex(_name: string, cb: (q: any) => any) {
    const status = cb({ eq: (_field: string, val: string) => val });
    return new FakeQuery(this.getRows, status);
  }
  order() {
    return this;
  }
  take(n: number) {
    const rows = this.getRows();
    const filtered = this.filterStatus ? rows.filter((r) => r.status === this.filterStatus) : rows;
    return filtered.slice(0, n);
  }
  collect() {
    return this.getRows();
  }
}

class FakeDb {
  constructor(
    private queueRows: QueueRow[],
    private rateLimitRows: Array<any> = [],
    private ignoredRows: Array<any> = []
  ) {}
  query = (table: string) => {
    if (table === "scrape_url_queue") {
      return new FakeQuery(() => this.queueRows);
    }
    if (table === "job_detail_rate_limits") {
      return new FakeQuery(() => this.rateLimitRows);
    }
    throw new Error(`Unexpected table ${table}`);
  };
  insert = vi.fn((table: string, payload: any) => {
    if (table === "ignored_jobs") {
      this.ignoredRows.push(payload);
      return "ignored-id";
    }
    if (table === "job_detail_rate_limits") {
      this.rateLimitRows.push({ _id: `rl-${this.rateLimitRows.length + 1}`, ...payload });
      return this.rateLimitRows[this.rateLimitRows.length - 1]._id;
    }
    throw new Error(`Unexpected insert table ${table}`);
  });
  patch = vi.fn((id: string, updates: any) => {
    const row = this.queueRows.find((r) => r._id === id);
    if (row) {
      Object.assign(row, updates);
      return;
    }
    const rate = this.rateLimitRows.find((r) => r._id === id);
    if (rate) {
      Object.assign(rate, updates);
      return;
    }
    throw new Error(`Unknown id ${id}`);
  });
  getIgnored() {
    return this.ignoredRows;
  }
}

describe("leaseScrapeUrlBatch", () => {
  it("releases stale processing rows before leasing pending ones", async () => {
    const now = Date.now();
    const stale: QueueRow = {
      _id: "stale-1",
      url: "https://example.com/stale",
      status: "pending",
      updatedAt: now - 25 * 60 * 1000,
      createdAt: now - 49 * 60 * 60 * 1000, // older than 48h to trigger ignore
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
});
