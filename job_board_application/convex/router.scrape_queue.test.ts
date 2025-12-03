import { describe, expect, it, vi } from "vitest";
import { leaseScrapeUrlBatch } from "./router";

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
  constructor(private rows: QueueRow[], private filterStatus: string | null = null) {}
  withIndex(_name: string, cb: (q: any) => any) {
    const status = cb({ eq: (_field: string, val: string) => val });
    return new FakeQuery(this.rows, status);
  }
  order() {
    return this;
  }
  take(n: number) {
    const rows = this.filterStatus ? this.rows.filter((r) => r.status === this.filterStatus) : this.rows;
    return rows.slice(0, n);
  }
  collect() {
    return [];
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

    const patches: Array<{ id: string; updates: any }> = [];
    const ignored: Array<any> = [];
    const ctx: any = {
      db: {
        query: (table: string) => {
          if (table === "scrape_url_queue") {
            return new FakeQuery([stale, pending]);
          }
          if (table === "job_detail_rate_limits") {
            return { collect: () => [] };
          }
          throw new Error(`Unexpected table ${table}`);
        },
        insert: vi.fn((table: string, payload: any) => {
          if (table === "ignored_jobs") {
            ignored.push(payload);
          }
          return "id";
        }),
        patch: vi.fn((id: string, updates: any) => {
          patches.push({ id, updates });
        }),
      },
    };

    const handler = (leaseScrapeUrlBatch as any).handler ?? leaseScrapeUrlBatch;
    const res = await handler(ctx, {
      provider: "spidercloud",
      limit: 2,
      processingExpiryMs: 15 * 60 * 1000,
    });

    const leasedUrls = res.urls.map((u: any) => u.url);
    expect(leasedUrls).toContain("https://example.com/pending");
    expect(ignored.some((row) => row?.reason === "stale_scrape_queue_entry")).toBe(true);
  });
});
