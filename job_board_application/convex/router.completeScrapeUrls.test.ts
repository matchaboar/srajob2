import { describe, expect, it, vi } from "vitest";
import { completeScrapeUrls } from "./router";
import { getHandler } from "./__tests__/getHandler";

type QueueRow = {
  _id: string;
  url: string;
  status: string;
  attempts?: number;
  sourceUrl?: string;
  provider?: string;
  siteId?: string;
};

describe("completeScrapeUrls", () => {
  it("ignores and deletes rows after max attempts", async () => {
    const now = Date.now();
    const queue: QueueRow = {
      _id: "q1",
      url: "https://example.com/job/1",
      status: "processing",
      attempts: 2,
      sourceUrl: "https://example.com/jobs",
      provider: "spidercloud",
      siteId: "s1",
    };
    const patches: any[] = [];
    const inserts: any[] = [];
    const deletes: any[] = [];
    const ctx: any = {
      db: {
        query: () => ({
          withIndex: () => ({
            first: () => queue,
          }),
        }),
        patch: vi.fn((id: string, updates: any) => patches.push({ id, updates })),
        insert: vi.fn((table: string, payload: any) => {
          inserts.push({ table, payload });
          return "ignored-id";
        }),
        delete: vi.fn((id: string) => deletes.push(id)),
      },
    };

    vi.setSystemTime(now);
    const handler = getHandler(completeScrapeUrls);
    await handler(ctx, { urls: [queue.url], status: "failed", error: "timeout" });
    vi.useRealTimers();

    expect(inserts[0]?.table).toBe("ignored_jobs");
    expect(inserts[0]?.payload.reason).toBe("max_attempts");
    expect(deletes).toContain("q1");
    expect(patches).toHaveLength(0);
  });

  it("ignores 404 failures immediately", async () => {
    const now = Date.now();
    const queue: QueueRow = {
      _id: "q404",
      url: "https://example.com/job/404",
      status: "processing",
      attempts: 0,
      sourceUrl: "https://example.com/jobs",
      provider: "spidercloud",
      siteId: "s1",
    };
    const patches: any[] = [];
    const inserts: any[] = [];
    const deletes: any[] = [];
    const ctx: any = {
      db: {
        query: () => ({
          withIndex: () => ({
            first: () => queue,
          }),
        }),
        patch: vi.fn((id: string, updates: any) => patches.push({ id, updates })),
        insert: vi.fn((table: string, payload: any) => {
          inserts.push({ table, payload });
          return "ignored-id";
        }),
        delete: vi.fn((id: string) => deletes.push(id)),
      },
    };

    vi.setSystemTime(now);
    const handler = getHandler(completeScrapeUrls);
    await handler(ctx, { urls: [queue.url], status: "failed", error: "404 not found" });
    vi.useRealTimers();

    expect(inserts[0]?.payload.reason).toBe("http_404");
    expect(deletes).toContain("q404");
    expect(patches).toHaveLength(0);
  });
});
