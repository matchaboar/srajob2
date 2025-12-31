import { describe, expect, it, vi } from "vitest";
import { getHandler } from "./__tests__/getHandler";

vi.mock("@convex-dev/auth/server", () => ({
  getAuthUserId: () => "user-1",
}));

import { listQueuedJobs } from "./jobs";

type QueueRow = {
  _id: string;
  url: string;
  sourceUrl: string;
  provider?: string;
  siteId?: string;
  pattern?: string;
  status: "pending" | "processing" | "completed" | "failed" | "invalid";
  attempts?: number;
  lastError?: string;
  createdAt: number;
  updatedAt: number;
  completedAt?: number;
  scheduledAt?: number;
};

type QueuePage = {
  page: QueueRow[];
  isDone: boolean;
  continueCursor: string | null;
  pageStatus?: string | null;
  splitCursor?: string;
};

class FakeScrapeQueueQuery {
  constructor(
    private readonly pageReturn: QueuePage,
    private readonly tracker: { lastIndexName: string | null; filterCalls: number; orderCalls: number }
  ) {}

  withIndex(name: string, cb?: (q: any) => any) {
    this.tracker.lastIndexName = name;
    if (cb) {
      const builder = {
        eq: (_field: string, _value: unknown) => builder,
        lte: (_field: string, _value: unknown) => builder,
        or: (..._args: unknown[]) => builder,
        field: (field: string) => field,
      };
      cb(builder);
    }
    return this;
  }

  filter(_cb: (q: any) => any) {
    this.tracker.filterCalls += 1;
    return this;
  }

  order(_dir: string) {
    this.tracker.orderCalls += 1;
    return this;
  }

  paginate(_opts: { cursor?: string | null; numItems?: number }) {
    return this.pageReturn;
  }
}

describe("listQueuedJobs pagination", () => {
  it("returns only the expected pagination fields", async () => {
    const row: QueueRow = {
      _id: "queue-1",
      url: "https://example.com/detail/1",
      sourceUrl: "https://example.com/jobs",
      provider: "spidercloud",
      pattern: "https://example.com/detail/**",
      status: "pending",
      attempts: 0,
      createdAt: 1700000000000,
      updatedAt: 1700000000001,
      scheduledAt: 1700000001000,
    };
    const pageReturn: QueuePage = {
      page: [row],
      isDone: false,
      continueCursor: "cursor-1",
      pageStatus: null,
      splitCursor: "split-1",
    };
    const tracker = { lastIndexName: null, filterCalls: 0, orderCalls: 0 };
    const ctx: any = {
      db: {
        query: (table: string) => {
          if (table !== "scrape_url_queue") throw new Error(`unexpected table ${table}`);
          return new FakeScrapeQueueQuery(pageReturn, tracker);
        },
      },
    };
    const handler = getHandler(listQueuedJobs);

    const result = await handler(ctx, {
      paginationOpts: { cursor: null, numItems: 5 },
      status: "pending",
    });

    expect(Object.keys(result).sort()).toEqual(["continueCursor", "isDone", "page"]);
    expect((result as any).pageStatus).toBeUndefined();
    expect((result as any).splitCursor).toBeUndefined();
    expect(result.page).toHaveLength(1);
    expect(result.page[0]).toMatchObject({
      _id: row._id,
      url: row.url,
      sourceUrl: row.sourceUrl,
      status: row.status,
    });
    expect(tracker.lastIndexName).toBe("by_status");
    expect(tracker.filterCalls).toBe(1);
    expect(tracker.orderCalls).toBe(1);
  });
});
