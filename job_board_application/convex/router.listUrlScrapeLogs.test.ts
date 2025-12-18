import { describe, expect, it } from "vitest";
import { listUrlScrapeLogs } from "./router";
import { getHandler } from "./__tests__/getHandler";

type Scrape = {
  _id: string;
  _creationTime: number;
  sourceUrl: string;
  provider?: string;
  workflowName?: string;
  workflowType?: string;
  completedAt?: number;
  startedAt?: number;
  items?: any;
};

class FakeScrapeQuery {
  constructor(private readonly rows: Scrape[]) {}

  withIndex(name: string, _cb: (q: any) => any) {
    if (name !== "by_source" && name !== "by_site") {
      throw new Error(`unexpected index ${name}`);
    }
    return this;
  }

  order(_dir: string) {
    return this;
  }

  take(n: number) {
    return this.rows.slice(0, n);
  }
}

class FakeJobsQuery {
  withIndex(name: string, cb: (q: any) => any) {
    if (name !== "by_url") throw new Error(`unexpected jobs index ${name}`);
    cb({ eq: (_field: string, val: any) => val });
    return this;
  }

  first() {
    return null;
  }
}

const buildCtx = (scrapes: Scrape[]) => ({
  db: {
    query: (table: string) => {
      if (table === "scrapes") return new FakeScrapeQuery(scrapes);
      if (table === "jobs") return new FakeJobsQuery();
      throw new Error(`unexpected table ${table}`);
    },
  },
});

describe("listUrlScrapeLogs", () => {
  it("avoids unsupported indexes while still returning latest logs", async () => {
    const scrapes: Scrape[] = [
      {
        _id: "scrape-new",
        _creationTime: 2,
        sourceUrl: "https://example.com/jobs",
        provider: "firecrawl",
        completedAt: 2,
        items: { normalized: [{ url: "https://example.com/jobs/1" }] },
      },
      {
        _id: "scrape-old",
        _creationTime: 1,
        sourceUrl: "https://example.com/jobs",
        provider: "firecrawl",
        completedAt: 1,
        items: { raw: { job_urls: ["https://example.com/jobs/old"] } },
      },
    ];

    const ctx = buildCtx(scrapes);
    const handler = getHandler(listUrlScrapeLogs) as any;

    const logs = await handler(ctx, { limit: 5 });

    expect(logs).toHaveLength(2);
    expect(logs[0].url).toContain("/jobs/1");
    expect(logs[1].url).toContain("/jobs/old");
  });
});
