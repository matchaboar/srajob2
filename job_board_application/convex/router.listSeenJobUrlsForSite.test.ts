import { describe, expect, it } from "vitest";
import { listSeenJobUrlsForSite } from "./router";
import { getHandler } from "./__tests__/getHandler";

type Row = { sourceUrl: string; url?: string; reason?: string };

class FakeQuery {
  constructor(
    private rows: Row[],
    private filters: { sourceUrl?: string; url?: string } = {}
  ) {}
  withIndex(_name: string, cb: (q: any) => any) {
    const filters = { ...this.filters };
    const builder = {
      eq: (field: string, val: string) => {
        filters[field as "sourceUrl" | "url"] = val;
        return builder;
      },
    };
    cb(builder);
    return new FakeQuery(this.rows, filters);
  }
  first() {
    return this.collect()[0] ?? null;
  }
  collect() {
    return this.rows.filter((row) => {
      if (this.filters.sourceUrl && row.sourceUrl !== this.filters.sourceUrl) {
        return false;
      }
      if (this.filters.url && row.url !== this.filters.url) {
        return false;
      }
      return true;
    });
  }
}

describe("listSeenJobUrlsForSite", () => {
  it("includes ignored non-listing URLs but skips listing-tagged ignores", async () => {
    const sourceUrl = "https://careers.confluent.io/jobs";
    const ignoredUrls = [
      "https://careers.confluent.io/jobs/united_states-united_arab_emirates",
      "https://careers.confluent.io/jobs/united_states-thailand",
      "https://careers.confluent.io/jobs/united_states-finance_&_operations",
    ];
    const listingIgnoredUrl = "https://careers.confluent.io/jobs/united_states-some_team";

    const seenRows: Row[] = [
      { sourceUrl, url: "https://careers.confluent.io/jobs/123" },
    ];

    const ignored: Row[] = [
      ...ignoredUrls.map((url) => ({ sourceUrl, url })),
      { sourceUrl, url: listingIgnoredUrl, reason: "listing_stale_scrape_queue_entry" },
    ];

    const ctx: any = {
      db: {
        query: (table: string) => {
          if (table === "seen_job_urls") {
            // Simulate the backend logic where non-listing ignored jobs are also inserted into seen_job_urls
            const effectiveSeen = [
              ...seenRows,
              ...ignoredUrls.map((url) => ({ sourceUrl, url })),
            ];
            return new FakeQuery(effectiveSeen);
          }
          if (table === "seen_job_url_index") {
            return new FakeQuery([]);
          }
          // The function should no longer query ignored_jobs
          if (table === "ignored_jobs") throw new Error("Should not query ignored_jobs");
          throw new Error(`Unexpected table ${table}`);
        },
      },
    };

    const handler = getHandler(listSeenJobUrlsForSite);
    const res = await handler(ctx, { sourceUrl });

    expect(res.urls).toEqual(
      expect.arrayContaining([
        "https://careers.confluent.io/jobs/123",
        ...ignoredUrls,
      ])
    );
    expect(res.urls).not.toContain(listingIgnoredUrl);
  });

  it("returns only matching candidate URLs using the index with fallback", async () => {
    const sourceUrl = "https://example.com/jobs";
    const ctx: any = {
      db: {
        query: (table: string) => {
          if (table === "seen_job_url_index") {
            return new FakeQuery([{ sourceUrl, url: "https://example.com/jobs/1" }]);
          }
          if (table === "seen_job_urls") {
            return new FakeQuery([{ sourceUrl, url: "https://example.com/jobs/2" }]);
          }
          throw new Error(`Unexpected table ${table}`);
        },
      },
    };

    const handler = getHandler(listSeenJobUrlsForSite);
    const res = await handler(ctx, {
      sourceUrl,
      urls: [
        "https://example.com/jobs/1",
        "https://example.com/jobs/2",
        "https://example.com/jobs/3",
        " ",
        "https://example.com/jobs/1",
      ],
    });

    expect(res.urls).toEqual([
      "https://example.com/jobs/1",
      "https://example.com/jobs/2",
    ]);
  });

  it("returns empty list for blank candidate URLs without hitting the db", async () => {
    const ctx: any = {
      db: {
        query: () => {
          throw new Error("Should not query db");
        },
      },
    };

    const handler = getHandler(listSeenJobUrlsForSite);
    const res = await handler(ctx, { sourceUrl: "https://example.com/jobs", urls: [" ", ""] });
    expect(res.urls).toEqual([]);
  });
});
