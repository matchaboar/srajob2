import { describe, expect, it } from "vitest";
import { listSeenJobUrlsForSite } from "./router";
import { getHandler } from "./__tests__/getHandler";

type Row = { sourceUrl: string; url?: string; reason?: string };

class FakeQuery {
  constructor(private rows: Row[], private sourceUrl: string | null = null) {}
  withIndex(_name: string, cb: (q: any) => any) {
    const sourceUrl = cb({ eq: (_field: string, val: string) => val });
    return new FakeQuery(this.rows, sourceUrl);
  }
  collect() {
    if (!this.sourceUrl) return this.rows;
    return this.rows.filter((row) => row.sourceUrl === this.sourceUrl);
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
          if (table === "seen_job_urls") return new FakeQuery(seenRows);
          if (table === "ignored_jobs") return new FakeQuery(ignored);
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
});
