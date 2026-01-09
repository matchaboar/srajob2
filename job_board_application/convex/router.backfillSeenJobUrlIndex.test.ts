import { describe, expect, it } from "vitest";
import { backfillSeenJobUrlIndex } from "./router";
import { getHandler } from "./__tests__/getHandler";

type SeenRow = { _id: string; sourceUrl: string; url: string; createdAt?: number };
type IndexRow = {
  _id: string;
  sourceUrl: string;
  url: string;
  seenJobUrlId?: string;
  createdAt?: number;
};

class SeenQuery {
  constructor(private rows: SeenRow[]) {}
  paginate({ cursor, numItems }: { cursor?: string | null; numItems?: number }) {
    const start = cursor ? Number(cursor) : 0;
    const size = numItems ?? this.rows.length;
    const page = this.rows.slice(start, start + size);
    const next = start + page.length;
    return {
      page,
      isDone: next >= this.rows.length,
      continueCursor: next >= this.rows.length ? null : String(next),
    };
  }
}

class IndexQuery {
  constructor(
    private rows: IndexRow[],
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
    return new IndexQuery(this.rows, filters);
  }
  first() {
    return (
      this.rows.find((row) => {
        if (this.filters.sourceUrl && row.sourceUrl !== this.filters.sourceUrl) {
          return false;
        }
        if (this.filters.url && row.url !== this.filters.url) {
          return false;
        }
        return true;
      }) ?? null
    );
  }
}

class FakeDb {
  constructor(
    private seenRows: SeenRow[],
    private indexRows: IndexRow[]
  ) {}
  query = (table: string) => {
    if (table === "seen_job_urls") {
      return new SeenQuery(this.seenRows);
    }
    if (table === "seen_job_url_index") {
      return new IndexQuery(this.indexRows);
    }
    throw new Error(`Unexpected table ${table}`);
  };
  insert = async (table: string, payload: any) => {
    if (table !== "seen_job_url_index") {
      throw new Error(`Unexpected insert table ${table}`);
    }
    const _id = `index-${this.indexRows.length + 1}`;
    this.indexRows.push({ _id, ...payload });
    return _id;
  };
}

describe("backfillSeenJobUrlIndex", () => {
  it("inserts missing index rows and trims values", async () => {
    const seenRows: SeenRow[] = [
      {
        _id: "seen-1",
        sourceUrl: " https://example.com/jobs ",
        url: " https://example.com/jobs/1 ",
        createdAt: 100,
      },
      {
        _id: "seen-2",
        sourceUrl: "https://example.com/jobs",
        url: "https://example.com/jobs/2",
        createdAt: 200,
      },
      { _id: "seen-3", sourceUrl: "", url: "https://example.com/jobs/3", createdAt: 300 },
      { _id: "seen-4", sourceUrl: "https://example.com/jobs", url: "", createdAt: 400 },
      {
        _id: "seen-5",
        sourceUrl: "https://example.com/jobs",
        url: "https://example.com/jobs/1",
        createdAt: 500,
      },
    ];
    const indexRows: IndexRow[] = [
      {
        _id: "index-1",
        sourceUrl: "https://example.com/jobs",
        url: "https://example.com/jobs/1",
        seenJobUrlId: "seen-1",
        createdAt: 100,
      },
    ];

    const ctx: any = { db: new FakeDb(seenRows, indexRows) };
    const handler = getHandler(backfillSeenJobUrlIndex);
    const res = await handler(ctx, { batchSize: 10 });

    expect(res.inserted).toBe(1);
    const inserted = indexRows.find((row) => row.url === "https://example.com/jobs/2");
    expect(inserted).toMatchObject({
      sourceUrl: "https://example.com/jobs",
      url: "https://example.com/jobs/2",
      seenJobUrlId: "seen-2",
      createdAt: 200,
    });
  });

  it("returns pagination cursor when more rows remain", async () => {
    const seenRows: SeenRow[] = [
      { _id: "seen-1", sourceUrl: "https://example.com/jobs", url: "1", createdAt: 1 },
      { _id: "seen-2", sourceUrl: "https://example.com/jobs", url: "2", createdAt: 2 },
      { _id: "seen-3", sourceUrl: "https://example.com/jobs", url: "3", createdAt: 3 },
    ];
    const indexRows: IndexRow[] = [];

    const ctx: any = { db: new FakeDb(seenRows, indexRows) };
    const handler = getHandler(backfillSeenJobUrlIndex);
    const res = await handler(ctx, { batchSize: 2 });

    expect(res.scanned).toBe(2);
    expect(res.hasMore).toBe(true);
    expect(res.cursor).toBe("2");
  });
});
