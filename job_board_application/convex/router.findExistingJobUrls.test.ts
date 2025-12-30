import { describe, expect, it } from "vitest";
import { findExistingJobUrls } from "./router";
import { getHandler } from "./__tests__/getHandler";

type JobRow = { url: string };

class FakeJobsQuery {
  private filterUrl: string | undefined;

  constructor(private readonly rows: JobRow[]) {}

  withIndex(name: string, cb: (q: any) => any) {
    if (name !== "by_url") throw new Error(`unexpected jobs index ${name}`);
    const builder = {
      eq: (_field: string, val: any) => {
        this.filterUrl = val;
        return builder;
      },
    };
    cb(builder);
    return this;
  }

  first() {
    return this.rows.find((row) => row.url === this.filterUrl) ?? null;
  }
}

class FakeDb {
  constructor(private readonly jobs: JobRow[]) {}

  query(table: string) {
    if (table === "jobs") return new FakeJobsQuery(this.jobs);
    throw new Error(`unexpected table ${table}`);
  }
}

describe("findExistingJobUrls", () => {
  it("returns urls that already exist in the jobs table", async () => {
    const jobs: JobRow[] = [
      { url: "https://example.com/jobs/1" },
      { url: "https://example.com/jobs/2" },
    ];
    const ctx: any = { db: new FakeDb(jobs) };
    const handler = getHandler(findExistingJobUrls);

    const res = await handler(ctx, {
      urls: ["https://example.com/jobs/2", "https://example.com/jobs/3"],
    });

    expect(res).toEqual({ existing: ["https://example.com/jobs/2"] });
  });
});
