import { describe, expect, it } from "vitest";
import { findExistingJobUrls } from "./router";
import { getHandler } from "./__tests__/getHandler";

type JobRow = { url: string };
type JobUrlKeyRow = { bucket: number; url: string };

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

class FakeJobUrlKeysQuery {
  private filterBucket: number | undefined;

  constructor(private readonly rows: JobUrlKeyRow[]) {}

  withIndex(name: string, cb: (q: any) => any) {
    if (name !== "by_bucket") throw new Error(`unexpected job_url_keys index ${name}`);
    const builder = {
      eq: (_field: string, val: any) => {
        this.filterBucket = val;
        return builder;
      },
    };
    cb(builder);
    return this;
  }

  collect() {
    if (typeof this.filterBucket !== "number") return [];
    return this.rows.filter((row) => row.bucket === this.filterBucket);
  }
}

class FakeDb {
  private readonly jobs: JobRow[] | null;
  private readonly jobUrlKeys: JobUrlKeyRow[] | null;
  jobsQueries = 0;

  constructor(options: { jobs?: JobRow[]; jobUrlKeys?: JobUrlKeyRow[] }) {
    this.jobs = options.jobs ?? null;
    this.jobUrlKeys = options.jobUrlKeys ?? null;
  }

  query(table: string) {
    if (table === "jobs") {
      if (!this.jobs) throw new Error("jobs table should not be queried");
      this.jobsQueries += 1;
      return new FakeJobsQuery(this.jobs);
    }
    if (table === "job_url_keys") {
      if (!this.jobUrlKeys) throw new Error("job_url_keys table not configured");
      return new FakeJobUrlKeysQuery(this.jobUrlKeys);
    }
    throw new Error(`unexpected table ${table}`);
  }
}

describe("findExistingJobUrls", () => {
  const hashStringToBucket = (value: string, bucketCount: number) => {
    let hash = 2166136261;
    for (let i = 0; i < value.length; i += 1) {
      hash ^= value.charCodeAt(i);
      hash = Math.imul(hash, 16777619);
    }
    return (hash >>> 0) % bucketCount;
  };

  const deriveJobUrlBucket = (url: string) => hashStringToBucket(url, 256);

  it("returns urls that already exist via job_url_keys", async () => {
    const url = "https://example.com/jobs/2";
    const ctx: any = {
      db: new FakeDb({
        jobUrlKeys: [{ bucket: deriveJobUrlBucket(url), url }],
      }),
    };
    const handler = getHandler(findExistingJobUrls);

    const res = await handler(ctx, {
      urls: ["https://example.com/jobs/2/", "https://example.com/jobs/3"],
    });

    expect(res).toEqual({ existing: ["https://example.com/jobs/2/"] });
    expect(ctx.db.jobsQueries).toBe(0);
  });

  it("does not fall back to jobs when job_url_keys has no match", async () => {
    const jobs: JobRow[] = [{ url: "https://example.com/jobs/2" }];
    const ctx: any = {
      db: new FakeDb({
        jobs,
        jobUrlKeys: [{ bucket: deriveJobUrlBucket("https://example.com/other"), url: "https://example.com/other" }],
      }),
    };
    const handler = getHandler(findExistingJobUrls);

    const res = await handler(ctx, {
      urls: ["https://example.com/jobs/2"],
    });

    expect(res).toEqual({ existing: [] });
    expect(ctx.db.jobsQueries).toBe(0);
  });
});
