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

  // The canonical format for all Greenhouse URLs
  const canonicalUrl = "https://boards.greenhouse.io/axon/jobs/7449723003";

  it("matches Greenhouse API format URL when canonical format is stored (bug: k176cc8syrfppk8y0th1w7hd6x7y9z6s)", async () => {
    // Jobs are stored with canonical (web) format URL
    const ctx: any = {
      db: new FakeDb({
        jobUrlKeys: [{ bucket: deriveJobUrlBucket(canonicalUrl), url: canonicalUrl }],
      }),
    };
    const handler = getHandler(findExistingJobUrls);

    // But the listing page returns API format URL
    const apiFormatUrl = "https://boards-api.greenhouse.io/v1/boards/axon/jobs/7449723003";
    const res = await handler(ctx, {
      urls: [apiFormatUrl],
    });

    // Should find the job as existing (API format is canonicalized for lookup)
    expect(res).toEqual({ existing: [apiFormatUrl] });
  });

  it("matches Greenhouse web format URL when canonical format is stored", async () => {
    // Jobs are stored with canonical (web) format URL
    const ctx: any = {
      db: new FakeDb({
        jobUrlKeys: [{ bucket: deriveJobUrlBucket(canonicalUrl), url: canonicalUrl }],
      }),
    };
    const handler = getHandler(findExistingJobUrls);

    // Checking with web format URL (same as canonical)
    const webFormatUrl = "https://boards.greenhouse.io/axon/jobs/7449723003";
    const res = await handler(ctx, {
      urls: [webFormatUrl],
    });

    // Should find the job as existing
    expect(res).toEqual({ existing: [webFormatUrl] });
  });

  it("matches Greenhouse job-boards format URL when canonical format is stored", async () => {
    // Jobs are stored with canonical (web) format URL
    const ctx: any = {
      db: new FakeDb({
        jobUrlKeys: [{ bucket: deriveJobUrlBucket(canonicalUrl), url: canonicalUrl }],
      }),
    };
    const handler = getHandler(findExistingJobUrls);

    // But the Greenhouse API returns job-boards format URL
    const jobBoardsFormatUrl = "https://job-boards.greenhouse.io/axon/jobs/7449723003";
    const res = await handler(ctx, {
      urls: [jobBoardsFormatUrl],
    });

    // Should find the job as existing (job-boards format is canonicalized for lookup)
    expect(res).toEqual({ existing: [jobBoardsFormatUrl] });
  });

  it("matches all three Greenhouse URL formats against canonical stored URL", async () => {
    // Jobs are stored with canonical (web) format URL
    const ctx: any = {
      db: new FakeDb({
        jobUrlKeys: [{ bucket: deriveJobUrlBucket(canonicalUrl), url: canonicalUrl }],
      }),
    };
    const handler = getHandler(findExistingJobUrls);

    // All three formats should match
    const webUrl = "https://boards.greenhouse.io/axon/jobs/7449723003";
    const apiUrl = "https://boards-api.greenhouse.io/v1/boards/axon/jobs/7449723003";
    const jobBoardsUrl = "https://job-boards.greenhouse.io/axon/jobs/7449723003";

    const res = await handler(ctx, {
      urls: [webUrl, apiUrl, jobBoardsUrl],
    });

    // All three should be found as existing
    expect(res).toEqual({ existing: [webUrl, apiUrl, jobBoardsUrl] });
  });
});
