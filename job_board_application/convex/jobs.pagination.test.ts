import { describe, expect, it, vi } from "vitest";
import { getHandler } from "./__tests__/getHandler";

vi.mock("@convex-dev/auth/server", () => ({
  getAuthUserId: () => "user-1",
}));

import { listJobs } from "./jobs";

type Job = {
  _id: string;
  title: string;
  company: string;
  location: string;
  remote: boolean;
  level: "junior" | "mid" | "senior" | "staff";
  totalCompensation: number;
  compensationUnknown?: boolean;
  url: string;
  postedAt: number;
  scrapedAt?: number;
};

type Page = {
  page: Job[];
  isDone: boolean;
  continueCursor: string | null;
};

class FakeJobsQuery {
  constructor(
    private readonly pagesByCursor: Map<string | null, Page>,
    private readonly tracker: { totalPaginateCalls: number; lastIndexName: string | null }
  ) {}

  withIndex(name: string, cb?: (q: any) => any) {
    this.tracker.lastIndexName = name;
    if (
      name !== "by_state_posted" &&
      name !== "by_posted_at" &&
      name !== "by_company_posted" &&
      name !== "by_scraped_posted"
    ) {
      throw new Error(`unexpected jobs index ${name}`);
    }
    if (cb) {
      cb({ eq: (_field: string, val: any) => val });
    }
    return this;
  }

  order(_dir: string) {
    return this;
  }

  filter(_cb: (q: any) => any) {
    return this;
  }

  paginate(opts: { cursor?: string | null }) {
    this.tracker.totalPaginateCalls += 1;
    if (this.tracker.totalPaginateCalls > 1) {
      throw new Error("paginate called more than once in a single handler");
    }
    const cursor = opts?.cursor ?? null;
    return (
      this.pagesByCursor.get(cursor) ?? {
        page: [],
        isDone: true,
        continueCursor: null,
      }
    );
  }
}

class FakeApplicationsQuery {
  withIndex(name: string, cb: (q: any) => any) {
    if (name !== "by_user" && name !== "by_job") {
      throw new Error(`unexpected applications index ${name}`);
    }
    cb({ eq: (_field: string, val: any) => val });
    return this;
  }

  filter(_cb: (q: any) => any) {
    return this;
  }

  collect() {
    return [];
  }
}

const buildJob = (id: string, postedAt: number, scrapedAt: number | undefined = postedAt, title = "Software Engineer"): Job => ({
  _id: id,
  title,
  company: "Example Co",
  location: "Remote",
  remote: true,
  level: "mid",
  totalCompensation: 120000,
  compensationUnknown: false,
  url: `https://example.com/jobs/${id}`,
  postedAt,
  scrapedAt,
});

const buildCtx = (
  pagesByCursor: Map<string | null, Page>,
  tracker: { totalPaginateCalls: number; lastIndexName: string | null }
) => ({
  db: {
    query: (table: string) => {
      if (table === "jobs") return new FakeJobsQuery(pagesByCursor, tracker);
      if (table === "applications") return new FakeApplicationsQuery();
      if (table === "domain_aliases") return { collect: () => [] };
      throw new Error(`unexpected table ${table}`);
    },
    get: async () => null,
    patch: async () => {},
  },
});

describe("listJobs pagination", () => {
  it("paginates filtered results without reusing a query instance", async () => {
    const page1: Page = {
      page: [buildJob("job-1", 100)],
      isDone: false,
      continueCursor: "cursor-1",
    };
    const pagesByCursor = new Map<string | null, Page>([
      [null, page1],
    ]);
    const tracker = { totalPaginateCalls: 0, lastIndexName: null };
    const ctx = buildCtx(pagesByCursor, tracker);
    const handler = getHandler(listJobs);

    const result = await handler(ctx, {
      paginationOpts: { cursor: null, numItems: 2 },
      hideUnknownCompensation: true,
    });

    expect(result.page.length).toBeGreaterThan(0);
    expect(result.continueCursor).not.toBeNull();
    expect(tracker.totalPaginateCalls).toBe(1);
  });

  it("uses the scraped+posted index by default", async () => {
    const page1: Page = {
      page: [buildJob("job-1", 100)],
      isDone: true,
      continueCursor: null,
    };
    const pagesByCursor = new Map<string | null, Page>([[null, page1]]);
    const tracker = { totalPaginateCalls: 0, lastIndexName: null };
    const ctx = buildCtx(pagesByCursor, tracker);
    const handler = getHandler(listJobs);

    await handler(ctx, {
      paginationOpts: { cursor: null, numItems: 2 },
    });

    expect(tracker.lastIndexName).toBe("by_scraped_posted");
  });

  it("orders results by scrapedAt then postedAt", async () => {
    const page1: Page = {
      page: [
        buildJob("job-1", 500, 100, "Engineer A"),
        buildJob("job-2", 1000, 200, "Engineer B"),
      ],
      isDone: true,
      continueCursor: null,
    };
    const pagesByCursor = new Map<string | null, Page>([[null, page1]]);
    const tracker = { totalPaginateCalls: 0, lastIndexName: null };
    const ctx = buildCtx(pagesByCursor, tracker);
    const handler = getHandler(listJobs);

    const result = await handler(ctx, {
      paginationOpts: { cursor: null, numItems: 2 },
    });

    expect(result.page[0]._id).toBe("job-2");
    expect(result.page[1]._id).toBe("job-1");
  });

  it("uses the scraped+posted index when a single company filter is set", async () => {
    const page1: Page = {
      page: [buildJob("job-1", 100)],
      isDone: true,
      continueCursor: null,
    };
    const pagesByCursor = new Map<string | null, Page>([
      [null, page1],
    ]);
    const tracker = { totalPaginateCalls: 0, lastIndexName: null };
    const ctx = buildCtx(pagesByCursor, tracker);
    const handler = getHandler(listJobs);

    await handler(ctx, {
      paginationOpts: { cursor: null, numItems: 2 },
      companies: ["Airbnb"],
    });

    expect(tracker.lastIndexName).toBe("by_scraped_posted");
  });

  it("matches company filters case-insensitively", async () => {
    const job1 = { ...buildJob("job-1", 200), company: "Lambda" };
    const job2 = { ...buildJob("job-2", 100), company: "Other Co" };
    const page1: Page = {
      page: [job1, job2],
      isDone: true,
      continueCursor: null,
    };
    const pagesByCursor = new Map<string | null, Page>([[null, page1]]);
    const tracker = { totalPaginateCalls: 0, lastIndexName: null };
    const ctx = buildCtx(pagesByCursor, tracker);
    const handler = getHandler(listJobs);

    const result = await handler(ctx, {
      paginationOpts: { cursor: null, numItems: 5 },
      companies: ["lambda"],
    });

    expect(result.page.map((job) => job.company)).toEqual(["Lambda"]);
  });
});
