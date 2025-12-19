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
};

type Page = {
  page: Job[];
  isDone: boolean;
  continueCursor: string | null;
};

class FakeJobsQuery {
  constructor(
    private readonly pagesByCursor: Map<string | null, Page>,
    private readonly tracker: { totalPaginateCalls: number }
  ) {}

  withIndex(name: string, cb?: (q: any) => any) {
    if (name !== "by_state_posted" && name !== "by_posted_at") {
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

const buildJob = (id: string, postedAt: number): Job => ({
  _id: id,
  title: "Software Engineer",
  company: "Example Co",
  location: "Remote",
  remote: true,
  level: "mid",
  totalCompensation: 120000,
  compensationUnknown: false,
  url: `https://example.com/jobs/${id}`,
  postedAt,
});

const buildCtx = (pagesByCursor: Map<string | null, Page>, tracker: { totalPaginateCalls: number }) => ({
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
    const tracker = { totalPaginateCalls: 0 };
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
});
