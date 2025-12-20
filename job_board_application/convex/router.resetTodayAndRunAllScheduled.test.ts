import { afterEach, describe, expect, it, vi } from "vitest";
import { resetTodayAndRunAllScheduled } from "./router";
import { getHandler } from "./__tests__/getHandler";

type TableName =
  | "jobs"
  | "job_details"
  | "scrapes"
  | "scrape_url_queue"
  | "ignored_jobs"
  | "sites"
  | "run_requests";

type Predicate<T extends Record<string, any>> = (row: T) => boolean;

const predicateBuilder = {
  field: (name: string) => (row: Record<string, any>) => row[name],
  gte: (field: (row: Record<string, any>) => any, value: number) => (row: Record<string, any>) =>
    field(row) >= value,
  lt: (field: (row: Record<string, any>) => any, value: number) => (row: Record<string, any>) =>
    field(row) < value,
  eq: (field: (row: Record<string, any>) => any, value: any) => (row: Record<string, any>) =>
    field(row) === value,
  and:
    (...predicates: Array<Predicate<Record<string, any>>>) =>
    (row: Record<string, any>) =>
      predicates.every((predicate) => predicate(row)),
  or:
    (...predicates: Array<Predicate<Record<string, any>>>) =>
    (row: Record<string, any>) =>
      predicates.some((predicate) => predicate(row)),
};

class FakeQuery<T extends { _id: string } & Record<string, any>> {
  private predicate: Predicate<T> | null = null;

  constructor(
    private readonly db: FakeDb,
    private readonly table: TableName,
    private readonly indexPredicate?: Predicate<T>
  ) {}

  filter(cb: (q: typeof predicateBuilder) => Predicate<T>) {
    this.predicate = cb(predicateBuilder);
    return this;
  }

  withIndex(name: string, cb: (q: any) => any) {
    if (this.table === "job_details" && name === "by_job") {
      const jobId = cb({ eq: (_field: string, value: string) => value });
      return new FakeQuery(this.db, this.table, (row) => row.jobId === jobId);
    }
    if (this.table === "sites" && name === "by_enabled") {
      const enabled = cb({ eq: (_field: string, value: boolean) => value });
      return new FakeQuery(this.db, this.table, (row) => row.enabled === enabled);
    }
    if (this.table === "jobs" && name === "by_scraped_at") {
      const range = buildRange(cb);
      return new FakeQuery(
        this.db,
        this.table,
        (row) => typeof row.scrapedAt === "number" && row.scrapedAt >= range.lower && row.scrapedAt < range.upper
      );
    }
    if (this.table === "scrapes" && name === "by_completedAt") {
      const range = buildRange(cb);
      return new FakeQuery(
        this.db,
        this.table,
        (row) => typeof row.completedAt === "number" && row.completedAt >= range.lower && row.completedAt < range.upper
      );
    }
    if (this.table === "scrapes" && name === "by_startedAt") {
      const range = buildRange(cb);
      return new FakeQuery(
        this.db,
        this.table,
        (row) => typeof row.startedAt === "number" && row.startedAt >= range.lower && row.startedAt < range.upper
      );
    }
    if (this.table === "ignored_jobs" && name === "by_created_at") {
      const range = buildRange(cb);
      return new FakeQuery(
        this.db,
        this.table,
        (row) => typeof row.createdAt === "number" && row.createdAt >= range.lower && row.createdAt < range.upper
      );
    }
    throw new Error(`Unexpected index ${name} for table ${this.table}`);
  }

  take(numItems: number) {
    return this.applyFilters().slice(0, numItems);
  }

  collect() {
    return this.applyFilters();
  }

  first() {
    return this.applyFilters()[0] ?? null;
  }

  paginate() {
    throw new Error("paginate should not be used in resetTodayAndRunAllScheduled");
  }

  private applyFilters() {
    let rows = this.db.getRows(this.table) as T[];
    if (this.indexPredicate) {
      rows = rows.filter(this.indexPredicate);
    }
    if (this.predicate) {
      rows = rows.filter(this.predicate);
    }
    return rows;
  }
}

function buildRange(cb: (q: any) => any) {
  const range = { lower: -Infinity, upper: Infinity };
  const chain = {
    lt: (_field: string, value: number) => {
      range.upper = value;
      return chain;
    },
  };
  const q = {
    gte: (_field: string, value: number) => {
      range.lower = value;
      return chain;
    },
    lt: (_field: string, value: number) => {
      range.upper = value;
      return chain;
    },
    eq: (_field: string, value: any) => value,
  };
  cb(q);
  return range;
}

class FakeDb {
  private tables: Record<TableName, any[]>;
  private seq = 1;

  constructor(seed: Partial<Record<TableName, any[]>> = {}) {
    this.tables = {
      jobs: seed.jobs ? [...seed.jobs] : [],
      job_details: seed.job_details ? [...seed.job_details] : [],
      scrapes: seed.scrapes ? [...seed.scrapes] : [],
      scrape_url_queue: seed.scrape_url_queue ? [...seed.scrape_url_queue] : [],
      ignored_jobs: seed.ignored_jobs ? [...seed.ignored_jobs] : [],
      sites: seed.sites ? [...seed.sites] : [],
      run_requests: seed.run_requests ? [...seed.run_requests] : [],
    };
  }

  getRows(table: TableName) {
    return this.tables[table];
  }

  query(table: TableName) {
    return new FakeQuery(this, table);
  }

  delete(id: string) {
    for (const table of Object.keys(this.tables) as TableName[]) {
      const rows = this.tables[table];
      const index = rows.findIndex((row) => row._id === id);
      if (index >= 0) {
        rows.splice(index, 1);
        return;
      }
    }
  }

  patch(id: string, updates: Record<string, any>) {
    const site = this.tables.sites.find((row) => row._id === id);
    if (!site) throw new Error(`Unknown record ${id}`);
    Object.assign(site, updates);
  }

  insert(table: TableName, payload: any) {
    const id = `${table}-${this.seq++}`;
    const row = { _id: id, ...payload };
    this.tables[table].push(row);
    return id;
  }
}

describe("resetTodayAndRunAllScheduled", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("clears today's data and triggers enabled scheduled sites without pagination", async () => {
    const now = new Date(2024, 4, 2, 12, 0, 0, 0);
    vi.useFakeTimers();
    vi.setSystemTime(now);

    const startOfDay = new Date(2024, 4, 2, 0, 0, 0, 0).getTime();
    const endOfDay = startOfDay + 24 * 60 * 60 * 1000;

    const db = new FakeDb({
      jobs: [
        { _id: "job-1", scrapedAt: startOfDay + 1_000 },
        { _id: "job-2", scrapedAt: endOfDay + 1_000 },
      ],
      job_details: [
        { _id: "detail-1", jobId: "job-1" },
        { _id: "detail-2", jobId: "job-2" },
      ],
      scrapes: [
        { _id: "scrape-1", startedAt: startOfDay + 1_000, completedAt: startOfDay + 2_000 },
        { _id: "scrape-2", startedAt: startOfDay + 3_000, completedAt: endOfDay + 5_000 },
        { _id: "scrape-3", startedAt: endOfDay + 100, completedAt: endOfDay + 500 },
      ],
      scrape_url_queue: [{ _id: "queue-1" }, { _id: "queue-2" }, { _id: "queue-3" }],
      ignored_jobs: [
        { _id: "ignored-1", createdAt: startOfDay + 4_000 },
        { _id: "ignored-2", createdAt: endOfDay + 4_000 },
      ],
      sites: [
        {
          _id: "site-1",
          enabled: true,
          scheduleId: "sched-1",
          completed: true,
          failed: true,
          lockedBy: "tester",
          lockExpiresAt: 123,
          lastRunAt: 456,
          lastFailureAt: 789,
          lastError: "boom",
          manualTriggerAt: 0,
          url: "https://example.com",
        },
        { _id: "site-2", enabled: true, scheduleId: undefined, url: "https://example.com/2" },
        { _id: "site-3", enabled: false, scheduleId: "sched-3", url: "https://example.com/3" },
      ],
    });

    const handler = getHandler(resetTodayAndRunAllScheduled);
    const result = await handler({ db } as any, {});

    expect(result.jobsDeleted).toBe(1);
    expect(result.scrapesDeleted).toBe(2);
    expect(result.queueDeleted).toBe(3);
    expect(result.skippedDeleted).toBe(1);
    expect(result.sitesTriggered).toBe(1);
    expect(result.hasMore).toBe(false);

    expect(db.getRows("jobs").some((row) => row._id === "job-1")).toBe(false);
    expect(db.getRows("job_details").some((row) => row._id === "detail-1")).toBe(false);
    expect(db.getRows("jobs").some((row) => row._id === "job-2")).toBe(true);
    expect(db.getRows("scrapes").some((row) => row._id === "scrape-1")).toBe(false);
    expect(db.getRows("scrapes").some((row) => row._id === "scrape-2")).toBe(false);
    expect(db.getRows("scrapes").some((row) => row._id === "scrape-3")).toBe(true);
    expect(db.getRows("scrape_url_queue")).toHaveLength(0);
    expect(db.getRows("ignored_jobs").some((row) => row._id === "ignored-1")).toBe(false);
    expect(db.getRows("ignored_jobs").some((row) => row._id === "ignored-2")).toBe(true);

    const site = db.getRows("sites").find((row) => row._id === "site-1");
    expect(site.completed).toBe(false);
    expect(site.failed).toBe(false);
    expect(site.lockedBy).toBe("");
    expect(site.lockExpiresAt).toBe(0);
    expect(site.lastRunAt).toBe(0);
    expect(site.lastFailureAt).toBeUndefined();
    expect(site.lastError).toBeUndefined();
    expect(site.manualTriggerAt).toBe(now.getTime());

    expect(db.getRows("run_requests")).toHaveLength(1);
  });

  it("batches deletions and only triggers sites on the final pass", async () => {
    const now = new Date(2024, 4, 3, 12, 0, 0, 0);
    vi.useFakeTimers();
    vi.setSystemTime(now);

    const startOfDay = new Date(2024, 4, 3, 0, 0, 0, 0).getTime();

    const db = new FakeDb({
      jobs: [
        { _id: "job-1", scrapedAt: startOfDay + 1_000 },
        { _id: "job-2", scrapedAt: startOfDay + 2_000 },
        { _id: "job-3", scrapedAt: startOfDay + 3_000 },
      ],
      job_details: [
        { _id: "detail-1", jobId: "job-1" },
        { _id: "detail-2", jobId: "job-2" },
        { _id: "detail-3", jobId: "job-3" },
      ],
      sites: [
        {
          _id: "site-1",
          enabled: true,
          scheduleId: "sched-1",
          completed: true,
          failed: true,
          lockedBy: "tester",
          lockExpiresAt: 123,
          lastRunAt: 456,
          lastFailureAt: 789,
          lastError: "boom",
          manualTriggerAt: 0,
          url: "https://example.com",
        },
      ],
    });

    const handler = getHandler(resetTodayAndRunAllScheduled);
    const first = await handler({ db } as any, { batchSize: 2 });

    expect(first.jobsDeleted).toBe(2);
    expect(first.hasMore).toBe(true);
    expect(first.sitesTriggered).toBe(0);
    expect(db.getRows("run_requests")).toHaveLength(0);

    const siteAfterFirst = db.getRows("sites").find((row) => row._id === "site-1");
    expect(siteAfterFirst.completed).toBe(true);
    expect(siteAfterFirst.failed).toBe(true);

    const second = await handler({ db } as any, { batchSize: 2 });

    expect(second.jobsDeleted).toBe(1);
    expect(second.hasMore).toBe(false);
    expect(second.sitesTriggered).toBe(1);
    expect(db.getRows("jobs")).toHaveLength(0);
    expect(db.getRows("job_details")).toHaveLength(0);
    expect(db.getRows("run_requests")).toHaveLength(1);

    const siteAfterSecond = db.getRows("sites").find((row) => row._id === "site-1");
    expect(siteAfterSecond.completed).toBe(false);
    expect(siteAfterSecond.failed).toBe(false);
  });
});
