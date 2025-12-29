import { describe, expect, it } from "vitest";
import { refreshCompanySummaries } from "./jobs";
import { getHandler } from "./__tests__/getHandler";

type JobRow = {
  _id: string;
  company?: string;
  url?: string;
  scrapedAt?: number;
  level?: "junior" | "mid" | "senior" | "staff" | string;
  totalCompensation?: number | null;
  compensationUnknown?: boolean;
  currencyCode?: string | null;
};

class FakeDb {
  private jobs: JobRow[];
  private summaries: any[];

  constructor(jobs: JobRow[]) {
    this.jobs = jobs.map((job) => ({ ...job }));
    this.summaries = [];
  }

  query(table: string) {
    if (table === "jobs") {
      return { collect: async () => this.jobs };
    }
    if (table === "company_summaries") {
      return { collect: async () => this.summaries };
    }
    throw new Error(`Unexpected table ${table}`);
  }

  insert = async (table: string, payload: any) => {
    if (table !== "company_summaries") {
      throw new Error(`Unexpected insert table ${table}`);
    }
    const _id = `summary-${this.summaries.length + 1}`;
    this.summaries.push({ _id, ...payload });
    return _id;
  };

  patch = async (id: string, payload: any) => {
    const row = this.summaries.find((entry) => entry._id === id);
    if (!row) throw new Error(`Unknown id ${id}`);
    Object.assign(row, payload);
  };

  delete = async (id: string) => {
    const index = this.summaries.findIndex((entry) => entry._id === id);
    if (index >= 0) {
      this.summaries.splice(index, 1);
    }
  };

  getSummaries() {
    return this.summaries;
  }
}

describe("refreshCompanySummaries", () => {
  it("excludes unknown and non-USD salaries from averages", async () => {
    const jobs: JobRow[] = [
      {
        _id: "job-1",
        company: "Acme",
        url: "https://example.com/jobs/1",
        level: "junior",
        totalCompensation: 100_000,
        currencyCode: "USD",
      },
      {
        _id: "job-2",
        company: "Acme",
        url: "https://example.com/jobs/2",
        level: "junior",
        totalCompensation: 200_000,
        currencyCode: "INR",
      },
      {
        _id: "job-3",
        company: "Acme",
        url: "https://example.com/jobs/3",
        level: "junior",
        totalCompensation: 150_000,
        currencyCode: "$",
      },
      {
        _id: "job-4",
        company: "Acme",
        url: "https://example.com/jobs/4",
        level: "junior",
        totalCompensation: 175_000,
        currencyCode: "USD",
        compensationUnknown: true,
      },
      {
        _id: "job-5",
        company: "Acme",
        url: "https://example.com/jobs/5",
        level: "junior",
        currencyCode: "USD",
      },
    ];
    const ctx: any = { db: new FakeDb(jobs) };

    const handler = getHandler(refreshCompanySummaries);
    await handler(ctx, {});

    const summaries = ctx.db.getSummaries();
    expect(summaries).toHaveLength(1);
    const summary = summaries[0];
    expect(summary.name).toBe("Acme");
    expect(summary.count).toBe(5);
    expect(summary.currencyCode).toBe("USD");
    expect(summary.avgCompensationJunior).toBe(125_000);
  });

  it("does not set averages when only non-USD salaries are present", async () => {
    const jobs: JobRow[] = [
      {
        _id: "job-1",
        company: "RupeeCo",
        url: "https://example.com/jobs/1",
        level: "mid",
        totalCompensation: 9_000_000,
        currencyCode: "INR",
      },
    ];
    const ctx: any = { db: new FakeDb(jobs) };

    const handler = getHandler(refreshCompanySummaries);
    await handler(ctx, {});

    const summaries = ctx.db.getSummaries();
    expect(summaries).toHaveLength(1);
    const summary = summaries[0];
    expect(summary.name).toBe("RupeeCo");
    expect(summary.currencyCode).toBeUndefined();
    expect(summary.avgCompensationJunior).toBeUndefined();
    expect(summary.avgCompensationMid).toBeUndefined();
    expect(summary.avgCompensationSenior).toBeUndefined();
  });
});
