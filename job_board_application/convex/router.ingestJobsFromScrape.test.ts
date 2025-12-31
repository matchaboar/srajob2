import { describe, expect, it } from "vitest";
import { ingestJobsFromScrape } from "./router";
import { getHandler } from "./__tests__/getHandler";

class FakeDb {
  jobs: Map<string, any> = new Map();
  jobDetails: any[] = [];
  domainAliases: Map<string, any> = new Map();
  private seq = 1;

  query(table: string) {
    if (table === "jobs") {
      const jobs = this.jobs;
      return {
        withIndex(_name: string, cb: (q: any) => any) {
          const url = cb({ eq: (_field: string, value: string) => value });
          const match = Array.from(jobs.values()).find((job) => job.url === url) ?? null;
          return {
            first() {
              return match;
            },
          };
        },
      };
    }
    if (table === "domain_aliases") {
      const aliases = this.domainAliases;
      return {
        withIndex(_name: string, cb: (q: any) => any) {
          const domain = cb({ eq: (_field: string, value: string) => value });
          const match = aliases.get(domain) ?? null;
          return {
            first() {
              return match;
            },
          };
        },
      };
    }
    throw new Error(`Unsupported query table ${table}`);
  }

  insert(table: string, payload: any) {
    const id = `${table}-${this.seq++}`;
    if (table === "jobs") {
      this.jobs.set(id, { _id: id, ...payload });
      return id;
    }
    if (table === "job_details") {
      this.jobDetails.push({ _id: id, ...payload });
      return id;
    }
    throw new Error(`Unsupported insert table ${table}`);
  }
}

describe("ingestJobsFromScrape", () => {
  it("prefers ashby slug over provider company name", async () => {
    const ctx: any = { db: new FakeDb() };
    const handler = getHandler(ingestJobsFromScrape);
    const now = Date.now();

    await handler(ctx, {
      jobs: [
        {
          title: "Senior Software Engineer",
          company: "Ashbyhq",
          description: "Role details",
          location: "Remote",
          remote: true,
          level: "mid",
          totalCompensation: 0,
          url: "https://jobs.ashbyhq.com/notion/5703a1d4-e1a2-4286-af10-a48c65fd4114",
          postedAt: now,
          postedAtUnknown: false,
        },
      ],
    });

    const job = Array.from(ctx.db.jobs.values())[0] as { company: string; companyKey: string } | undefined;
    expect(job?.company).toBe("Notion");
    expect(job?.companyKey).toBe("notion");
  });
});
