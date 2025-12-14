import { describe, expect, it } from "vitest";
import { updateSiteName } from "./router";

type Job = { _id: string; company: string; url?: string };
type Site = { _id: string; name?: string; url: string };
type CompanyProfile = { _id: string; slug: string; name: string; aliases?: string[]; domains?: string[] };

const normalize = (value: string) => (value || "").toLowerCase().replace(/[^a-z0-9]/g, "");

class FakeDb {
  sites: Map<string, Site>;
  jobs: Map<string, Job>;
  companyProfiles: Map<string, CompanyProfile>;
  private seq = 1;

  constructor({
    sites = [],
    jobs = [],
    companyProfiles = [],
  }: {
    sites?: Site[];
    jobs?: Job[];
    companyProfiles?: CompanyProfile[];
  }) {
    this.sites = new Map(sites.map((s) => [s._id, { ...s }]));
    this.jobs = new Map(jobs.map((j) => [j._id, { ...j }]));
    this.companyProfiles = new Map(companyProfiles.map((p) => [p._id, { ...p }]));
  }

  get(id: string) {
    return this.sites.get(id) ?? this.jobs.get(id) ?? this.companyProfiles.get(id) ?? null;
  }

  patch(id: string, updates: Record<string, any>) {
    if (this.sites.has(id)) {
      this.sites.set(id, { ...this.sites.get(id)!, ...updates });
      return;
    }
    if (this.jobs.has(id)) {
      this.jobs.set(id, { ...this.jobs.get(id)!, ...updates });
      return;
    }
    if (this.companyProfiles.has(id)) {
      this.companyProfiles.set(id, { ...this.companyProfiles.get(id)!, ...updates });
      return;
    }
    throw new Error(`Unknown id ${id}`);
  }

  insert(table: string, payload: any) {
    const id = `${table}-${this.seq++}`;
    if (table === "company_profiles") {
      const record = { _id: id, ...payload };
      this.companyProfiles.set(id, record);
      return id;
    }
    throw new Error(`Unsupported insert table ${table}`);
  }

  query(table: string) {
    const self = this;
    return {
      collect() {
        if (table === "jobs") {
          return Array.from(self.jobs.values());
        }
        throw new Error(`Unsupported collect for table ${table}`);
      },
      withIndex(_name: string, cb: (q: any) => any) {
        const value = cb({ eq: (_field: string, val: any) => val });
        if (table === "jobs") {
          const rows = Array.from(self.jobs.values()).filter((j) => j.company === value);
          return {
            paginate({ cursor, numItems }: { cursor: string | null; numItems: number }) {
              const start = cursor ? Number(cursor) : 0;
              const page = rows.slice(start, start + numItems);
              const nextCursor = start + numItems >= rows.length ? null : String(start + numItems);
              return { page, isDone: nextCursor === null, continueCursor: nextCursor };
            },
          };
        }
        if (table === "company_profiles") {
          const rows = Array.from(self.companyProfiles.values()).filter((p) => p.slug === value);
          return {
            first() {
              return rows[0] ?? null;
            },
          };
        }
        throw new Error(`Unsupported index for table ${table}`);
      },
    };
  }

  search(table: string, _index: string, term: string) {
    if (table !== "jobs") throw new Error(`Unsupported search table ${table}`);
    const target = normalize(term);
    const rows = Array.from(this.jobs.values()).filter((j) => normalize(j.company).includes(target));
    return {
      paginate({ cursor, numItems }: { cursor: string | null; numItems: number }) {
        const start = cursor ? Number(cursor) : 0;
        const page = rows.slice(start, start + numItems);
        const nextCursor = start + numItems >= rows.length ? null : String(start + numItems);
        return { page, isDone: nextCursor === null, continueCursor: nextCursor };
      },
    };
  }
}

describe("updateSiteName", () => {
  it("retags jobs whose company matches old name ignoring case and spacing", async () => {
    const ctx: any = {
      db: new FakeDb({
        sites: [{ _id: "site-1", name: "datadoghq", url: "https://careers.datadoghq.com" }],
        jobs: [
          { _id: "job-1", company: "Datadoghq" },
          { _id: "job-2", company: "datadoghq" },
          { _id: "job-3", company: "DATADOGHQ" },
          { _id: "job-4", company: "DataDog HQ" }, // search index fallback
          { _id: "job-5", company: "Datadog" }, // should remain untouched
        ],
      }),
    };

    const handler = (updateSiteName as any).handler ?? updateSiteName;
    const result = await handler(ctx, { id: "site-1", name: "Datadog" });

    expect(result.updatedJobs).toBe(4);
    expect(ctx.db.sites.get("site-1")!.name).toBe("Datadog");
    expect(ctx.db.jobs.get("job-5")!.company).toBe("Datadog"); // unchanged
    for (const id of ["job-1", "job-2", "job-3", "job-4"]) {
      expect(ctx.db.jobs.get(id)!.company).toBe("Datadog");
    }
  });

  it("retags jobs from the same domain even if company does not match old name", async () => {
    const ctx: any = {
      db: new FakeDb({
        sites: [{ _id: "site-1", name: "Exampleco", url: "https://careers.example.com/jobs" }],
        jobs: [
          { _id: "job-1", company: "WeirdScrapeName", url: "https://jobs.example.com/positions/1" },
          { _id: "job-2", company: "OtherCo", url: "https://jobs.other.com/positions/2" },
        ],
      }),
    };

    const handler = (updateSiteName as any).handler ?? updateSiteName;
    const result = await handler(ctx, { id: "site-1", name: "Example" });

    expect(result.updatedJobs).toBe(1);
    expect(ctx.db.jobs.get("job-1")!.company).toBe("Example");
    expect(ctx.db.jobs.get("job-2")!.company).toBe("OtherCo");
  });
});
