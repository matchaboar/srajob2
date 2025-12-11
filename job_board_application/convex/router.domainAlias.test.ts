import { describe, expect, it, vi } from "vitest";
import { setDomainAlias } from "./router";

type Site = { _id: string; name?: string; url: string };
type Job = { _id: string; company: string; url?: string };
type DomainAlias = { _id: string; domain: string; alias: string; derivedName?: string; createdAt?: number; updatedAt?: number };
type CompanyProfile = { _id: string; slug: string; name: string; aliases?: string[]; domains?: string[] };

const normalize = (value: string) => (value || "").toLowerCase().replace(/[^a-z0-9]/g, "");

class FakeDb {
  sites: Map<string, Site>;
  jobs: Map<string, Job>;
  domainAliases: Map<string, DomainAlias>;
  companyProfiles: Map<string, CompanyProfile>;
  private seq = 1;

  constructor({
    sites = [],
    jobs = [],
    domainAliases = [],
    companyProfiles = [],
  }: {
    sites?: Site[];
    jobs?: Job[];
    domainAliases?: DomainAlias[];
    companyProfiles?: CompanyProfile[];
  }) {
    this.sites = new Map(sites.map((s) => [s._id, { ...s }]));
    this.jobs = new Map(jobs.map((j) => [j._id, { ...j }]));
    this.domainAliases = new Map(domainAliases.map((d) => [d._id, { ...d }]));
    this.companyProfiles = new Map(companyProfiles.map((p) => [p._id, { ...p }]));
  }

  get(id: string) {
    return this.sites.get(id) ?? this.jobs.get(id) ?? this.domainAliases.get(id) ?? this.companyProfiles.get(id) ?? null;
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
    if (this.domainAliases.has(id)) {
      this.domainAliases.set(id, { ...this.domainAliases.get(id)!, ...updates });
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
    if (table === "domain_aliases") {
      const record = { _id: id, ...payload };
      this.domainAliases.set(id, record);
      return id;
    }
    if (table === "company_profiles") {
      const record = { _id: id, ...payload };
      this.companyProfiles.set(id, record);
      return id;
    }
    throw new Error(`Unsupported insert table ${table}`);
  }

  query(table: string) {
    const self = this;
    if (table === "sites") {
      return {
        collect() {
          return Array.from(self.sites.values());
        },
      };
    }

    if (table === "domain_aliases") {
      return {
        withIndex(_name: string, cb: (q: any) => any) {
          const domain = cb({ eq: (_field: string, val: any) => val });
          const rows = Array.from(self.domainAliases.values()).filter((d) => d.domain === domain);
          return {
            first() {
              return rows[0] ?? null;
            },
          };
        },
      };
    }

    if (table === "jobs") {
      return {
        withIndex(_name: string, cb: (q: any) => any) {
          const company = cb({ eq: (_field: string, val: any) => val });
          const rows = Array.from(self.jobs.values()).filter((j) => j.company === company);
          return {
            collect() {
              return rows;
            },
          };
        },
      };
    }

    if (table === "company_profiles") {
      return {
        withIndex(_name: string, cb: (q: any) => any) {
          const slug = cb({ eq: (_field: string, val: any) => val });
          const rows = Array.from(self.companyProfiles.values()).filter((p) => p.slug === slug);
          return {
            first() {
              return rows[0] ?? null;
            },
          };
        },
      };
    }

    throw new Error(`Unsupported query table ${table}`);
  }

  search(table: string, _index: string, term: string) {
    if (table !== "jobs") throw new Error(`Unsupported search table ${table}`);
    const target = normalize(term);
    const rows = Array.from(this.jobs.values()).filter((j) => normalize(j.company) === target);
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

describe("setDomainAlias", () => {
  it("isolates greenhouse board aliases by slug so updating one does not rename others", async () => {
    const ctx: any = {
      db: new FakeDb({
        sites: [
          {
            _id: "site-stubhub",
            name: "Stubhub",
            url: "https://api.greenhouse.io/v1/boards/stubhubinc/jobs",
          },
          {
            _id: "site-coupang",
            name: "Coupang",
            url: "https://boards.greenhouse.io/v1/boards/coupang/jobs",
          },
        ],
        jobs: [
          { _id: "job-stubhub", company: "Stubhub", url: "https://boards.greenhouse.io/v1/boards/stubhubinc/jobs/123" },
          { _id: "job-coupang", company: "Coupang", url: "https://boards.greenhouse.io/v1/boards/coupang/jobs/456" },
        ],
      }),
    };

    const handler = (setDomainAlias as any).handler ?? setDomainAlias;
    const res = await handler(ctx, {
      domainOrUrl: "https://api.greenhouse.io/v1/boards/stubhubinc/jobs",
      alias: "Stubhub Inc",
    });

    expect(res.domain).toBe("stubhubinc.greenhouse.io");
    expect(res.alias).toBe("Stubhub Inc");
    expect(ctx.db.sites.get("site-stubhub")?.name).toBe("Stubhub Inc");
    expect(ctx.db.sites.get("site-coupang")?.name).toBe("Coupang");
    expect(ctx.db.jobs.get("job-stubhub")?.company).toBe("Stubhub Inc");
    expect(ctx.db.jobs.get("job-coupang")?.company).toBe("Coupang");
  });
});
