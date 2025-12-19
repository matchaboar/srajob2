import { describe, expect, it } from "vitest";
import { bulkUpsertSites, upsertSite } from "./router";
import { getHandler } from "./__tests__/getHandler";

type Site = { _id: string; url: string; name?: string };
type CompanyProfile = { _id: string; slug: string; name: string; aliases?: string[]; domains?: string[] };

class FakeDb {
  sites: Map<string, Site>;
  companyProfiles: Map<string, CompanyProfile>;
  private seq = 1;

  constructor({ sites = [], companyProfiles = [] }: { sites?: Site[]; companyProfiles?: CompanyProfile[] } = {}) {
    this.sites = new Map(sites.map((s) => [s._id, { ...s }]));
    this.companyProfiles = new Map(companyProfiles.map((p) => [p._id, { ...p }]));
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
    if (table === "company_profiles") {
      return {
        withIndex(_name: string, cb: (q: any) => any) {
          const slug = cb({ eq: (_field: string, value: string) => value });
          const match = Array.from(self.companyProfiles.values()).find((p) => p.slug === slug) ?? null;
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

  patch(id: string, updates: Record<string, any>) {
    if (this.sites.has(id)) {
      this.sites.set(id, { ...this.sites.get(id)!, ...updates });
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
    const record = { _id: id, ...payload };
    if (table === "sites") {
      this.sites.set(id, record);
      return id;
    }
    if (table === "company_profiles") {
      this.companyProfiles.set(id, record);
      return id;
    }
    throw new Error(`Unsupported insert table ${table}`);
  }
}

describe("upsertSite", () => {
  it("preserves query strings in stored site URLs", async () => {
    const ctx: any = {
      db: new FakeDb(),
    };

    const handler = getHandler(upsertSite) as any;
    const url =
      "https://www.github.careers/careers-home/jobs?keywords=engineer&sortBy=relevance&limit=100";

    const id = await handler(ctx, { url, enabled: true, type: "general" });

    const stored = ctx.db.sites.get(id);
    expect(stored?.url).toBe(url);
  });
});

describe("bulkUpsertSites", () => {
  it("preserves query strings in stored site URLs", async () => {
    const ctx: any = {
      db: new FakeDb(),
    };

    const handler = getHandler(bulkUpsertSites) as any;
    const url =
      "https://www.github.careers/careers-home/jobs?keywords=engineer&sortBy=relevance&limit=100";

    const [id] = await handler(ctx, {
      sites: [{ url, enabled: true, type: "general" }],
    });

    const stored = ctx.db.sites.get(id);
    expect(stored?.url).toBe(url);
  });
});
