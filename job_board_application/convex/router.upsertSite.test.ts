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
    const { sites, companyProfiles } = this;
    if (table === "sites") {
      return {
        collect() {
          return Array.from(sites.values());
        },
      };
    }
    if (table === "company_profiles") {
      return {
        withIndex(_name: string, cb: (q: any) => any) {
          const slug = cb({ eq: (_field: string, value: string) => value });
          const match = Array.from(companyProfiles.values()).find((p) => p.slug === slug) ?? null;
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

  get(id: string) {
    if (this.sites.has(id)) {
      return this.sites.get(id)!;
    }
    if (this.companyProfiles.has(id)) {
      return this.companyProfiles.get(id)!;
    }
    return null;
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
  it("updates existing site when name matches (case-insensitive)", async () => {
    const existingSite = {
      _id: "sites-existing",
      url: "https://old.example.com/jobs",
      name: "Acme Corp",
      type: "general",
    };
    const ctx: any = {
      db: new FakeDb({ sites: [existingSite] }),
      runMutation: async () => null,
    };

    const handler = getHandler(upsertSite);
    const newUrl = "https://new.example.com/careers";

    // Call with same name (different case) but different URL
    const id = await handler(ctx, {
      url: newUrl,
      name: "acme corp", // lowercase to test case-insensitivity
      enabled: true,
      type: "general",
    });

    // Should update existing site, not create new one
    expect(id).toBe("sites-existing");
    expect(ctx.db.sites.size).toBe(1);
    const stored = ctx.db.sites.get(id);
    expect(stored?.url).toBe(newUrl);
    expect(stored?.name).toBe("acme corp");
  });

  it("creates new site when name does not match any existing", async () => {
    const existingSite = {
      _id: "sites-existing",
      url: "https://old.example.com/jobs",
      name: "Acme Corp",
      type: "general",
    };
    const ctx: any = {
      db: new FakeDb({ sites: [existingSite] }),
      runMutation: async () => null,
    };

    const handler = getHandler(upsertSite);
    const newUrl = "https://other.example.com/careers";

    // Call with different name and URL
    const id = await handler(ctx, {
      url: newUrl,
      name: "Other Company",
      enabled: true,
      type: "general",
    });

    // Should create a new site
    expect(id).not.toBe("sites-existing");
    expect(ctx.db.sites.size).toBe(2);
  });

  it("preserves query strings in stored site URLs", async () => {
    const ctx: any = {
      db: new FakeDb(),
      runMutation: async () => null,
    };

    const handler = getHandler(upsertSite);
    const url =
      "https://www.github.careers/careers-home/jobs?keywords=engineer&sortBy=relevance&limit=100";

    const id = await handler(ctx, { url, enabled: true, type: "general" });

    const stored = ctx.db.sites.get(id);
    expect(stored?.url).toBe(url);
  });

  it("preserves avature search URLs exactly when inserting from the admin UI", async () => {
    const ctx: any = {
      db: new FakeDb(),
      runMutation: async () => null,
    };

    const handler = getHandler(upsertSite);
    const url =
      "https://bloomberg.avature.net/careers/SearchJobs/engineer?1845=%5B162619%2C162522%2C162483%2C162484%2C162552%2C162508%2C162520%2C162535%5D&1845_format=3996&1686=%5B57029%5D&1686_format=2312&listFilterMode=1&jobRecordsPerPage=12&jobOffset=0";

    const id = await handler(ctx, { url, enabled: true, type: "general" });

    const stored = ctx.db.sites.get(id);
    expect(stored?.url).toBe(url);
  });
});

describe("bulkUpsertSites", () => {
  it("preserves query strings in stored site URLs", async () => {
    const ctx: any = {
      db: new FakeDb(),
      runMutation: async () => null,
    };

    const handler = getHandler(bulkUpsertSites);
    const url =
      "https://www.github.careers/careers-home/jobs?keywords=engineer&sortBy=relevance&limit=100";

    const [id] = await handler(ctx, {
      sites: [{ url, enabled: true, type: "general" }],
    });

    const stored = ctx.db.sites.get(id);
    expect(stored?.url).toBe(url);
  });

  it("preserves avature search URLs exactly when bulk inserting from the admin UI", async () => {
    const ctx: any = {
      db: new FakeDb(),
      runMutation: async () => null,
    };

    const handler = getHandler(bulkUpsertSites);
    const url =
      "https://bloomberg.avature.net/careers/SearchJobs/engineer?1845=%5B162619%2C162522%2C162483%2C162484%2C162552%2C162508%2C162520%2C162535%5D&1845_format=3996&1686=%5B57029%5D&1686_format=2312&listFilterMode=1&jobRecordsPerPage=12&jobOffset=0";

    const [id] = await handler(ctx, {
      sites: [{ url, enabled: true, type: "general" }],
    });

    const stored = ctx.db.sites.get(id);
    expect(stored?.url).toBe(url);
  });
});
