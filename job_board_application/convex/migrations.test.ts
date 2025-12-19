import { describe, expect, it, vi } from "vitest";
import {
  backfillScrapeRecords,
  buildScrapeRecordPatch,
  dedupeSitesImpl,
  deriveCostMilliCents,
  deriveProvider,
  retagGreenhouseJobsImpl,
} from "./migrations";

describe("backfillScrapeRecords", () => {
  it("normalizes null or missing costMilliCents to 0", async () => {
    const patches: any[] = [];
    const ctx: any = {
      db: {
        patch: vi.fn((id: string, payload: any) => patches.push({ id, payload })),
      },
    };

    // Simulate migrateOne logic with deriveCostMilliCents for two docs.
    const docs = [
      { _id: "scrape-1", costMilliCents: null, items: {} },
      { _id: "scrape-2", items: {} },
    ];

    for (const doc of docs) {
      const update: Record<string, any> = {};
      const cost = deriveCostMilliCents(doc);
      if (cost !== (doc as any).costMilliCents) {
        update.costMilliCents = cost;
      }
      if (Object.keys(update).length > 0) {
        await ctx.db.patch(doc._id, update);
      }
    }

    expect(patches).toHaveLength(2);
    expect(patches[0].payload.costMilliCents).toBe(0);
    expect(patches[1].payload.costMilliCents).toBe(0);
  });

  it("does not set workflowName to null when missing", () => {
    const patch = buildScrapeRecordPatch({
      provider: "firecrawl",
      costMilliCents: 0,
      items: {},
    });

    expect(patch).toEqual({});
  });

  it("clears null workflowName instead of writing null", () => {
    const patch = buildScrapeRecordPatch({
      workflowName: null,
      provider: "firecrawl",
      costMilliCents: 0,
      items: {},
    });

    expect(patch).toHaveProperty("workflowName", undefined);
  });
});

describe("deriveCostMilliCents", () => {
  it("prefers top-level numeric cost", () => {
    expect(deriveCostMilliCents({ costMilliCents: 10 })).toBe(10);
  });

  it("falls back to items.costMilliCents", () => {
    expect(deriveCostMilliCents({ items: { costMilliCents: 5 } })).toBe(5);
  });

  it("returns 0 for null/missing", () => {
    expect(deriveCostMilliCents({ costMilliCents: null })).toBe(0);
    expect(deriveCostMilliCents({})).toBe(0);
  });
});

describe("deriveProvider", () => {
  it("prefers existing provider when valid", () => {
    expect(deriveProvider({ provider: "firecrawl" })).toBe("firecrawl");
  });

  it("falls back to items.provider when missing", () => {
    expect(deriveProvider({ items: { provider: "fetchfox" } })).toBe("fetchfox");
  });

  it("returns unknown when null or empty", () => {
    expect(deriveProvider({ provider: null })).toBe("unknown");
    expect(deriveProvider({})).toBe("unknown");
  });
});

describe("dedupeSites", () => {
  it("merges greenhouse board variants and disables duplicates", async () => {
    const sites = [
      {
        _id: "a",
        url: "https://boards-api.greenhouse.io/v1/boards/stubhubinc/jobs",
        enabled: true,
        failed: false,
        completed: false,
        type: "greenhouse",
        _creationTime: 1,
      },
      {
        _id: "b",
        url: "https://api.greenhouse.io/v1/boards/stubhubinc/jobs",
        enabled: false,
        failed: false,
        completed: false,
        type: "greenhouse",
        _creationTime: 2,
      },
    ];

    const patches: any[] = [];
    const ctx: any = {
      db: {
        query: (table: string) => {
          if (table !== "sites") throw new Error("expected sites table");
          return { collect: async () => sites };
        },
        patch: async (id: string, payload: any) => patches.push({ id, payload }),
      },
    };

    await dedupeSitesImpl(ctx);

    expect(patches.find((p) => p.id === "a")?.payload.url).toBe("https://api.greenhouse.io/v1/boards/stubhubinc/jobs");
    const dupPatch = patches.find((p) => p.id === "b");
    expect(dupPatch?.payload.enabled).toBe(false);
    expect(dupPatch?.payload.failed).toBe(true);
    expect(dupPatch?.payload.lastError).toContain("duplicate_of:a");
  });
});

describe("retagGreenhouseJobs", () => {
  it("retags greenhouse jobs using slug-specific alias", async () => {
    const patches: any[] = [];
    const ctx: any = {
      db: {
        query: (table: string) => {
          if (table === "domain_aliases") {
            return {
              collect: async () => [
                { domain: "stubhubinc.greenhouse.io", alias: "Stubhub" },
                { domain: "coupang.greenhouse.io", alias: "Coupang" },
              ],
            };
          }
          if (table === "jobs") {
            return {
              collect: async () => [
                {
                  _id: "job-1",
                  company: "Coupang",
                  url: "https://job-boards.eu.greenhouse.io/stubhubinc/jobs/4648156101",
                },
              ],
            };
          }
          throw new Error(`Unexpected table ${table}`);
        },
        patch: async (id: string, payload: any) => patches.push({ id, payload }),
      },
    };

    await retagGreenhouseJobsImpl(ctx);

    expect(patches).toHaveLength(1);
    expect(patches[0]).toEqual({
      id: "job-1",
      payload: { company: "Stubhub" },
    });
  });
});
