import { describe, expect, it, vi } from "vitest";
import {
  buildScrapeRecordPatch,
  dedupeSitesImpl,
  deriveCostMilliCents,
  deriveProvider,
  deriveScrapeQueueScheduledAt,
  retagGreenhouseJobsImpl,
  repairJobIdReferencesImpl,
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

describe("deriveScrapeQueueScheduledAt", () => {
  it("uses createdAt when present", () => {
    expect(deriveScrapeQueueScheduledAt({ createdAt: 123, updatedAt: 456 })).toBe(123);
  });

  it("falls back to updatedAt when createdAt missing", () => {
    expect(deriveScrapeQueueScheduledAt({ updatedAt: 456 })).toBe(456);
  });

  it("falls back to Date.now when timestamps missing", () => {
    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(999);
    expect(deriveScrapeQueueScheduledAt({})).toBe(999);
    nowSpy.mockRestore();
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

  it("does not strip query strings when normalizing general sites", async () => {
    const sites = [
      {
        _id: "a",
        name: "GitHub",
        url: "https://www.github.careers/careers-home/jobs?keywords=engineer&sortBy=relevance&limit=100",
        enabled: true,
        failed: false,
        completed: false,
        type: "general",
        _creationTime: 1,
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

    expect(patches).toHaveLength(0);
  });
});

describe("repairJobIdReferences", () => {
  it("repairs job_details/applications that point at job_details ids", async () => {
    const jobs = [{ _id: "job-1", title: "Engineer", company: "Example" }];
    const jobDetails = [
      { _id: "detail-1", jobId: "job-1", description: "ok" },
      { _id: "detail-2", jobId: "detail-1", description: "bad" },
    ];
    const applications = [{ _id: "app-1", jobId: "detail-1", userId: "user-1", status: "applied", appliedAt: 0 }];

    const patches: any[] = [];
    const deletes: any[] = [];
    const ctx: any = {
      db: {
        get: async (id: string) => {
          return jobs.find((row) => row._id === id)
            ?? jobDetails.find((row) => row._id === id)
            ?? applications.find((row) => row._id === id)
            ?? null;
        },
        query: (table: string) => {
          if (table === "job_details") return { collect: async () => jobDetails };
          if (table === "applications") return { collect: async () => applications };
          throw new Error(`unexpected table ${table}`);
        },
        patch: async (id: string, payload: any) => patches.push({ id, payload }),
        delete: async (id: string) => deletes.push(id),
      },
    };

    const result = await repairJobIdReferencesImpl(ctx);

    expect(result.jobDetailsFixed).toBe(1);
    expect(result.applicationsFixed).toBe(1);
    expect(patches).toEqual([
      { id: "detail-2", payload: { jobId: "job-1" } },
      { id: "app-1", payload: { jobId: "job-1" } },
    ]);
    expect(deletes).toHaveLength(0);
  });

  it("deletes rows that cannot be resolved to a job", async () => {
    const jobDetails = [{ _id: "detail-1", jobId: "missing-job", description: "bad" }];
    const applications = [{ _id: "app-1", jobId: "missing-job", userId: "user-1", status: "applied", appliedAt: 0 }];

    const deletes: any[] = [];
    const ctx: any = {
      db: {
        get: async (_id: string) => null,
        query: (table: string) => {
          if (table === "job_details") return { collect: async () => jobDetails };
          if (table === "applications") return { collect: async () => applications };
          throw new Error(`unexpected table ${table}`);
        },
        patch: async () => {},
        delete: async (id: string) => deletes.push(id),
      },
    };

    const result = await repairJobIdReferencesImpl(ctx);

    expect(result.jobDetailsDeleted).toBe(1);
    expect(result.applicationsDeleted).toBe(1);
    expect(deletes).toEqual(["detail-1", "app-1"]);
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
      payload: { company: "Stubhub", companyKey: "stubhub" },
    });
  });
});
