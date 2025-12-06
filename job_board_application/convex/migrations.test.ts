import { describe, expect, it, vi } from "vitest";
import { backfillScrapeRecords, deriveCostMilliCents } from "./migrations";

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
