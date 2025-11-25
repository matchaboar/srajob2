import { internal } from "./_generated/api";
import { splitLocation, formatLocationLabel } from "./location";
import { Migrations } from "@convex-dev/migrations";
import { components } from "./_generated/api.js";
import { DataModel } from "./_generated/dataModel.js";

export const migrations = new Migrations<DataModel>(components.migrations);
export const run = migrations.runner();

export const fixJobLocations = migrations.define({
  table: "jobs",
  migrateOne: async (ctx, doc) => {
    const { city, state } = splitLocation(doc.location || "");
    const location = formatLocationLabel(city, state, doc.location);

    const update: Record<string, any> = {};
    if (doc.city !== city) update.city = city;
    if (doc.state !== state) update.state = state;
    if (doc.location !== location) update.location = location;

    if (Object.keys(update).length > 0) {
      await ctx.db.patch(doc._id, update);
    }
  },
});

export const backfillScrapeMetadata = migrations.define({
  table: "jobs",
  migrateOne: async (ctx, doc) => {
    const update: Record<string, any> = {};
    if (doc.scrapedAt === undefined) {
      update.scrapedAt = doc.postedAt ?? Date.now();
    }
    if (doc.scrapedWith === undefined) {
      update.scrapedWith = null;
    }
    if (doc.workflowName === undefined) {
      update.workflowName = null;
    }
    if (doc.scrapedCostMilliCents === undefined) {
      update.scrapedCostMilliCents = null;
    }
    if (Object.keys(update).length > 0) {
      await ctx.db.patch(doc._id, update);
    }
  },
});

export const backfillScrapeRecords = migrations.define({
  table: "scrapes",
  migrateOne: async (ctx, doc) => {
    const update: Record<string, any> = {};
    if (doc.provider === undefined) {
      const provider = (doc.items as any)?.provider;
      update.provider = typeof provider === "string" ? provider : null;
    }
    if (doc.workflowName === undefined) {
      update.workflowName = null;
    }
    if (doc.costMilliCents === undefined) {
      const maybeCost = (doc.items as any)?.costMilliCents;
      update.costMilliCents = typeof maybeCost === "number" ? maybeCost : null;
    }
    if (Object.keys(update).length > 0) {
      await ctx.db.patch(doc._id, update);
    }
  },
});

export const runAll = migrations.runner([
  internal.migrations.fixJobLocations,
  internal.migrations.backfillScrapeMetadata,
  internal.migrations.backfillScrapeRecords,
]);
