import { internal } from "./_generated/api";
import { splitLocation, formatLocationLabel, deriveLocationFields } from "./location";
import { Migrations } from "@convex-dev/migrations";
import { components } from "./_generated/api.js";
import { DataModel } from "./_generated/dataModel.js";

export const migrations = new Migrations<DataModel>(components.migrations);
export const run = migrations.runner();

export const fixJobLocations = migrations.define({
  table: "jobs",
  migrateOne: async (ctx, doc) => {
    const locationInfo = deriveLocationFields({ locations: (doc as any).locations, location: doc.location });
    const { city, state, primaryLocation } = locationInfo;
    const location = formatLocationLabel(city, state, primaryLocation);

    const update: Record<string, any> = {};
    if (doc.city !== city) update.city = city;
    if (doc.state !== state) update.state = state;
    if (doc.location !== location) update.location = location;
    if (!Array.isArray((doc as any).locations) || JSON.stringify((doc as any).locations) !== JSON.stringify(locationInfo.locations)) {
      update.locations = locationInfo.locations;
    }
    if (!Array.isArray((doc as any).countries) || JSON.stringify((doc as any).countries) !== JSON.stringify(locationInfo.countries)) {
      update.countries = locationInfo.countries;
    }
    if ((doc as any).country !== locationInfo.country) {
      update.country = locationInfo.country;
    }
    if (!Array.isArray((doc as any).locationStates) || JSON.stringify((doc as any).locationStates) !== JSON.stringify(locationInfo.locationStates)) {
      update.locationStates = locationInfo.locationStates;
    }
    if ((doc as any).locationSearch !== locationInfo.locationSearch) {
      update.locationSearch = locationInfo.locationSearch;
    }

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
    const costVal = deriveCostMilliCents(doc);
    if (costVal !== (doc as any).costMilliCents) {
      update.costMilliCents = costVal;
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

export const deriveCostMilliCents = (doc: any): number => {
  const costVal = doc?.costMilliCents;
  if (typeof costVal === "number") return costVal;
  const fromItems = doc?.items?.costMilliCents;
  if (typeof fromItems === "number") return fromItems;
  return 0;
};
