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

export const runAll = migrations.runner([
  internal.migrations.fixJobLocations,
]);

