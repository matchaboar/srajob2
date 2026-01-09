import { TableAggregate } from "@convex-dev/aggregate";
import type { Id } from "../_generated/dataModel";
import type { DataModel } from "../_generated/dataModel";
import { components } from "../_generated/api";

export const siteScheduleCounts = new TableAggregate<{
  Key: [Id<"scrape_schedules"> | null, Id<"sites">];
  DataModel: DataModel;
  TableName: "sites";
}>(components.aggregate, {
  sortKey: (doc) => [doc.scheduleId ?? null, doc._id],
});
