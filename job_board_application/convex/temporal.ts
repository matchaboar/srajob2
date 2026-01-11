import { mutation, query } from "./_generated/server";
import { v } from "convex/values";
import type { Id } from "./_generated/dataModel";

const SCHEDULE_KEY = "scrape_schedule";
const DEFAULT_SCHEDULE = {
  mode: "daily" as const,
  time: "08:00",
  timezone: "MST",
  intervalMinutes: 24 * 60,
  name: "scrape-every-10-secs",
  catchupWindowHours: 12,
  overlap: "skip",
  workflow: "ScrapeWorkflow",
  taskQueue: "scraper-task-queue",
};

export const getScrapeSchedule = query({
  args: {},
  returns: v.object({
    mode: v.union(v.literal("daily"), v.literal("interval")),
    time: v.optional(v.string()),
    timezone: v.optional(v.string()),
    intervalMinutes: v.number(),
    name: v.string(),
    catchupWindowHours: v.number(),
    overlap: v.string(),
    workflow: v.string(),
    taskQueue: v.string(),
  }),
  handler: async (ctx) => {
    const existing = await ctx.db
      .query("schedule_config")
      .withIndex("by_key", (q) => q.eq("key", SCHEDULE_KEY))
      .first();

    if (!existing) {
      return DEFAULT_SCHEDULE;
    }

    return {
      ...DEFAULT_SCHEDULE,
      mode: existing.mode,
      time: existing.time ?? DEFAULT_SCHEDULE.time,
      timezone: existing.timezone ?? DEFAULT_SCHEDULE.timezone,
      intervalMinutes: existing.intervalMinutes ?? DEFAULT_SCHEDULE.intervalMinutes,
      name: existing.key,
    };
  },
});

export const setScrapeSchedule = mutation({
  args: {
    mode: v.union(v.literal("daily"), v.literal("interval")),
    time: v.optional(v.string()),
    timezone: v.optional(v.string()),
    intervalMinutes: v.optional(v.number()),
  },
  returns: v.id("schedule_config"),
  handler: async (ctx, args) => {
    const now = Date.now();
    const config = {
      mode: args.mode,
      time: args.mode === "daily" ? (args.time ?? DEFAULT_SCHEDULE.time) : undefined,
      timezone: args.mode === "daily" ? (args.timezone ?? DEFAULT_SCHEDULE.timezone) : undefined,
      intervalMinutes:
        args.mode === "interval"
          ? args.intervalMinutes ?? DEFAULT_SCHEDULE.intervalMinutes
          : DEFAULT_SCHEDULE.intervalMinutes,
      updatedAt: now,
    };

    const existing = await ctx.db
      .query("schedule_config")
      .withIndex("by_key", (q) => q.eq("key", SCHEDULE_KEY))
      .first();

    if (existing) {
      await ctx.db.patch(existing._id as Id<"schedule_config">, config);
      return existing._id as Id<"schedule_config">;
    }

    return await ctx.db.insert("schedule_config", {
      key: SCHEDULE_KEY,
      ...config,
      createdAt: now,
    });
  },
});
