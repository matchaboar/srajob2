import { mutation, query } from "./_generated/server";
import { v } from "convex/values";

const SCRATCH_LIMIT_DEFAULT = 25;

const sanitizeData = (value: any) => {
  if (value === null || value === undefined) return undefined;

  try {
    const serialized = JSON.stringify(value);
    if (serialized.length <= 1200) return value;
    return `${serialized.slice(0, 1200)}… (+${serialized.length - 1200} chars)`;
  } catch {
    const str = String(value);
    return str.length > 1200 ? `${str.slice(0, 1200)}… (+${str.length - 1200} chars)` : str;
  }
};

export const append = mutation({
  args: {
    runId: v.optional(v.string()),
    workflowId: v.optional(v.string()),
    workflowName: v.optional(v.string()),
    siteUrl: v.optional(v.string()),
    siteId: v.optional(v.id("sites")),
    event: v.string(),
    message: v.optional(v.string()),
    data: v.optional(v.any()),
    level: v.optional(v.union(v.literal("info"), v.literal("warn"), v.literal("error"))),
    createdAt: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const createdAt = args.createdAt ?? Date.now();
    const sanitized = {
      ...args,
      createdAt,
      data: sanitizeData(args.data),
    };
    return await ctx.db.insert("scratchpad_entries", sanitized);
  },
});

export const listByRun = query({
  args: { runId: v.string(), limit: v.optional(v.number()) },
  handler: async (ctx, args) => {
    const limit = args.limit ?? SCRATCH_LIMIT_DEFAULT;
    const entries = await ctx.db
      .query("scratchpad_entries")
      .withIndex("by_run", (q) => q.eq("runId", args.runId))
      .collect();

    return entries
      .sort((a: any, b: any) => (b.createdAt ?? 0) - (a.createdAt ?? 0))
      .slice(0, limit);
  },
});

export const listRecent = query({
  args: { limit: v.optional(v.number()) },
  handler: async (ctx, args) => {
    const limit = args.limit ?? SCRATCH_LIMIT_DEFAULT;
    const entries = await ctx.db.query("scratchpad_entries").collect();
    return entries
      .sort((a: any, b: any) => (b.createdAt ?? 0) - (a.createdAt ?? 0))
      .slice(0, limit);
  },
});
