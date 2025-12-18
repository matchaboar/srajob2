import { mutation, query } from "./_generated/server";
import { v } from "convex/values";

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
    // Scratchpad storage is disabled; use OTLP/PostHog logs instead.
    void ctx;
    void args;
    return { disabled: true };
  },
});

export const listByRun = query({
  args: { runId: v.string(), limit: v.optional(v.number()) },
  handler: async (ctx, args) => {
    // Scratchpad storage is disabled; return empty results.
    void ctx;
    void args;
    return [];
  },
});

export const listRecent = query({
  args: { limit: v.optional(v.number()) },
  handler: async (ctx, args) => {
    // Scratchpad storage is disabled; return empty results.
    void ctx;
    void args;
    return [];
  },
});
