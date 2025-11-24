import { mutation, query } from "./_generated/server";
import { v } from "convex/values";
import type { Id } from "./_generated/dataModel";

const SCHEDULE_KEY = "scrape_schedule";
const DEFAULT_SCHEDULE = {
  mode: "daily" as const,
  time: "08:00",
  timezone: "MST",
  intervalMinutes: 24 * 60,
  name: "scrape-every-15-mins",
  catchupWindowHours: 12,
  overlap: "skip",
  workflow: "ScrapeWorkflow",
  taskQueue: "scraper-task-queue",
};

export const updateStatus = mutation({
    args: {
        workerId: v.string(),
        hostname: v.string(),
        temporalAddress: v.string(),
        temporalNamespace: v.string(),
        taskQueue: v.string(),
        workflows: v.array(
            v.object({
                id: v.string(),
                type: v.string(),
                status: v.string(),
                startTime: v.string(),
            })
        ),
        noWorkflowsReason: v.optional(v.string()),
    },
    handler: async (ctx, args) => {
        const now = Date.now();

        // Find existing worker record
        const existing = await ctx.db
            .query("temporal_status")
            .withIndex("by_worker_id", (q) => q.eq("workerId", args.workerId))
            .first();

        if (existing) {
            // Update existing worker
            await ctx.db.patch(existing._id, {
                hostname: args.hostname,
                temporalAddress: args.temporalAddress,
                temporalNamespace: args.temporalNamespace,
                taskQueue: args.taskQueue,
                workflows: args.workflows,
                noWorkflowsReason: args.noWorkflowsReason,
                lastHeartbeat: now,
            });
        } else {
            // Insert new worker
            await ctx.db.insert("temporal_status", {
                workerId: args.workerId,
                hostname: args.hostname,
                temporalAddress: args.temporalAddress,
                temporalNamespace: args.temporalNamespace,
                taskQueue: args.taskQueue,
                workflows: args.workflows,
                noWorkflowsReason: args.noWorkflowsReason,
                lastHeartbeat: now,
            });
        }
    },
});

// Get all active workers (heartbeat within last 90 seconds)
export const getActiveWorkers = query({
    args: {},
    handler: async (ctx) => {
        const now = Date.now();
        const staleThreshold = now - 90 * 1000; // 90 seconds (3x update interval)

        const allWorkers = await ctx.db.query("temporal_status").collect();
        return allWorkers.filter(w => w.lastHeartbeat >= staleThreshold);
    },
});

// Get all stale workers (no heartbeat in last 90 seconds)
export const getStaleWorkers = query({
    args: {},
    handler: async (ctx) => {
        const now = Date.now();
        const staleThreshold = now - 90 * 1000; // 90 seconds (3x update interval)

        const allWorkers = await ctx.db.query("temporal_status").collect();
        return allWorkers.filter(w => w.lastHeartbeat < staleThreshold);
    },
});

// Get single worker by ID
export const getWorker = query({
    args: { workerId: v.string() },
    handler: async (ctx, args) => {
        return await ctx.db
            .query("temporal_status")
            .withIndex("by_worker_id", (q) => q.eq("workerId", args.workerId))
            .first();
    },
});

// Clean up very old stale workers (older than 24 hours)
export const cleanupOldWorkers = mutation({
    args: {},
    handler: async (ctx) => {
        const now = Date.now();
        const cleanupThreshold = now - 24 * 60 * 60 * 1000; // 24 hours

        const oldWorkers = await ctx.db
            .query("temporal_status")
            .withIndex("by_heartbeat")
            .filter((q) => q.lt(q.field("lastHeartbeat"), cleanupThreshold))
            .collect();

        for (const worker of oldWorkers) {
            await ctx.db.delete(worker._id);
        }

        return { deleted: oldWorkers.length };
    },
});

// Clear all worker records (for schema migration)
export const clearAllWorkers = mutation({
    args: {},
    handler: async (ctx) => {
        const allWorkers = await ctx.db.query("temporal_status").collect();
        for (const worker of allWorkers) {
            await ctx.db.delete(worker._id);
        }
    return { deleted: allWorkers.length };
  },
});

export const recordWorkflowRun = mutation({
    args: {
        runId: v.string(),
        workflowId: v.string(),
        workflowName: v.optional(v.string()),
        status: v.string(),
        startedAt: v.number(),
        completedAt: v.optional(v.number()),
        siteUrls: v.array(v.string()),
        sitesProcessed: v.optional(v.number()),
        jobsScraped: v.optional(v.number()),
        workerId: v.optional(v.string()),
        taskQueue: v.optional(v.string()),
        error: v.optional(v.union(v.string(), v.null())),
    },
    handler: async (ctx, args) => {
        const existing = await ctx.db
            .query("workflow_runs")
            .withIndex("by_run", (q) => q.eq("runId", args.runId))
            .first();

        if (existing) {
            const patch: any = { ...args };
            // Avoid storing null for error to satisfy TS/Convex types
            if (patch.error === null) delete patch.error;
            await ctx.db.patch(existing._id as Id<"workflow_runs">, patch);
            return existing._id;
        }

        const insertArgs: any = { ...args };
        if (insertArgs.error === null) delete insertArgs.error;
        return await ctx.db.insert("workflow_runs", insertArgs);
    },
});

export const listWorkflowRunsByUrl = query({
    args: {
        url: v.string(),
        limit: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        const lim = args.limit ?? 20;
        if (!args.url) return [];

        const runs = await ctx.db.query("workflow_runs").collect();
        return runs
            .filter((r: any) => Array.isArray(r.siteUrls) && r.siteUrls.includes(args.url))
            .sort((a: any, b: any) => (b.startedAt ?? 0) - (a.startedAt ?? 0))
            .slice(0, lim);
    },
});

export const listWorkflowRuns = query({
    args: { limit: v.optional(v.number()) },
    handler: async (ctx, args) => {
        const lim = args.limit ?? 50;
        const runs = await ctx.db.query("workflow_runs").collect();
        return runs
            .sort((a: any, b: any) => (b.startedAt ?? 0) - (a.startedAt ?? 0))
            .slice(0, lim);
    },
});

// Expose schedule configuration (kept in sync with Temporal schedule creator)
export const getScrapeSchedule = query({
    args: {},
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

export const triggerScrapeNow = mutation({
    args: {
        url: v.string(),
    },
    handler: async (_ctx, args) => {
        // The worker polls Temporal; we just return a directive so the UI can request it.
        // The actual immediate run should be triggered by a Temporal client, not Convex.
        return { requested: true, url: args.url };
    },
});

export const setScrapeSchedule = mutation({
    args: {
        mode: v.union(v.literal("daily"), v.literal("interval")),
        time: v.optional(v.string()),
        timezone: v.optional(v.string()),
        intervalMinutes: v.optional(v.number()),
    },
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
            return existing._id;
        }

        return await ctx.db.insert("schedule_config", {
            key: SCHEDULE_KEY,
            ...config,
            createdAt: now,
        });
    },
});
