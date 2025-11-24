import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";
import { authTables } from "@convex-dev/auth/server";

const applicationTables = {
  jobs: defineTable({
    title: v.string(),
    company: v.string(),
    description: v.string(),
    location: v.string(),
    city: v.optional(v.string()),
    state: v.optional(v.string()),
    remote: v.boolean(),
    level: v.union(v.literal("junior"), v.literal("mid"), v.literal("senior"), v.literal("staff")),
    totalCompensation: v.number(),
    url: v.string(),
    postedAt: v.number(),
    // Optional flag to identify internal/test rows not meant for UI
    test: v.optional(v.boolean()),
  })
    .index("by_posted_at", ["postedAt"])
    .index("by_state_posted", ["state", "postedAt"])
    .searchIndex("search_title", {
      searchField: "title",
      filterFields: ["remote", "level", "state"],
    }),

  saved_filters: defineTable({
    userId: v.id("users"),
    name: v.string(),
    search: v.optional(v.string()),
    remote: v.optional(v.boolean()),
    includeRemote: v.optional(v.boolean()),
    state: v.optional(v.string()),
    level: v.optional(v.union(v.literal("junior"), v.literal("mid"), v.literal("senior"), v.literal("staff"))),
    minCompensation: v.optional(v.number()),
    maxCompensation: v.optional(v.number()),
    isSelected: v.boolean(),
    createdAt: v.number(),
  })
    .index("by_user", ["userId"])
    .index("by_user_selected", ["userId", "isSelected"]),

  applications: defineTable({
    userId: v.id("users"),
    jobId: v.id("jobs"),
    status: v.union(v.literal("applied"), v.literal("rejected")),
    appliedAt: v.number(),
  })
    .index("by_user", ["userId"])
    .index("by_job", ["jobId"])
    .index("by_user_and_job", ["userId", "jobId"]),

  // List of websites to scrape for jobs
  sites: defineTable({
    name: v.optional(v.string()),
    url: v.string(),
    // Optional pattern for detail pages (e.g., "https://example.com/jobs/**")
    pattern: v.optional(v.string()),
    // Optional reusable schedule reference
    scheduleId: v.optional(v.id("scrape_schedules")),
    enabled: v.boolean(),
    // Optional timestamp of the last successful run
    lastRunAt: v.optional(v.number()),
    // Simple cooperative locking for scraper workers
    lockedBy: v.optional(v.string()),
    lockExpiresAt: v.optional(v.number()),
    // Optional completion flag if treating a site as a one-off job
    completed: v.optional(v.boolean()),
    // If true, site is in a failed state and excluded from auto-leasing until manually retried
    failed: v.optional(v.boolean()),
    // Failure tracking so stuck jobs get retried and diagnosable
    failCount: v.optional(v.number()),
    lastFailureAt: v.optional(v.number()),
    lastError: v.optional(v.string()),
    manualTriggerAt: v.optional(v.number()),
  })
    .index("by_enabled", ["enabled"])
    .index("by_schedule", ["scheduleId"]),

  // Raw scrape results captured by the scraper
  scrapes: defineTable({
    sourceUrl: v.string(),
    pattern: v.optional(v.string()),
    startedAt: v.number(),
    completedAt: v.number(),
    items: v.any(),
  }).index("by_source", ["sourceUrl"]),

  resumes: defineTable({
    userId: v.id("users"),
    data: v.any(),
  }).index("by_user", ["userId"]),

  form_fill_queue: defineTable(v.any()).index("by_user", ["userId"]),

  temporal_status: defineTable({
    // Worker identification
    workerId: v.string(),
    hostname: v.string(),

    // Temporal connection details
    temporalAddress: v.string(),
    temporalNamespace: v.string(),
    taskQueue: v.string(),

    // Health check
    lastHeartbeat: v.number(),

    // Workflow status
    workflows: v.array(
      v.object({
        id: v.string(),
        type: v.string(),
        status: v.string(),
        startTime: v.string(),
      })
    ),

    // Reason when no workflows are running
    noWorkflowsReason: v.optional(v.string()),
  })
    .index("by_worker_id", ["workerId"])
    .index("by_heartbeat", ["lastHeartbeat"]),

  workflow_runs: defineTable({
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
    error: v.optional(v.string()),
  })
    .index("by_run", ["runId"])
    .index("by_started", ["startedAt"]),

  schedule_config: defineTable({
    key: v.string(),
    mode: v.union(v.literal("daily"), v.literal("interval")),
    time: v.optional(v.string()), // HH:MM 24h format
    timezone: v.optional(v.string()),
    intervalMinutes: v.optional(v.number()),
    createdAt: v.number(),
    updatedAt: v.number(),
  }).index("by_key", ["key"]),

  // Reusable scrape schedules for sites
  scrape_schedules: defineTable({
    name: v.string(),
    days: v.array(
      v.union(
        v.literal("mon"),
        v.literal("tue"),
        v.literal("wed"),
        v.literal("thu"),
        v.literal("fri"),
        v.literal("sat"),
        v.literal("sun")
      )
    ),
    startTime: v.string(), // HH:MM 24h format
    intervalMinutes: v.number(),
    timezone: v.optional(v.string()),
    createdAt: v.number(),
    updatedAt: v.number(),
  }).index("by_name", ["name"]),
};

export default defineSchema({
  ...authTables,
  ...applicationTables,
});
