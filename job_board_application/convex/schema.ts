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
    compensationUnknown: v.optional(v.boolean()),
    compensationReason: v.optional(v.string()),
    currencyCode: v.optional(v.string()),
    url: v.string(),
    postedAt: v.number(),
    scrapedAt: v.optional(v.number()),
    scrapedWith: v.optional(v.string()),
    workflowName: v.optional(v.string()),
    scrapedCostMilliCents: v.optional(v.number()),
    heuristicAttempts: v.optional(v.number()),
    heuristicLastTried: v.optional(v.number()),
    // Optional flag to identify internal/test rows not meant for UI
    test: v.optional(v.boolean()),
  })
    .index("by_posted_at", ["postedAt"])
    .index("by_state_posted", ["state", "postedAt"])
    .index("by_url", ["url"])
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
    hideUnknownCompensation: v.optional(v.boolean()),
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
    type: v.optional(v.union(v.literal("general"), v.literal("greenhouse"))),
    scrapeProvider: v.optional(
      v.union(v.literal("fetchfox"), v.literal("firecrawl"), v.literal("spidercloud"))
    ),
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
    provider: v.optional(v.string()),
    workflowName: v.optional(v.string()),
    costMilliCents: v.optional(v.number()),
    // Optional metadata for richer audit/history views
    jobBoardJobId: v.optional(v.string()),
    batchId: v.optional(v.string()),
    workflowId: v.optional(v.string()),
    workflowType: v.optional(v.string()),
    response: v.optional(v.any()),
    asyncState: v.optional(v.string()),
    asyncResponse: v.optional(v.any()),
    subUrls: v.optional(v.array(v.string())),
    // Canonical snapshot of the outbound request to the provider (body, headers, etc.)
    request: v.optional(v.any()),
    // Provider-specific request payload (Firecrawl, FetchFox, etc.)
    providerRequest: v.optional(v.any()),
  }).index("by_source", ["sourceUrl"]),

  firecrawl_webhooks: defineTable({
    jobId: v.string(),
    event: v.string(),
    status: v.optional(v.string()),
    success: v.optional(v.boolean()),
    sourceUrl: v.optional(v.string()),
    siteId: v.optional(v.string()),
    statusUrl: v.optional(v.string()),
    payload: v.any(),
    metadata: v.optional(v.any()),
    receivedAt: v.number(),
    processed: v.boolean(),
    processedAt: v.optional(v.number()),
    error: v.optional(v.string()),
  })
    .index("by_job", ["jobId"])
    .index("by_processed", ["processed"]),

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

  // Queue of individual job URLs discovered from site listings (e.g., Greenhouse boards)
  scrape_url_queue: defineTable({
    url: v.string(),
    sourceUrl: v.string(),
    provider: v.optional(v.string()),
    siteId: v.optional(v.id("sites")),
    pattern: v.optional(v.string()),
    status: v.union(v.literal("pending"), v.literal("processing"), v.literal("completed"), v.literal("failed")),
    attempts: v.optional(v.number()),
    lastError: v.optional(v.string()),
    createdAt: v.number(),
    updatedAt: v.number(),
    completedAt: v.optional(v.number()),
  })
    .index("by_url", ["url"])
    .index("by_status", ["status"])
    .index("by_site_status", ["siteId", "status"]),

  job_detail_configs: defineTable({
    domain: v.string(),
    field: v.string(),
    regex: v.string(),
    successCount: v.number(),
    lastSuccessAt: v.optional(v.number()),
    createdAt: v.number(),
  })
    .index("by_domain", ["domain"])
    .index("by_domain_field", ["domain", "field"]),

  job_detail_rate_limits: defineTable({
    domain: v.string(),
    maxPerMinute: v.number(),
    lastWindowStart: v.number(),
    sentInWindow: v.number(),
  }).index("by_domain", ["domain"]),

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

  scratchpad_entries: defineTable({
    runId: v.optional(v.string()),
    workflowId: v.optional(v.string()),
    workflowName: v.optional(v.string()),
    siteUrl: v.optional(v.string()),
    siteId: v.optional(v.id("sites")),
    event: v.string(),
    message: v.optional(v.string()),
    data: v.optional(v.any()),
    level: v.optional(v.union(v.literal("info"), v.literal("warn"), v.literal("error"))),
    createdAt: v.number(),
  })
    .index("by_run", ["runId"])
    .index("by_site", ["siteUrl"])
    .index("by_workflow", ["workflowName"])
    .index("by_created", ["createdAt"]),

  // Centralized log of scraper failures (e.g., invalid Firecrawl job ids)
  scrape_errors: defineTable({
    jobId: v.optional(v.string()),
    sourceUrl: v.optional(v.string()),
    siteId: v.optional(v.string()),
    event: v.optional(v.string()),
    status: v.optional(v.string()),
    error: v.string(),
    metadata: v.optional(v.any()),
    payload: v.optional(v.any()),
    createdAt: v.number(),
  })
    .index("by_job", ["jobId"])
    .index("by_created", ["createdAt"]),

  // Manual run-now requests initiated from the admin UI
  run_requests: defineTable({
    siteId: v.id("sites"),
    siteUrl: v.string(),
    status: v.union(v.literal("pending"), v.literal("processing"), v.literal("done")),
    createdAt: v.number(),
    expectedEta: v.optional(v.number()),
    completedAt: v.optional(v.number()),
  })
    .index("by_status", ["status"])
    .index("by_created", ["createdAt"]),
};

export default defineSchema({
  ...authTables,
  ...applicationTables,
});
