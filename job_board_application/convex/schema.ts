import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";
import { authTables } from "@convex-dev/auth/server";

const applicationTables = {
  jobs: defineTable({
    title: v.string(),
    company: v.string(),
    location: v.string(),
    locations: v.optional(v.array(v.string())),
    countries: v.optional(v.array(v.string())),
    country: v.optional(v.string()),
    locationStates: v.optional(v.array(v.string())),
    locationSearch: v.optional(v.string()),
    city: v.optional(v.string()),
    state: v.optional(v.string()),
    remote: v.boolean(),
    level: v.union(v.literal("junior"), v.literal("mid"), v.literal("senior"), v.literal("staff")),
    totalCompensation: v.number(),
    compensationUnknown: v.optional(v.boolean()),
    compensationReason: v.optional(v.string()),
    currencyCode: v.optional(v.string()),
    description: v.optional(v.string()),
    scrapedWith: v.optional(v.string()),
    workflowName: v.optional(v.string()),
    scrapedCostMilliCents: v.optional(v.number()),
    heuristicAttempts: v.optional(v.number()),
    heuristicLastTried: v.optional(v.number()),
    heuristicVersion: v.optional(v.number()),
    url: v.string(),
    postedAt: v.number(),
    scrapedAt: v.optional(v.number()),
    // Optional flag to identify internal/test rows not meant for UI
    test: v.optional(v.boolean()),
  })
    .index("by_posted_at", ["postedAt"])
    .index("by_scraped_at", ["scrapedAt"])
    .index("by_company", ["company"])
    .index("by_state_posted", ["state", "postedAt"])
    .index("by_country_posted", ["country", "postedAt"])
    .index("by_company_posted", ["company", "postedAt"])
    .index("by_title_posted", ["title", "postedAt"])
    .index("by_url", ["url"])
    .searchIndex("search_locations", {
      searchField: "locationSearch",
      filterFields: ["remote", "level", "state"],
    })
    .searchIndex("search_title", {
      searchField: "title",
      filterFields: ["remote", "level", "state"],
    })
    .searchIndex("search_company", {
      searchField: "company",
    }),

  job_details: defineTable({
    jobId: v.id("jobs"),
    description: v.optional(v.string()),
    scrapedWith: v.optional(v.string()),
    workflowName: v.optional(v.string()),
    scrapedCostMilliCents: v.optional(v.number()),
    heuristicAttempts: v.optional(v.number()),
    heuristicLastTried: v.optional(v.number()),
    heuristicVersion: v.optional(v.number()),
  }).index("by_job", ["jobId"]),

  company_profiles: defineTable({
    slug: v.string(),
    name: v.string(),
    aliases: v.optional(v.array(v.string())),
    domains: v.optional(v.array(v.string())),
    updatedAt: v.number(),
    createdAt: v.number(),
  })
    .index("by_slug", ["slug"])
    .index("by_name", ["name"]),

  domain_aliases: defineTable({
    domain: v.string(),
    alias: v.string(),
    derivedName: v.string(),
    updatedAt: v.number(),
    createdAt: v.number(),
  }).index("by_domain", ["domain"]),

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
    country: v.optional(v.string()),
    useSearch: v.optional(v.boolean()),
    companies: v.optional(v.array(v.string())),
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
    type: v.optional(v.union(v.literal("general"), v.literal("greenhouse"), v.literal("avature"))),
    scrapeProvider: v.optional(
      v.union(
        v.literal("fetchfox"),
        v.literal("firecrawl"),
        v.literal("spidercloud"),
        v.literal("fetchfox_spidercloud")
      )
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
    siteId: v.optional(v.id("sites")),
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
  })
    .index("by_source", ["sourceUrl"])
    .index("by_source_completed", ["sourceUrl", "completedAt"])
    .index("by_completedAt", ["completedAt"])
    .index("by_startedAt", ["startedAt"])
    .index("by_site", ["siteId"]),

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

  workflow_run_sites: defineTable({
    runId: v.string(),
    workflowId: v.string(),
    workflowName: v.optional(v.string()),
    status: v.string(),
    startedAt: v.number(),
    completedAt: v.optional(v.number()),
    siteUrl: v.string(),
    workerId: v.optional(v.string()),
    taskQueue: v.optional(v.string()),
  })
    .index("by_run", ["runId"])
    .index("by_site", ["siteUrl", "startedAt"]),

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

  ignored_jobs: defineTable({
    url: v.string(),
    sourceUrl: v.optional(v.string()),
    reason: v.optional(v.string()),
    provider: v.optional(v.string()),
    workflowName: v.optional(v.string()),
    details: v.optional(v.any()),
    title: v.optional(v.string()),
    description: v.optional(v.string()),
    createdAt: v.number(),
  })
    .index("by_url", ["url"])
    .index("by_source", ["sourceUrl", "createdAt"])
    .index("by_created_at", ["createdAt"]),

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
