import { query, mutation } from "./_generated/server";
import { api } from "./_generated/api";
import { v } from "convex/values";

export const listSuccessfulSites = query({
  args: { limit: v.optional(v.number()) },
  returns: v.array(
    v.object({
      _id: v.id("sites"),
      name: v.optional(v.string()),
      url: v.string(),
      pattern: v.optional(v.string()),
      lastRunAt: v.optional(v.number()),
    })
  ),
  handler: async (ctx, args) => {
    const limit = args.limit ?? 50;
    const sites = await ctx.db.query("sites").collect();
    const completed = (sites as any[])
      .filter((s) => s.completed === true)
      .sort((a, b) => (b.lastRunAt ?? 0) - (a.lastRunAt ?? 0))
      .slice(0, limit)
      .map((s) => ({
        _id: s._id,
        name: s.name,
        url: s.url,
        pattern: s.pattern,
        lastRunAt: s.lastRunAt,
      }));
    return completed;
  },
});

export const listFailedSites = query({
  args: { limit: v.optional(v.number()) },
  returns: v.array(
    v.object({
      _id: v.id("sites"),
      name: v.optional(v.string()),
      url: v.string(),
      pattern: v.optional(v.string()),
      lastFailureAt: v.optional(v.number()),
      failCount: v.optional(v.number()),
      lastError: v.optional(v.string()),
    })
  ),
  handler: async (ctx, args) => {
    const limit = args.limit ?? 50;
    const sites = await ctx.db.query("sites").collect();
    const failed = (sites as any[])
      .filter((s) => s.failed === true && s.completed !== true)
      .sort((a, b) => (b.lastFailureAt ?? 0) - (a.lastFailureAt ?? 0))
      .slice(0, limit)
      .map((s) => ({
        _id: s._id,
        name: s.name,
        url: s.url,
        pattern: s.pattern,
        lastFailureAt: s.lastFailureAt,
        failCount: s.failCount,
        lastError: s.lastError,
      }));
    return failed;
  },
});

export const retrySite = mutation({
  args: { id: v.id("sites"), clearError: v.optional(v.boolean()) },
  returns: v.object({ success: v.boolean() }),
  handler: async (ctx, args) => {
    const patch: any = {
      completed: false,
      failed: false,
      lockedBy: "",
      lockExpiresAt: 0,
      lastRunAt: 0,
    };
    if (args.clearError !== false) {
      patch.lastError = undefined;
      patch.lastFailureAt = undefined;
      // keep failCount to preserve history
    }
    await ctx.db.patch(args.id, patch);
    return { success: true };
  },
});

export const retryProcessing = mutation({
  args: { id: v.id("sites"), limitScrapes: v.optional(v.number()) },
  returns: v.object({
    success: v.boolean(),
    scrapesProcessed: v.number(),
    jobsAttempted: v.number(),
    jobsInserted: v.number(),
  }),
  handler: async (ctx, args) => {
    const site = await ctx.db.get(args.id);
    if (!site) {
      throw new Error("Site not found");
    }

    // Clear failure flags so the site can resume normal scheduling
    await ctx.db.patch(args.id, {
      completed: false,
      failed: false,
      lockedBy: "",
      lockExpiresAt: 0,
      lastRunAt: site.lastRunAt ?? 0,
      lastError: undefined,
      lastFailureAt: undefined,
    } as any);

    const limit = Math.max(1, Math.min(args.limitScrapes ?? 5, 20));
    const scrapes = await ctx.db
      .query("scrapes")
      .withIndex("by_source", (q) => q.eq("sourceUrl", site.url))
      .order("desc")
      .take(limit);

    const normalizeJob = (row: any, scrape: any) => {
      if (!row || typeof row !== "object") return null;
      const urlCandidates = [row.url, row.job_url, row.jobUrl, row.link, row.href, row._url];
      const url = urlCandidates.find((u) => typeof u === "string" && u.trim());
      if (!url) return null;

      const titleCandidates = [row.title, row.job_title, row.jobTitle];
      const title = (titleCandidates.find((t) => typeof t === "string" && t.trim()) as string | undefined) || "Untitled";

      const company = typeof row.company === "string" && row.company.trim() ? row.company : "Unknown";
      const description = typeof row.description === "string" ? row.description : typeof row.job_description === "string" ? row.job_description : "";
      const location = typeof row.location === "string" && row.location.trim() ? row.location : "Unknown";
      const remote = Boolean(row.remote);

      const rawLevel = typeof row.level === "string" ? row.level.toLowerCase() : "";
      const level: "junior" | "mid" | "senior" | "staff" = (["junior", "mid", "senior", "staff"] as const).includes(rawLevel as any)
        ? (rawLevel as any)
        : "mid";

      const totalComp = typeof row.total_compensation === "number" ? row.total_compensation : 0;
      const postedAt =
        typeof row.posted_at === "number"
          ? row.posted_at
          : typeof row.postedAt === "number"
            ? row.postedAt
            : Date.now();

      const compensationUnknown = row.compensation_unknown === true || totalComp <= 0;
      const compensationReason =
        typeof row.compensation_reason === "string" && row.compensation_reason.trim()
          ? row.compensation_reason
          : compensationUnknown
            ? "unknown_compensation"
            : undefined;

      return {
        title,
        company,
        description,
        location,
        remote,
        level,
        totalCompensation: totalComp,
        url,
        postedAt,
        scrapedAt: scrape.completedAt ?? scrape.startedAt ?? Date.now(),
        scrapedWith: scrape.provider ?? scrape.items?.provider,
        workflowName: scrape.workflowName ?? scrape.workflowType,
        scrapedCostMilliCents:
          typeof scrape.costMilliCents === "number"
            ? scrape.costMilliCents
            : typeof scrape.items?.costMilliCents === "number"
              ? scrape.items.costMilliCents
              : undefined,
        compensationUnknown,
        compensationReason,
      };
    };

    let jobs: any[] = [];
    for (const scrape of scrapes as any[]) {
      const normalized = Array.isArray(scrape.items?.normalized) ? scrape.items.normalized : [];
      for (const row of normalized) {
        const job = normalizeJob(row, scrape);
        if (job) jobs.push(job);
      }
    }

    if (jobs.length === 0) {
      return { success: true, scrapesProcessed: scrapes.length, jobsAttempted: 0, jobsInserted: 0 };
    }

    // Cap batch size to avoid accidental overload
    jobs = jobs.slice(0, 500);
    await ctx.runMutation(api.router.ingestJobsFromScrape, { jobs });

    return {
      success: true,
      scrapesProcessed: scrapes.length,
      jobsAttempted: jobs.length,
      jobsInserted: jobs.length,
    };
  },
});

export const getScrapeHistoryForUrls = query({
  args: {
    urls: v.array(v.string()),
    limit: v.optional(v.number()),
  },
  returns: v.array(
    v.object({
      sourceUrl: v.string(),
      entries: v.array(
        v.object({
          _id: v.id("scrapes"),
          startedAt: v.number(),
          completedAt: v.number(),
        })
      ),
    })
  ),
  handler: async (ctx, args) => {
    const lim = args.limit ?? 3;
    const out: { sourceUrl: string; entries: { _id: any; startedAt: number; completedAt: number }[] }[] = [];
    for (const url of args.urls) {
      const list = await ctx.db
        .query("scrapes")
        .withIndex("by_source", (q) => q.eq("sourceUrl", url))
        .collect();
      const entries = (list as any[])
        .sort((a, b) => (b.completedAt ?? 0) - (a.completedAt ?? 0))
        .slice(0, lim)
        .map((s) => ({ _id: s._id, startedAt: s.startedAt, completedAt: s.completedAt }));
      out.push({ sourceUrl: url, entries });
    }
    return out;
  },
});

export const listScrapeActivity = query({
  args: {},
  returns: v.array(
    v.object({
      siteId: v.id("sites"),
      name: v.optional(v.string()),
      url: v.string(),
      pattern: v.optional(v.string()),
      enabled: v.boolean(),
      createdAt: v.number(),
      updatedAt: v.number(),
      lastRunAt: v.optional(v.number()),
      lastScrapeStart: v.optional(v.number()),
      lastScrapeEnd: v.optional(v.number()),
      lastJobsScraped: v.number(),
      workerId: v.optional(v.string()),
      lastFailureAt: v.optional(v.number()),
      failed: v.optional(v.boolean()),
      totalScrapes: v.number(),
      totalJobsScraped: v.number(),
    })
  ),
  handler: async (ctx) => {
    const sites = await ctx.db.query("sites").collect();
    const runs = await ctx.db.query("workflow_runs").collect();

    const countJobs = (items: any): number => {
      if (!items) return 0;

      // Common shapes: array, { items: [...] }, { results: { items: [...] } }, { results: [...] }
      if (Array.isArray(items)) return items.length;
      if (typeof items === "object") {
        if (Array.isArray((items as any).normalized)) return (items as any).normalized.length;
        if (Array.isArray((items as any).items)) return (items as any).items.length;
        if (Array.isArray((items as any).results)) return (items as any).results.length;
        if (items.results && Array.isArray((items as any).results.items)) {
          return (items as any).results.items.length;
        }
      }
      return 0;
    };

    const rows = [];

    for (const site of sites as any[]) {
      const scrapes = await ctx.db
        .query("scrapes")
        .withIndex("by_source", (q) => q.eq("sourceUrl", site.url))
        .collect();

      const sortedScrapes = scrapes.sort((a: any, b: any) => (b.completedAt ?? 0) - (a.completedAt ?? 0));
      const latest = sortedScrapes[0];

      const totalJobsScraped = (scrapes as any[]).reduce((sum, s) => sum + countJobs((s as any).items), 0);
      const lastJobsScraped = latest ? countJobs((latest as any).items) : 0;

      const runsForSite = runs
        .filter((r: any) => Array.isArray(r.siteUrls) && r.siteUrls.includes(site.url))
        .sort((a: any, b: any) => (b.completedAt ?? b.startedAt ?? 0) - (a.completedAt ?? a.startedAt ?? 0));
      const latestRun = runsForSite[0];
      const latestCompletedRun = runsForSite.find((r: any) => r.status === "completed");
      const latestAnyRunTime = latestRun ? (latestRun.completedAt ?? latestRun.startedAt ?? 0) : undefined;
      const latestSuccessTime = latestCompletedRun ? (latestCompletedRun.completedAt ?? latestCompletedRun.startedAt ?? 0) : undefined;

      const updatedAt = Math.max(
        site._creationTime ?? 0,
        site.lastRunAt ?? 0,
        site.lastFailureAt ?? 0,
        site.lockExpiresAt ?? 0,
      );

      rows.push({
        siteId: site._id,
        name: site.name,
        url: site.url,
        pattern: site.pattern,
        enabled: site.enabled,
        createdAt: site._creationTime ?? 0,
        updatedAt,
        lastRunAt: latestSuccessTime ?? site.lastRunAt ?? latestAnyRunTime,
        lastScrapeStart: latest?.startedAt ?? latestRun?.startedAt,
        lastScrapeEnd: latest?.completedAt ?? latestRun?.completedAt,
        lastJobsScraped,
        workerId: site.lockedBy,
        lastFailureAt: site.lastFailureAt,
        failed: site.failed,
        totalScrapes: scrapes.length,
        totalJobsScraped,
      });
    }

    return rows.sort((a, b) => {
      const aLast = Math.max(a.lastRunAt ?? 0, a.lastFailureAt ?? 0, a.lastScrapeEnd ?? 0);
      const bLast = Math.max(b.lastRunAt ?? 0, b.lastFailureAt ?? 0, b.lastScrapeEnd ?? 0);
      return bLast - aLast;
    });
  },
});
