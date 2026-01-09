import { query, mutation } from "./_generated/server";
import { api } from "./_generated/api";
import { Id } from "./_generated/dataModel";
import { v } from "convex/values";
import { countJobs } from "./lib/scrapeCounts";
import { deriveNextEligibleAt, scheduleFromRow } from "./lib/siteScheduling";

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
    const site = await ctx.db.get(args.id);
    if (!site) {
      throw new Error("Site not found");
    }
    const now = Date.now();
    const scheduleConfig = (site).scheduleId
      ? scheduleFromRow(await ctx.db.get((site).scheduleId as Id<"scrape_schedules">))
      : null;
    const nextEligibleAt = deriveNextEligibleAt({
      hasSchedule: !!(site).scheduleId,
      schedule: scheduleConfig,
      lastRunAt: 0,
      completed: false,
      nowMs: now,
    });
    const patch: any = {
      completed: false,
      failed: false,
      lockedBy: "",
      lockExpiresAt: 0,
      lastRunAt: 0,
      nextEligibleAt,
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
    const now = Date.now();
    const scheduleConfig = (site).scheduleId
      ? scheduleFromRow(await ctx.db.get((site).scheduleId as Id<"scrape_schedules">))
      : null;
    const nextEligibleAt = deriveNextEligibleAt({
      hasSchedule: !!(site).scheduleId,
      schedule: scheduleConfig,
      lastRunAt: (site).lastRunAt ?? 0,
      completed: (site).completed ?? false,
      nowMs: now,
    });

    // Clear failure flags so the site can resume normal scheduling
    await ctx.db.patch(args.id, {
      completed: false,
      failed: false,
      lockedBy: "",
      lockExpiresAt: 0,
      lastRunAt: site.lastRunAt ?? 0,
      nextEligibleAt,
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
      const level: "junior" | "mid" | "senior" | "staff" = (["junior", "mid", "senior", "staff"] as const).includes(rawLevel)
        ? (rawLevel)
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
        .withIndex("by_source_completed", (q) => q.eq("sourceUrl", url))
        .order("desc")
        .take(lim);
      const entries = (list as any[]).map((s) => ({ _id: s._id, startedAt: s.startedAt, completedAt: s.completedAt }));
      out.push({ sourceUrl: url, entries });
    }
    return out;
  },
});

const DEFAULT_PAGE_SIZE = 100;
const SITE_LIMIT = 300;
const RUN_LIMIT = 200;
const RUN_LOOKBACK_MS = 45 * 24 * 60 * 60 * 1000; // 45 days
const SCRAPE_LIMIT = 80;
const SCRAPE_PAGE_SIZE = 40;
const SCRAPE_LOOKBACK_MS = 30 * 24 * 60 * 60 * 1000; // 30 days
const COST_LOOKBACK_MINUTES = 60;
const COST_MAX_SCRAPES = 5000;
const COST_PAGE_SIZE = 200;

async function collectWithLimit(cursorable: any, maxItems: number = 500, pageSize: number = DEFAULT_PAGE_SIZE) {
  try {
    if (!cursorable) return [];
    if (typeof cursorable.take === "function") {
      return await cursorable.take(maxItems);
    }
    if (typeof cursorable.collect === "function") {
      const rows = await cursorable.collect();
      return rows.slice(0, maxItems);
    }
    if (typeof cursorable.paginate === "function") {
      let cursor: any = null;
      const rows: any[] = [];
      const seen = new Set<string | null>();
      const maxPages = Math.ceil(maxItems / Math.max(pageSize, 1)) + 5;
      let pages = 0;
      const normalizeCursor = (value: any) => (value === null || value === undefined ? null : String(value));
      while (true) {
        const { page, isDone, continueCursor } = await cursorable.paginate({ cursor, numItems: pageSize });
        rows.push(...(page || []));
        if (rows.length >= maxItems || isDone || !continueCursor) break;
        const nextKey = normalizeCursor(continueCursor);
        const currentKey = normalizeCursor(cursor);
        if (!page?.length || nextKey === currentKey || seen.has(nextKey) || pages >= maxPages) break;
        seen.add(nextKey);
        cursor = continueCursor;
        pages += 1;
      }
      return rows.slice(0, maxItems);
    }
  } catch (err) {
    console.error("listScrapeActivity: collectWithLimit failed", err);
  }
  return [];
}

function deriveCostMilliCents(scrape: any): number {
  if (typeof scrape?.costMilliCents === "number") {
    return scrape.costMilliCents;
  }
  if (typeof scrape?.items?.costMilliCents === "number") {
    return scrape.items.costMilliCents;
  }
  return 0;
}

const listScrapeActivityHandler = async (ctx: any) => {
  const now = Date.now();
  const runCutoff = now - RUN_LOOKBACK_MS;
  const scrapeCutoff = now - SCRAPE_LOOKBACK_MS;
  const sites = await collectWithLimit(ctx.db.query("sites").order("desc"), SITE_LIMIT, 50);
  const runs = await ctx.db
    .query("workflow_runs")
    .withIndex("by_started", (q: any) => q.gte("startedAt", runCutoff))
    .order("desc")
    .take(RUN_LIMIT);

  const rows = [];

  for (const site of sites as any[]) {
    try {
      const siteUrl = typeof site.url === "string" ? site.url : "";
      if (!siteUrl) continue;

      const scrapes = await collectWithLimit(
        site._id
          ? ctx.db
            .query("scrape_activity")
            .withIndex("by_site_completed", (q: any) => q.eq("siteId", site._id).gte("completedAt", scrapeCutoff))
            .order("desc")
          : ctx.db
            .query("scrape_activity")
            .withIndex("by_source_completed", (q: any) => q.eq("sourceUrl", siteUrl).gte("completedAt", scrapeCutoff))
            .order("desc"),
        SCRAPE_LIMIT,
        SCRAPE_PAGE_SIZE
      );

      const filteredScrapes = (scrapes as any[]).filter(
        (s) => (s).completedAt === undefined || (s).completedAt >= scrapeCutoff
      );
      const sortedScrapes = filteredScrapes.sort((a: any, b: any) => (b.completedAt ?? 0) - (a.completedAt ?? 0));
      const latest = sortedScrapes[0];

      const totalJobsScraped = filteredScrapes.reduce((sum, s) => sum + ((s as any).jobCount ?? 0), 0);
      const lastJobsScraped = latest ? ((latest as any).jobCount ?? 0) : 0;

      const runsForSite = runs
        .filter((r: any) => Array.isArray(r.siteUrls) && r.siteUrls.includes(site.url))
        .sort((a: any, b: any) => (b.completedAt ?? b.startedAt ?? 0) - (a.completedAt ?? a.startedAt ?? 0));
      const latestRun = runsForSite[0];
      const latestCompletedRun = runsForSite.find((r: any) => r.status === "completed");
      const latestAnyRunTime = latestRun ? (latestRun.completedAt ?? latestRun.startedAt ?? 0) : undefined;
      const latestSuccessTime = latestCompletedRun
        ? (latestCompletedRun.completedAt ?? latestCompletedRun.startedAt ?? 0)
        : undefined;

      const updatedAt = Math.max(
        site._creationTime ?? 0,
        site.lastRunAt ?? 0,
        site.lastFailureAt ?? 0,
        site.lockExpiresAt ?? 0
      );
      const enabled = site.enabled !== false;

      rows.push({
        siteId: site._id,
        name: site.name,
        url: siteUrl,
        pattern: site.pattern,
        enabled,
        createdAt: site._creationTime ?? 0,
        updatedAt,
        lastRunAt: latestSuccessTime ?? site.lastRunAt ?? latestAnyRunTime,
        lastScrapeStart: latest?.startedAt ?? latestRun?.startedAt,
        lastScrapeEnd: latest?.completedAt ?? latestRun?.completedAt,
        lastJobsScraped,
        workerId: typeof site.lockedBy === "string" ? site.lockedBy : undefined,
        lastFailureAt: site.lastFailureAt,
        failed: site.failed,
        totalScrapes: filteredScrapes.length,
        totalJobsScraped,
      });
    } catch (err) {
      console.error("listScrapeActivity: failed to process site", site?._id, err);
      continue;
    }
  }

  return rows.sort((a, b) => {
    const aLast = Math.max(a.lastRunAt ?? 0, a.lastFailureAt ?? 0, a.lastScrapeEnd ?? 0);
    const bLast = Math.max(b.lastRunAt ?? 0, b.lastFailureAt ?? 0, b.lastScrapeEnd ?? 0);
    return bLast - aLast;
  });
};

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
  handler: listScrapeActivityHandler,
});
(listScrapeActivity as any).handler = listScrapeActivityHandler;

export const listScrapeCostSummary = query({
  args: {
    lookbackMinutes: v.optional(v.number()),
    maxScrapes: v.optional(v.number()),
  },
  returns: v.object({
    windowStartMs: v.number(),
    windowEndMs: v.number(),
    lookbackMinutes: v.number(),
    scrapesChecked: v.number(),
    rows: v.array(
      v.object({
        company: v.string(),
        siteId: v.optional(v.id("sites")),
        siteName: v.optional(v.string()),
        siteUrl: v.optional(v.string()),
        scrapeCount: v.number(),
        totalCostMilliCents: v.number(),
        avgCostPerPageMilliCents: v.number(),
        maxCostPerPageMilliCents: v.number(),
      })
    ),
  }),
  handler: async (ctx, args) => {
    const now = Date.now();
    const lookbackMinutes = Math.max(1, Math.min(args.lookbackMinutes ?? COST_LOOKBACK_MINUTES, 24 * 60));
    const maxScrapes = Math.max(1, Math.min(args.maxScrapes ?? COST_MAX_SCRAPES, 20000));
    const cutoff = now - lookbackMinutes * 60 * 1000;

    const scrapes = await collectWithLimit(
      ctx.db
        .query("scrapes")
        .withIndex("by_completedAt_site", (q) => q.gte("completedAt", cutoff))
        .order("desc"),
      maxScrapes,
      COST_PAGE_SIZE
    );

    const siteIds = new Set<Id<"sites">>();
    for (const scrape of scrapes as any[]) {
      if (scrape?.siteId) {
        siteIds.add(scrape.siteId as Id<"sites">);
      }
    }

    const siteIdList = Array.from(siteIds);
    const siteList = await Promise.all(siteIdList.map((siteId) => ctx.db.get(siteId)));
    const siteMap = new Map<string, any>();
    siteIdList.forEach((siteId, index) => {
      const site = siteList[index];
      if (site) {
        siteMap.set(siteId.toString(), site);
      }
    });

    const summaries = new Map<
      string,
      {
        company: string;
        siteId?: Id<"sites">;
        siteName?: string;
        siteUrl?: string;
        scrapeCount: number;
        totalCostMilliCents: number;
        maxCostPerPageMilliCents: number;
      }
    >();

    for (const scrape of scrapes as any[]) {
      const cost = deriveCostMilliCents(scrape);
      const siteId = scrape?.siteId ? (scrape.siteId as Id<"sites">) : undefined;
      const site = siteId ? siteMap.get(siteId.toString()) : undefined;
      const company =
        (typeof site?.name === "string" && site.name.trim()) ||
        (typeof site?.url === "string" && site.url.trim()) ||
        (typeof scrape.sourceUrl === "string" && scrape.sourceUrl.trim()) ||
        "unknown";
      const key = siteId ? siteId.toString() : typeof scrape.sourceUrl === "string" ? scrape.sourceUrl : company;

      const entry = summaries.get(key) ?? {
        company,
        siteId,
        siteName: typeof site?.name === "string" ? site.name : undefined,
        siteUrl: typeof site?.url === "string" ? site.url : undefined,
        scrapeCount: 0,
        totalCostMilliCents: 0,
        maxCostPerPageMilliCents: 0,
      };

      entry.scrapeCount += 1;
      entry.totalCostMilliCents += cost;
      entry.maxCostPerPageMilliCents = Math.max(entry.maxCostPerPageMilliCents, cost);

      summaries.set(key, entry);
    }

    const rows = Array.from(summaries.values()).map((entry) => ({
      company: entry.company,
      siteId: entry.siteId,
      siteName: entry.siteName,
      siteUrl: entry.siteUrl,
      scrapeCount: entry.scrapeCount,
      totalCostMilliCents: entry.totalCostMilliCents,
      avgCostPerPageMilliCents:
        entry.scrapeCount > 0 ? Math.round(entry.totalCostMilliCents / entry.scrapeCount) : 0,
      maxCostPerPageMilliCents: entry.maxCostPerPageMilliCents,
    }));

    rows.sort((a, b) => {
      if (b.totalCostMilliCents !== a.totalCostMilliCents) {
        return b.totalCostMilliCents - a.totalCostMilliCents;
      }
      if (b.scrapeCount !== a.scrapeCount) {
        return b.scrapeCount - a.scrapeCount;
      }
      return a.company.localeCompare(b.company);
    });

    return {
      windowStartMs: cutoff,
      windowEndMs: now,
      lookbackMinutes,
      scrapesChecked: scrapes.length,
      rows,
    };
  },
});

export const __test = { collectWithLimit, countJobs };
