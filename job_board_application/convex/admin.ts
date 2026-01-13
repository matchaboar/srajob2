import { mutation, query, internalMutation } from "./_generated/server";
import { internal } from "./_generated/api";
import type { Id, TableNames } from "./_generated/dataModel";
import { v } from "convex/values";
import { normalizeCompanyFilterKey } from "./jobs";
import { deleteDescriptionFromStorage } from "./jobDescriptionStorage";
import { normalizeJobUrlKey } from "./jobUrlUtils";

type AnyDoc = Record<string, any>;
type WipeTable =
  | "jobs"
  | "scrapes"
  | "scrape_activity"
  | "seen_job_urls"
  | "seen_job_url_index"
  | "ignored_jobs"
  | "scrape_errors";

type SalaryLevel = "junior" | "mid" | "senior" | "staff";
const SALARY_LEVELS: SalaryLevel[] = ["junior", "mid", "senior", "staff"];

export const wipeSiteDataByDomainPage = mutation({
  args: {
    domain: v.string(),
    prefix: v.optional(v.string()),
    table: v.union(
      v.literal("jobs"),
      v.literal("scrapes"),
      v.literal("scrape_activity"),
      v.literal("seen_job_urls"),
      v.literal("seen_job_url_index"),
      v.literal("ignored_jobs"),
      v.literal("scrape_errors")
    ),
    dryRun: v.optional(v.boolean()),
    batchSize: v.optional(v.number()),
    cursor: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    const domain = args.domain.trim().toLowerCase();
    if (!domain) {
      throw new Error("domain is required");
    }

    const tableName = args.table as TableNames & WipeTable;
    const prefix = (args.prefix || `https://${domain}`).trim().toLowerCase();
    const prefixUpper = `${prefix}\uffff`;
    const dryRun = args.dryRun ?? false;
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 500, 2000));
    const cursor = args.cursor ?? null;

    const matchesUrl = (value: unknown): boolean =>
      typeof value === "string" && value.toLowerCase().includes(domain);

    const sites = await ctx.db.query("sites").collect();
    const matchedSites = (sites as AnyDoc[]).filter((site) => matchesUrl(site.url));
    const siteIds = new Set(matchedSites.map((site) => site._id));
    const siteIdStrings = new Set([...siteIds].map((id) => String(id)));

    const baseQuery = (() => {
      switch (tableName) {
        case "jobs":
          return ctx.db
            .query("jobs")
            .withIndex("by_url", (q) => q.gte("url", prefix).lt("url", prefixUpper));
        case "scrapes":
          return ctx.db
            .query("scrapes")
            .withIndex("by_source", (q) => q.gte("sourceUrl", prefix).lt("sourceUrl", prefixUpper));
        case "scrape_activity":
          return ctx.db
            .query("scrape_activity")
            .withIndex("by_source_completed", (q) => q.gte("sourceUrl", prefix).lt("sourceUrl", prefixUpper));
        case "ignored_jobs":
          return ctx.db
            .query("ignored_jobs")
            .withIndex("by_url", (q) => q.gte("url", prefix).lt("url", prefixUpper));
        case "seen_job_urls":
          return ctx.db
            .query("seen_job_urls")
            .withIndex("by_source", (q) => q.gte("sourceUrl", prefix).lt("sourceUrl", prefixUpper));
        case "seen_job_url_index":
          return ctx.db
            .query("seen_job_url_index" as any)
            .withIndex("by_url_source", (q) => q.gte("url", prefix).lt("url", prefixUpper));
        default:
          return ctx.db.query(tableName);
      }
    })();

    const shouldDelete = (row: AnyDoc): boolean => {
      switch (tableName) {
        case "jobs":
          return matchesUrl(row.url);
        case "scrapes":
          if (row.siteId && siteIds.has(row.siteId)) return true;
          if (matchesUrl(row.sourceUrl)) return true;
          if (Array.isArray(row.subUrls) && row.subUrls.some((url: unknown) => matchesUrl(url))) {
            return true;
          }
          return false;
        case "scrape_activity":
          if (row.siteId && siteIds.has(row.siteId)) return true;
          return matchesUrl(row.sourceUrl);
        case "ignored_jobs":
          return matchesUrl(row.url) || matchesUrl(row.sourceUrl);
        case "seen_job_urls":
          return matchesUrl(row.url) || matchesUrl(row.sourceUrl);
        case "seen_job_url_index":
          return matchesUrl(row.url) || matchesUrl(row.sourceUrl);
        case "scrape_errors":
          if (row.siteId && siteIdStrings.has(String(row.siteId))) return true;
          return matchesUrl(row.sourceUrl);
        default:
          return false;
      }
    };

    const page = await baseQuery.paginate({ cursor, numItems: batchSize });
    let deleted = 0;
    for (const row of page.page as AnyDoc[]) {
      if (!shouldDelete(row)) continue;
      deleted += 1;
      if (dryRun) continue;
      if (tableName === "jobs") {
        const details = await ctx.db
          .query("job_details")
          .withIndex("by_job", (q) => q.eq("jobId", row._id))
          .collect();
        for (const detail of details as AnyDoc[]) {
          await deleteDescriptionFromStorage(ctx, detail.descriptionStorageId);
          await ctx.db.delete(detail._id);
        }
      }
      await ctx.db.delete(row._id);
    }

    return {
      domain,
      table: tableName,
      dryRun,
      batchSize,
      scanned: page.page.length,
      deleted,
      hasMore: !page.isDone,
      cursor: page.continueCursor,
      sites: matchedSites.map((site) => ({
        id: site._id,
        url: site.url,
        name: site.name ?? null,
      })),
    };
  },
});

export const wipeJobsByCompanyPage = mutation({
  args: {
    company: v.string(),
    dryRun: v.optional(v.boolean()),
    batchSize: v.optional(v.number()),
    cursor: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    const company = (args.company || "").trim();
    if (!company) {
      throw new Error("company is required");
    }

    const needleLower = company.toLowerCase();
    const needleKey = normalizeCompanyFilterKey(company);
    const dryRun = args.dryRun ?? false;
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 500, 2000));
    const cursor = args.cursor ?? null;

    const matchesCompany = (value: unknown): boolean => {
      if (typeof value !== "string") return false;
      const cleaned = value.trim();
      if (!cleaned) return false;
      const normalized = normalizeCompanyFilterKey(cleaned);
      if (needleKey && normalized && normalized === needleKey) return true;
      const lowered = cleaned.toLowerCase();
      if (lowered === needleLower) return true;
      if (lowered.startsWith(needleLower) && lowered.length > needleLower.length) {
        const next = lowered[needleLower.length];
        if (!next || !/[a-z0-9]/.test(next)) return true;
      }
      return false;
    };

    const page = await ctx.db.query("jobs").paginate({ cursor, numItems: batchSize });
    let deleted = 0;
    let deletedDetails = 0;
    let deletedUrlKeys = 0;
    let deletedApplications = 0;

    for (const row of page.page as AnyDoc[]) {
      const companyValue = row.company ?? "";
      const companyKey = row.companyKey ?? "";
      if (!matchesCompany(companyValue) && !matchesCompany(companyKey)) {
        continue;
      }
      deleted += 1;
      if (dryRun) continue;

      // Delete job_details
      const details = await ctx.db
        .query("job_details")
        .withIndex("by_job", (q) => q.eq("jobId", row._id))
        .collect();
      for (const detail of details as AnyDoc[]) {
        await deleteDescriptionFromStorage(ctx, detail.descriptionStorageId);
        await ctx.db.delete(detail._id);
      }
      deletedDetails += details.length;

      // Delete job_url_keys
      const urlKeys = await ctx.db
        .query("job_url_keys")
        .withIndex("by_job", (q) => q.eq("jobId", row._id))
        .collect();
      for (const urlKey of urlKeys as AnyDoc[]) {
        await ctx.db.delete(urlKey._id);
      }
      deletedUrlKeys += urlKeys.length;

      // Delete applications
      const applications = await ctx.db
        .query("applications")
        .withIndex("by_job", (q) => q.eq("jobId", row._id))
        .collect();
      for (const application of applications as AnyDoc[]) {
        await ctx.db.delete(application._id);
      }
      deletedApplications += applications.length;

      // Finally delete the job itself
      await ctx.db.delete(row._id);
    }

    return {
      company,
      dryRun,
      batchSize,
      scanned: page.page.length,
      deleted,
      deletedDetails,
      deletedUrlKeys,
      deletedApplications,
      hasMore: !page.isDone,
      cursor: page.continueCursor,
    };
  },
});

export const deleteJobsById = mutation({
  args: {
    jobIds: v.array(v.id("jobs")),
    dryRun: v.optional(v.boolean()),
  },
  handler: async (ctx, args) => {
    const dryRun = args.dryRun ?? false;
    const results: Array<{ jobId: string; hadJob: boolean; details: number; urlKeys: number; applications: number }> = [];
    let deletedJobs = 0;
    let deletedDetails = 0;
    let deletedUrlKeys = 0;
    let deletedApplications = 0;

    for (const jobId of args.jobIds) {
      const job = await ctx.db.get(jobId);

      // Get job_details
      const details = await ctx.db
        .query("job_details")
        .withIndex("by_job", (q) => q.eq("jobId", jobId))
        .collect();
      const detailCount = details.length;

      // Get job_url_keys
      const urlKeys = await ctx.db
        .query("job_url_keys")
        .withIndex("by_job", (q) => q.eq("jobId", jobId))
        .collect();
      const urlKeyCount = urlKeys.length;

      // Get applications
      const applications = await ctx.db
        .query("applications")
        .withIndex("by_job", (q) => q.eq("jobId", jobId))
        .collect();
      const applicationCount = applications.length;

      if (!dryRun) {
        // Delete job_details
        for (const detail of details as AnyDoc[]) {
          await deleteDescriptionFromStorage(ctx, detail.descriptionStorageId);
          await ctx.db.delete(detail._id);
        }

        // Delete job_url_keys
        for (const urlKey of urlKeys as AnyDoc[]) {
          await ctx.db.delete(urlKey._id);
        }

        // Delete applications
        for (const application of applications as AnyDoc[]) {
          await ctx.db.delete(application._id);
        }

        // Delete the job itself
        if (job) {
          await ctx.db.delete(jobId);
        }
      }

      if (job) {
        deletedJobs += 1;
      }
      deletedDetails += detailCount;
      deletedUrlKeys += urlKeyCount;
      deletedApplications += applicationCount;
      results.push({
        jobId: String(jobId),
        hadJob: Boolean(job),
        details: detailCount,
        urlKeys: urlKeyCount,
        applications: applicationCount,
      });
    }

    return {
      dryRun,
      deletedJobs,
      deletedDetails,
      deletedUrlKeys,
      deletedApplications,
      results,
    };
  },
});

export const deleteJobDetailsByJobIds = mutation({
  args: {
    jobIds: v.array(v.id("jobs")),
    dryRun: v.optional(v.boolean()),
  },
  returns: v.object({
    dryRun: v.boolean(),
    deletedDetails: v.number(),
    results: v.array(
      v.object({
        jobId: v.string(),
        details: v.number(),
      })
    ),
  }),
  handler: async (ctx, args) => {
    const dryRun = args.dryRun ?? false;
    const results: Array<{ jobId: string; details: number }> = [];
    let deletedDetails = 0;

    for (const jobId of args.jobIds) {
      const details = await ctx.db
        .query("job_details")
        .withIndex("by_job", (q) => q.eq("jobId", jobId))
        .collect();
      const detailCount = details.length;

      if (!dryRun) {
        for (const detail of details as AnyDoc[]) {
          await deleteDescriptionFromStorage(ctx, detail.descriptionStorageId);
          await ctx.db.delete(detail._id);
        }
      }

      deletedDetails += detailCount;
      results.push({ jobId: String(jobId), details: detailCount });
    }

    return {
      dryRun,
      deletedDetails,
      results,
    };
  },
});

export const deleteRecentJobsPage = mutation({
  args: {
    sinceMs: v.number(),
    untilMs: v.optional(v.number()),
    dryRun: v.optional(v.boolean()),
    batchSize: v.optional(v.number()),
    cursor: v.optional(v.union(v.string(), v.null())),
  },
  returns: v.object({
    dryRun: v.boolean(),
    sinceMs: v.number(),
    untilMs: v.number(),
    batchSize: v.number(),
    scanned: v.number(),
    deletedJobs: v.number(),
    deletedDetails: v.number(),
    deletedJobUrlKeys: v.number(),
    deletedSeenUrls: v.number(),
    deletedSeenUrlIndex: v.number(),
    deletedIgnoredJobs: v.number(),
    hasMore: v.boolean(),
    cursor: v.union(v.string(), v.null()),
  }),
  handler: async (ctx, args) => {
    const dryRun = args.dryRun ?? false;
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 200, 2000));
    const untilMs = args.untilMs ?? Date.now();

    if (untilMs <= args.sinceMs) {
      throw new Error("untilMs must be greater than sinceMs");
    }

    const page = await ctx.db
      .query("jobs")
      .withIndex("by_scraped_at", (q) =>
        q.gte("scrapedAt", args.sinceMs).lt("scrapedAt", untilMs),
      )
      .paginate({ cursor: args.cursor ?? null, numItems: batchSize });

    let deletedJobs = 0;
    let deletedDetails = 0;
    let deletedJobUrlKeys = 0;
    let deletedSeenUrls = 0;
    let deletedSeenUrlIndex = 0;
    let deletedIgnoredJobs = 0;

    for (const job of page.page as AnyDoc[]) {
      deletedJobs += 1;

      const details = await ctx.db
        .query("job_details")
        .withIndex("by_job", (q) => q.eq("jobId", job._id))
        .collect();
      deletedDetails += details.length;

      const urlKeys = await ctx.db
        .query("job_url_keys")
        .withIndex("by_url", (q) => q.eq("url", job.url))
        .collect();
      deletedJobUrlKeys += urlKeys.length;

      const seenIndexRows = await (ctx.db as any)
        .query("seen_job_url_index")
        .withIndex("by_url_source", (q: any) => q.eq("url", job.url))
        .collect();
      deletedSeenUrlIndex += seenIndexRows.length;
      const seenUrlIds = new Set<any>();
      for (const row of seenIndexRows as AnyDoc[]) {
        if (row.seenJobUrlId) {
          seenUrlIds.add(row.seenJobUrlId);
        }
      }
      deletedSeenUrls += seenUrlIds.size;

      const ignored = await ctx.db
        .query("ignored_jobs")
        .withIndex("by_url", (q) => q.eq("url", job.url))
        .collect();
      deletedIgnoredJobs += ignored.length;

      if (!dryRun) {
        for (const detail of details as AnyDoc[]) {
          await deleteDescriptionFromStorage(ctx, detail.descriptionStorageId);
          await ctx.db.delete(detail._id);
        }
        for (const row of urlKeys as AnyDoc[]) {
          await ctx.db.delete(row._id);
        }
        for (const row of seenIndexRows as AnyDoc[]) {
          await ctx.db.delete(row._id);
        }
        for (const seenId of seenUrlIds) {
          await ctx.db.delete(seenId);
        }
        for (const row of ignored as AnyDoc[]) {
          await ctx.db.delete(row._id);
        }
        await ctx.db.delete(job._id);
      }
    }

    return {
      dryRun,
      sinceMs: args.sinceMs,
      untilMs,
      batchSize,
      scanned: page.page.length,
      deletedJobs,
      deletedDetails,
      deletedJobUrlKeys,
      deletedSeenUrls,
      deletedSeenUrlIndex,
      deletedIgnoredJobs,
      hasMore: !page.isDone,
      cursor: page.continueCursor,
    };
  },
});

export const listRecentSeenJobUrls = query({
  args: {
    sinceMs: v.number(),
    sampleLimit: v.optional(v.number()),
    sourceLimit: v.optional(v.number()),
    batchSize: v.optional(v.number()),
  },
  returns: v.object({
    sinceMs: v.number(),
    untilMs: v.number(),
    seenUrls: v.number(),
    seenUrlIndex: v.number(),
    sample: v.array(
      v.object({
        sourceUrl: v.string(),
        url: v.string(),
        createdAt: v.number(),
      }),
    ),
    sources: v.array(
      v.object({
        sourceUrl: v.string(),
        count: v.number(),
      }),
    ),
  }),
  handler: async (ctx, args) => {
    const sinceMs = args.sinceMs;
    const untilMs = Date.now();
    const sampleLimit = Math.max(1, Math.min(args.sampleLimit ?? 25, 200));
    const sourceLimit = Math.max(1, Math.min(args.sourceLimit ?? 50, 200));
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 2000, 5000));

    const sampleRows = await ctx.db
      .query("seen_job_urls")
      .withIndex("by_created_at", (q) => q.gte("createdAt", sinceMs))
      .order("desc")
      .take(sampleLimit);

    const sample = sampleRows.map((row: AnyDoc) => ({
      sourceUrl: row.sourceUrl,
      url: row.url,
      createdAt: row.createdAt,
    }));

    const seenRows = await ctx.db
      .query("seen_job_urls")
      .withIndex("by_created_at", (q) => q.gte("createdAt", sinceMs))
      .collect();

    const sourceCounts = new Map<string, number>();
    for (const row of seenRows as AnyDoc[]) {
      const sourceUrl = row.sourceUrl ?? "";
      if (sourceUrl) {
        sourceCounts.set(sourceUrl, (sourceCounts.get(sourceUrl) ?? 0) + 1);
      }
    }
    const seenUrls = seenRows.length;

    const seenIndexRows = await (ctx.db as any)
      .query("seen_job_url_index")
      .withIndex("by_created_at", (q: any) => q.gte("createdAt", sinceMs))
      .collect();
    const seenUrlIndex = seenIndexRows.length;

    const sources = Array.from(sourceCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, sourceLimit)
      .map(([sourceUrl, count]) => ({ sourceUrl, count }));

    return {
      sinceMs,
      untilMs,
      seenUrls,
      seenUrlIndex,
      sample,
      sources,
    };
  },
});

export const deleteRecentScrapesPage = mutation({
  args: {
    sinceMs: v.number(),
    untilMs: v.optional(v.number()),
    dryRun: v.optional(v.boolean()),
    batchSize: v.optional(v.number()),
    cursor: v.optional(v.union(v.string(), v.null())),
  },
  returns: v.object({
    dryRun: v.boolean(),
    sinceMs: v.number(),
    untilMs: v.number(),
    batchSize: v.number(),
    scanned: v.number(),
    deleted: v.number(),
    hasMore: v.boolean(),
    cursor: v.union(v.string(), v.null()),
  }),
  handler: async (ctx, args) => {
    const dryRun = args.dryRun ?? false;
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 500, 2000));
    const untilMs = args.untilMs ?? Date.now();

    if (untilMs <= args.sinceMs) {
      throw new Error("untilMs must be greater than sinceMs");
    }

    const page = await ctx.db
      .query("scrapes")
      .withIndex("by_completedAt", (q) =>
        q.gte("completedAt", args.sinceMs).lt("completedAt", untilMs),
      )
      .paginate({ cursor: args.cursor ?? null, numItems: batchSize });

    let deleted = 0;
    if (!dryRun) {
      for (const row of page.page as AnyDoc[]) {
        await ctx.db.delete(row._id);
        deleted += 1;
      }
    } else {
      deleted = page.page.length;
    }

    return {
      dryRun,
      sinceMs: args.sinceMs,
      untilMs,
      batchSize,
      scanned: page.page.length,
      deleted,
      hasMore: !page.isDone,
      cursor: page.continueCursor,
    };
  },
});

export const deleteRecentScrapeActivityPage = mutation({
  args: {
    sinceMs: v.number(),
    untilMs: v.optional(v.number()),
    dryRun: v.optional(v.boolean()),
    batchSize: v.optional(v.number()),
    cursor: v.optional(v.union(v.string(), v.null())),
  },
  returns: v.object({
    dryRun: v.boolean(),
    sinceMs: v.number(),
    untilMs: v.number(),
    batchSize: v.number(),
    scanned: v.number(),
    deleted: v.number(),
    hasMore: v.boolean(),
    cursor: v.union(v.string(), v.null()),
  }),
  handler: async (ctx, args) => {
    const dryRun = args.dryRun ?? false;
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 500, 2000));
    const untilMs = args.untilMs ?? Date.now();

    if (untilMs <= args.sinceMs) {
      throw new Error("untilMs must be greater than sinceMs");
    }

    const page = await ctx.db
      .query("scrape_activity")
      .withIndex("by_completedAt", (q) =>
        q.gte("completedAt", args.sinceMs).lt("completedAt", untilMs),
      )
      .paginate({ cursor: args.cursor ?? null, numItems: batchSize });

    let deleted = 0;
    if (!dryRun) {
      for (const row of page.page as AnyDoc[]) {
        await ctx.db.delete(row._id);
        deleted += 1;
      }
    } else {
      deleted = page.page.length;
    }

    return {
      dryRun,
      sinceMs: args.sinceMs,
      untilMs,
      batchSize,
      scanned: page.page.length,
      deleted,
      hasMore: !page.isDone,
      cursor: page.continueCursor,
    };
  },
});

export const deleteRecentScrapeErrorsPage = mutation({
  args: {
    sinceMs: v.number(),
    untilMs: v.optional(v.number()),
    dryRun: v.optional(v.boolean()),
    batchSize: v.optional(v.number()),
    cursor: v.optional(v.union(v.string(), v.null())),
  },
  returns: v.object({
    dryRun: v.boolean(),
    sinceMs: v.number(),
    untilMs: v.number(),
    batchSize: v.number(),
    scanned: v.number(),
    deleted: v.number(),
    hasMore: v.boolean(),
    cursor: v.union(v.string(), v.null()),
  }),
  handler: async (ctx, args) => {
    const dryRun = args.dryRun ?? false;
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 500, 2000));
    const untilMs = args.untilMs ?? Date.now();

    if (untilMs <= args.sinceMs) {
      throw new Error("untilMs must be greater than sinceMs");
    }

    const page = await ctx.db
      .query("scrape_errors")
      .withIndex("by_created", (q) =>
        q.gte("createdAt", args.sinceMs).lt("createdAt", untilMs),
      )
      .paginate({ cursor: args.cursor ?? null, numItems: batchSize });

    let deleted = 0;
    if (!dryRun) {
      for (const row of page.page as AnyDoc[]) {
        await ctx.db.delete(row._id);
        deleted += 1;
      }
    } else {
      deleted = page.page.length;
    }

    return {
      dryRun,
      sinceMs: args.sinceMs,
      untilMs,
      batchSize,
      scanned: page.page.length,
      deleted,
      hasMore: !page.isDone,
      cursor: page.continueCursor,
    };
  },
});

export const deleteRecentSeenJobUrlsPage = mutation({
  args: {
    sinceMs: v.number(),
    dryRun: v.optional(v.boolean()),
    batchSize: v.optional(v.number()),
    cursor: v.optional(v.union(v.string(), v.null())),
  },
  returns: v.object({
    dryRun: v.boolean(),
    sinceMs: v.number(),
    batchSize: v.number(),
    scanned: v.number(),
    deleted: v.number(),
    hasMore: v.boolean(),
    cursor: v.union(v.string(), v.null()),
  }),
  handler: async (ctx, args) => {
    const dryRun = args.dryRun ?? false;
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 500, 2000));

    const page = await ctx.db
      .query("seen_job_urls")
      .withIndex("by_created_at", (q) => q.gte("createdAt", args.sinceMs))
      .paginate({ cursor: args.cursor ?? null, numItems: batchSize });

    let deleted = 0;
    if (!dryRun) {
      for (const row of page.page as AnyDoc[]) {
        await ctx.db.delete(row._id);
        deleted += 1;
        const indexRow = await (ctx.db as any)
          .query("seen_job_url_index")
          .withIndex("by_url_source", (q: any) =>
            q.eq("url", row.url).eq("sourceUrl", row.sourceUrl),
          )
          .first();
        if (indexRow) {
          await ctx.db.delete(indexRow._id);
        }
      }
    } else {
      deleted = page.page.length;
    }

    return {
      dryRun,
      sinceMs: args.sinceMs,
      batchSize,
      scanned: page.page.length,
      deleted,
      hasMore: !page.isDone,
      cursor: page.continueCursor,
    };
  },
});

export const deleteRecentSeenJobUrlIndexPage = mutation({
  args: {
    sinceMs: v.number(),
    dryRun: v.optional(v.boolean()),
    batchSize: v.optional(v.number()),
    cursor: v.optional(v.union(v.string(), v.null())),
  },
  returns: v.object({
    dryRun: v.boolean(),
    sinceMs: v.number(),
    batchSize: v.number(),
    scanned: v.number(),
    deleted: v.number(),
    hasMore: v.boolean(),
    cursor: v.union(v.string(), v.null()),
  }),
  handler: async (ctx, args) => {
    const dryRun = args.dryRun ?? false;
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 500, 2000));

    const page = await (ctx.db as any)
      .query("seen_job_url_index")
      .withIndex("by_created_at", (q: any) => q.gte("createdAt", args.sinceMs))
      .paginate({ cursor: args.cursor ?? null, numItems: batchSize });

    let deleted = 0;
    if (!dryRun) {
      for (const row of page.page as AnyDoc[]) {
        await ctx.db.delete(row._id);
        deleted += 1;
      }
    } else {
      deleted = page.page.length;
    }

    return {
      dryRun,
      sinceMs: args.sinceMs,
      batchSize,
      scanned: page.page.length,
      deleted,
      hasMore: !page.isDone,
      cursor: page.continueCursor,
    };
  },
});

export const deleteRecentIgnoredJobsPage = mutation({
  args: {
    sinceMs: v.number(),
    dryRun: v.optional(v.boolean()),
    batchSize: v.optional(v.number()),
    cursor: v.optional(v.union(v.string(), v.null())),
  },
  returns: v.object({
    dryRun: v.boolean(),
    sinceMs: v.number(),
    batchSize: v.number(),
    scanned: v.number(),
    deleted: v.number(),
    hasMore: v.boolean(),
    cursor: v.union(v.string(), v.null()),
  }),
  handler: async (ctx, args) => {
    const dryRun = args.dryRun ?? false;
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 500, 2000));

    const page = await ctx.db
      .query("ignored_jobs")
      .withIndex("by_created_at", (q) => q.gte("createdAt", args.sinceMs))
      .paginate({ cursor: args.cursor ?? null, numItems: batchSize });

    let deleted = 0;
    if (!dryRun) {
      for (const row of page.page as AnyDoc[]) {
        await ctx.db.delete(row._id);
        deleted += 1;
      }
    } else {
      deleted = page.page.length;
    }

    return {
      dryRun,
      sinceMs: args.sinceMs,
      batchSize,
      scanned: page.page.length,
      deleted,
      hasMore: !page.isDone,
      cursor: page.continueCursor,
    };
  },
});

export const listCompanySalaryMaxima = query({
  args: {
    minCompensation: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const minCompensation = Math.max(0, args.minCompensation ?? 0);
    const jobs = await ctx.db.query("jobs").collect();
    const companies = new Set<string>();
    const caps: Record<string, Partial<Record<SalaryLevel, number>>> = {};
    const globalMaxByLevel: Record<SalaryLevel, number> = {
      junior: 0,
      mid: 0,
      senior: 0,
      staff: 0,
    };

    const addCompany = (value: unknown) => {
      if (typeof value !== "string") return;
      const trimmed = value.trim();
      if (!trimmed || trimmed.toLowerCase() === "unknown") return;
      companies.add(trimmed);
    };

    for (const job of jobs as AnyDoc[]) {
      const company = typeof job.company === "string" ? job.company.trim() : "";
      addCompany(company);

      const level = job.level as SalaryLevel;
      if (!SALARY_LEVELS.includes(level)) continue;

      const compensation = typeof job.totalCompensation === "number" ? job.totalCompensation : null;
      if (compensation === null || Number.isNaN(compensation)) continue;
      if (job.compensationUnknown === true) continue;
      if (compensation < minCompensation) continue;

      const companyCaps = caps[company] ?? {};
      const existing = companyCaps[level];
      if (existing === undefined || compensation > existing) {
        companyCaps[level] = compensation;
        caps[company] = companyCaps;
      }
      if (compensation > globalMaxByLevel[level]) {
        globalMaxByLevel[level] = compensation;
      }
    }

    const profiles = await ctx.db.query("company_profiles").collect();
    for (const profile of profiles as AnyDoc[]) {
      addCompany(profile.name);
      if (Array.isArray(profile.aliases)) {
        for (const alias of profile.aliases) {
          addCompany(alias);
        }
      }
    }

    const sites = await ctx.db.query("sites").collect();
    for (const site of sites as AnyDoc[]) {
      addCompany(site.name);
    }

    return {
      allCompanies: Array.from(companies).sort((a, b) => a.localeCompare(b)),
      caps,
      globalMaxByLevel,
    };
  },
});

// ============================================================================
// BACKFILL: seen_job_urls and job_url_keys for existing jobs
// ============================================================================
// This mutation backfills the dedup tables for jobs that were scraped before
// the fix to process_spidercloud_job_batch was deployed.
//
// The bug was that siteId wasn't being passed to store_scrape, so
// recordSeenJobUrl was never called during job ingestion.
// ============================================================================

const JOB_URL_BUCKETS = 256;

const hashStringToBucket = (value: string, bucketCount: number) => {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0) % bucketCount;
};

const deriveJobUrlBucket = (urlKey: string) =>
  hashStringToBucket(urlKey, JOB_URL_BUCKETS);

/**
 * Extract the greenhouse board slug from a job URL.
 * e.g., "https://boards.greenhouse.io/airbnb/jobs/123" -> "airbnb"
 */
const extractGreenhouseSlug = (url: string): string | null => {
  try {
    const parsed = new URL(url);
    const host = parsed.hostname.toLowerCase();

    // boards.greenhouse.io/{company}/jobs/{id}
    if (host === "boards.greenhouse.io") {
      const parts = parsed.pathname.split("/").filter(Boolean);
      if (parts.length >= 1) {
        return parts[0].toLowerCase();
      }
    }

    // boards-api.greenhouse.io/v1/boards/{company}/jobs/{id}
    if (host === "boards-api.greenhouse.io") {
      const match = parsed.pathname.match(/\/boards\/([^/]+)/i);
      if (match) {
        return match[1].toLowerCase();
      }
    }

    return null;
  } catch {
    return null;
  }
};

/**
 * Extract the greenhouse board slug from a site source URL.
 * e.g., "https://api.greenhouse.io/v1/boards/airbnb/jobs" -> "airbnb"
 */
const extractGreenhouseSlugFromSource = (url: string): string | null => {
  try {
    const match = url.match(/\/boards\/([^/]+)/i);
    if (match) {
      return match[1].toLowerCase();
    }
    return null;
  } catch {
    return null;
  }
};

/**
 * Backfill seen_job_urls and job_url_keys for a specific site.
 *
 * This finds all jobs that match the site's URL pattern and ensures
 * they are recorded in the dedup tables.
 */
export const backfillSeenJobUrlsForSite = mutation({
  args: {
    siteId: v.id("sites"),
    dryRun: v.optional(v.boolean()),
    batchSize: v.optional(v.number()),
    cursor: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    const site = await ctx.db.get(args.siteId);
    if (!site) {
      throw new Error(`Site not found: ${args.siteId}`);
    }

    const sourceUrl = (site.url ?? "").trim();
    if (!sourceUrl) {
      throw new Error(`Site has no URL: ${args.siteId}`);
    }

    const dryRun = args.dryRun ?? false;
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 500, 2000));
    const cursor = args.cursor ?? null;

    // Determine matching strategy based on site type
    const siteType = (site.type ?? "general").toLowerCase();
    const sourceSlug = extractGreenhouseSlugFromSource(sourceUrl);

    // Build URL prefix for job matching
    let jobUrlPrefix: string | null = null;
    if (siteType === "greenhouse" && sourceSlug) {
      jobUrlPrefix = `https://boards.greenhouse.io/${sourceSlug}/`;
    }

    // If we can't determine a prefix, use domain matching
    let sourceDomain: string | null = null;
    if (!jobUrlPrefix) {
      try {
        const parsed = new URL(sourceUrl);
        sourceDomain = parsed.hostname.toLowerCase();
      } catch {
        // fallback
      }
    }

    // Query jobs matching this site
    const baseQuery = jobUrlPrefix
      ? ctx.db.query("jobs").withIndex("by_url", (q) =>
          q.gte("url", jobUrlPrefix!).lt("url", jobUrlPrefix! + "\uffff")
        )
      : ctx.db.query("jobs");

    const page = await baseQuery.paginate({ cursor, numItems: batchSize });

    let scanned = 0;
    let seenUrlsCreated = 0;
    let seenUrlsExisted = 0;
    let seenIndexCreated = 0;
    let jobUrlKeysCreated = 0;
    let jobUrlKeysExisted = 0;
    let skippedNoMatch = 0;

    for (const job of page.page as AnyDoc[]) {
      scanned += 1;
      const jobUrl = (job.url ?? "").trim();
      if (!jobUrl) continue;

      // Check if job matches this site
      let matches = false;
      if (jobUrlPrefix && jobUrl.toLowerCase().startsWith(jobUrlPrefix.toLowerCase())) {
        matches = true;
      } else if (sourceDomain && siteType === "greenhouse") {
        // For greenhouse, match by slug
        const jobSlug = extractGreenhouseSlug(jobUrl);
        if (jobSlug && sourceSlug && jobSlug === sourceSlug) {
          matches = true;
        }
      } else if (sourceDomain) {
        // For other sites, match by domain
        try {
          const jobParsed = new URL(jobUrl);
          if (jobParsed.hostname.toLowerCase().includes(sourceDomain)) {
            matches = true;
          }
        } catch {
          // skip
        }
      }

      if (!matches) {
        skippedNoMatch += 1;
        continue;
      }

      if (dryRun) continue;

      // 1. Record in seen_job_urls
      const existingSeenUrl = await ctx.db
        .query("seen_job_urls")
        .withIndex("by_source_url", (q: any) =>
          q.eq("sourceUrl", sourceUrl).eq("url", jobUrl)
        )
        .first();

      let seenJobUrlId: Id<"seen_job_urls"> | undefined;
      let seenCreatedAt: number;

      if (existingSeenUrl) {
        seenUrlsExisted += 1;
        seenJobUrlId = existingSeenUrl._id;
        seenCreatedAt = (existingSeenUrl as any).createdAt ?? Date.now();
      } else {
        seenUrlsCreated += 1;
        seenCreatedAt = Date.now();
        seenJobUrlId = await ctx.db.insert("seen_job_urls", {
          sourceUrl,
          url: jobUrl,
          createdAt: seenCreatedAt,
        });
      }

      // 2. Record in seen_job_url_index
      const existingSeenIndex = await ctx.db
        .query("seen_job_url_index")
        .withIndex("by_url_source", (q: any) =>
          q.eq("url", jobUrl).eq("sourceUrl", sourceUrl)
        )
        .first();

      if (!existingSeenIndex) {
        seenIndexCreated += 1;
        await ctx.db.insert("seen_job_url_index", {
          sourceUrl,
          url: jobUrl,
          seenJobUrlId,
          createdAt: seenCreatedAt,
        });
      }

      // 3. Record in job_url_keys
      const key = normalizeJobUrlKey(jobUrl);
      if (key) {
        const bucket = deriveJobUrlBucket(key);
        const existingKey = await ctx.db
          .query("job_url_keys")
          .withIndex("by_bucket_url", (q: any) =>
            q.eq("bucket", bucket).eq("url", key)
          )
          .first();

        if (existingKey) {
          jobUrlKeysExisted += 1;
        } else {
          jobUrlKeysCreated += 1;
          await ctx.db.insert("job_url_keys", {
            bucket,
            url: key,
            jobId: job._id,
            createdAt: Date.now(),
          });
        }
      }
    }

    return {
      siteId: args.siteId,
      siteName: site.name ?? null,
      sourceUrl,
      siteType,
      dryRun,
      batchSize,
      scanned,
      skippedNoMatch,
      seenUrlsCreated,
      seenUrlsExisted,
      seenIndexCreated,
      jobUrlKeysCreated,
      jobUrlKeysExisted,
      hasMore: !page.isDone,
      cursor: page.continueCursor,
    };
  },
});

/**
 * Backfill seen_job_urls for ALL sites.
 *
 * This iterates through all enabled sites and backfills dedup tables.
 * Call this repeatedly until hasMore is false.
 */
export const backfillSeenJobUrlsForAllSites = mutation({
  args: {
    dryRun: v.optional(v.boolean()),
    siteIndex: v.optional(v.number()),
    jobCursor: v.optional(v.string()),
    batchSize: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const dryRun = args.dryRun ?? false;
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 200, 500));
    let siteIndex = args.siteIndex ?? 0;
    let jobCursor = args.jobCursor ?? null;

    // Get all sites
    const sites = await ctx.db.query("sites").collect();
    if (siteIndex >= sites.length) {
      return {
        status: "complete",
        totalSites: sites.length,
        processedSites: sites.length,
      };
    }

    const site = sites[siteIndex] as AnyDoc;
    const sourceUrl = (site.url ?? "").trim();

    if (!sourceUrl) {
      // Skip sites without URL
      return {
        status: "skipped",
        siteIndex,
        siteName: site.name ?? null,
        reason: "no_url",
        nextSiteIndex: siteIndex + 1,
        nextJobCursor: null,
        hasMore: siteIndex + 1 < sites.length,
      };
    }

    const siteType = (site.type ?? "general").toLowerCase();
    const sourceSlug = extractGreenhouseSlugFromSource(sourceUrl);

    // Build URL prefix for job matching
    let jobUrlPrefix: string | null = null;
    if (siteType === "greenhouse" && sourceSlug) {
      jobUrlPrefix = `https://boards.greenhouse.io/${sourceSlug}/`;
    }

    // Query jobs matching this site
    const baseQuery = jobUrlPrefix
      ? ctx.db.query("jobs").withIndex("by_url", (q) =>
          q.gte("url", jobUrlPrefix!).lt("url", jobUrlPrefix! + "\uffff")
        )
      : null;

    if (!baseQuery) {
      // Skip non-greenhouse sites for now (would need different matching logic)
      return {
        status: "skipped",
        siteIndex,
        siteName: site.name ?? null,
        siteType,
        reason: "non_greenhouse",
        nextSiteIndex: siteIndex + 1,
        nextJobCursor: null,
        hasMore: siteIndex + 1 < sites.length,
      };
    }

    const page = await baseQuery.paginate({ cursor: jobCursor, numItems: batchSize });

    let processed = 0;
    let created = 0;
    let existed = 0;

    for (const job of page.page as AnyDoc[]) {
      processed += 1;
      const jobUrl = (job.url ?? "").trim();
      if (!jobUrl) continue;

      if (dryRun) continue;

      // Check if already in seen_job_urls
      const existingSeenUrl = await ctx.db
        .query("seen_job_urls")
        .withIndex("by_source_url", (q: any) =>
          q.eq("sourceUrl", sourceUrl).eq("url", jobUrl)
        )
        .first();

      if (existingSeenUrl) {
        existed += 1;
        continue;
      }

      created += 1;

      // Insert into seen_job_urls
      const createdAt = Date.now();
      const seenJobUrlId = await ctx.db.insert("seen_job_urls", {
        sourceUrl,
        url: jobUrl,
        createdAt,
      });

      // Insert into seen_job_url_index
      const existingIndex = await ctx.db
        .query("seen_job_url_index")
        .withIndex("by_url_source", (q: any) =>
          q.eq("url", jobUrl).eq("sourceUrl", sourceUrl)
        )
        .first();

      if (!existingIndex) {
        await ctx.db.insert("seen_job_url_index", {
          sourceUrl,
          url: jobUrl,
          seenJobUrlId,
          createdAt,
        });
      }

      // Insert into job_url_keys if not exists
      const key = normalizeJobUrlKey(jobUrl);
      if (key) {
        const bucket = deriveJobUrlBucket(key);
        const existingKey = await ctx.db
          .query("job_url_keys")
          .withIndex("by_bucket_url", (q: any) =>
            q.eq("bucket", bucket).eq("url", key)
          )
          .first();

        if (!existingKey) {
          await ctx.db.insert("job_url_keys", {
            bucket,
            url: key,
            jobId: job._id,
            createdAt: Date.now(),
          });
        }
      }
    }

    const siteComplete = page.isDone;
    const nextSiteIndex = siteComplete ? siteIndex + 1 : siteIndex;
    const nextJobCursor = siteComplete ? null : page.continueCursor;
    const hasMore = nextSiteIndex < sites.length;

    return {
      status: "processing",
      siteIndex,
      siteName: site.name ?? null,
      sourceUrl,
      siteType,
      dryRun,
      processed,
      created,
      existed,
      siteComplete,
      nextSiteIndex,
      nextJobCursor,
      hasMore,
      totalSites: sites.length,
    };
  },
});

/**
 * Internal mutation that runs a single iteration of the backfill loop.
 * Schedules itself to continue if there's more work to do.
 */
export const backfillSeenJobUrlsLoop = internalMutation({
  args: {
    dryRun: v.boolean(),
    siteIndex: v.number(),
    jobCursor: v.optional(v.string()),
    batchSize: v.number(),
    // Counters for final summary
    totalCreated: v.number(),
    totalExisted: v.number(),
    totalProcessed: v.number(),
    sitesProcessed: v.number(),
  },
  handler: async (ctx, args) => {
    const { dryRun, batchSize, totalCreated, totalExisted, totalProcessed, sitesProcessed } = args;
    let { siteIndex, jobCursor } = args;

    // Get all sites
    const sites = await ctx.db.query("sites").collect();
    if (siteIndex >= sites.length) {
      console.log(`[backfill] Complete! Sites: ${sitesProcessed}, Jobs: ${totalProcessed}, Created: ${totalCreated}, Existed: ${totalExisted}`);
      return;
    }

    const site = sites[siteIndex] as AnyDoc;
    const sourceUrl = (site.url ?? "").trim();
    const siteName = site.name ?? `site-${siteIndex}`;
    const siteType = (site.type ?? "general").toLowerCase();
    const sourceSlug = extractGreenhouseSlugFromSource(sourceUrl);

    // Skip sites without URL
    if (!sourceUrl) {
      console.log(`[backfill] Skipping ${siteName}: no URL`);
      await ctx.scheduler.runAfter(0, internal.admin.backfillSeenJobUrlsLoop, {
        dryRun,
        siteIndex: siteIndex + 1,
        batchSize,
        totalCreated,
        totalExisted,
        totalProcessed,
        sitesProcessed,
      });
      return;
    }

    // Build URL prefix for job matching
    let jobUrlPrefix: string | null = null;
    if (siteType === "greenhouse" && sourceSlug) {
      jobUrlPrefix = `https://boards.greenhouse.io/${sourceSlug}/`;
    }

    // Skip non-greenhouse sites for now
    if (!jobUrlPrefix) {
      console.log(`[backfill] Skipping ${siteName}: non-greenhouse (${siteType})`);
      await ctx.scheduler.runAfter(0, internal.admin.backfillSeenJobUrlsLoop, {
        dryRun,
        siteIndex: siteIndex + 1,
        batchSize,
        totalCreated,
        totalExisted,
        totalProcessed,
        sitesProcessed,
      });
      return;
    }

    // Query jobs matching this site
    const baseQuery = ctx.db.query("jobs").withIndex("by_url", (q) =>
      q.gte("url", jobUrlPrefix!).lt("url", jobUrlPrefix! + "\uffff")
    );

    const page = await baseQuery.paginate({ cursor: jobCursor ?? null, numItems: batchSize });

    let processed = 0;
    let created = 0;
    let existed = 0;

    for (const job of page.page as AnyDoc[]) {
      processed += 1;
      const jobUrl = (job.url ?? "").trim();
      if (!jobUrl) continue;

      if (dryRun) continue;

      // Check if already in seen_job_urls
      const existingSeenUrl = await ctx.db
        .query("seen_job_urls")
        .withIndex("by_source_url", (q: any) =>
          q.eq("sourceUrl", sourceUrl).eq("url", jobUrl)
        )
        .first();

      if (existingSeenUrl) {
        existed += 1;
        continue;
      }

      created += 1;

      // Insert into seen_job_urls
      const createdAt = Date.now();
      const seenJobUrlId = await ctx.db.insert("seen_job_urls", {
        sourceUrl,
        url: jobUrl,
        createdAt,
      });

      // Insert into seen_job_url_index
      const existingIndex = await ctx.db
        .query("seen_job_url_index")
        .withIndex("by_url_source", (q: any) =>
          q.eq("url", jobUrl).eq("sourceUrl", sourceUrl)
        )
        .first();

      if (!existingIndex) {
        await ctx.db.insert("seen_job_url_index", {
          sourceUrl,
          url: jobUrl,
          seenJobUrlId,
          createdAt,
        });
      }

      // Insert into job_url_keys if not exists
      const key = normalizeJobUrlKey(jobUrl);
      if (key) {
        const bucket = deriveJobUrlBucket(key);
        const existingKey = await ctx.db
          .query("job_url_keys")
          .withIndex("by_bucket_url", (q: any) =>
            q.eq("bucket", bucket).eq("url", key)
          )
          .first();

        if (!existingKey) {
          await ctx.db.insert("job_url_keys", {
            bucket,
            url: key,
            jobId: job._id,
            createdAt: Date.now(),
          });
        }
      }
    }

    const siteComplete = page.isDone;
    const marker = siteComplete ? "DONE" : "...";
    console.log(`[backfill] [${siteIndex}] ${siteName}: processed=${processed}, created=${created}, existed=${existed} ${marker}`);

    const nextSiteIndex = siteComplete ? siteIndex + 1 : siteIndex;
    const nextJobCursor = siteComplete ? undefined : page.continueCursor;
    const nextSitesProcessed = siteComplete ? sitesProcessed + 1 : sitesProcessed;
    const hasMore = nextSiteIndex < sites.length;

    if (hasMore) {
      // Schedule next iteration
      await ctx.scheduler.runAfter(0, internal.admin.backfillSeenJobUrlsLoop, {
        dryRun,
        siteIndex: nextSiteIndex,
        jobCursor: nextJobCursor,
        batchSize,
        totalCreated: totalCreated + created,
        totalExisted: totalExisted + existed,
        totalProcessed: totalProcessed + processed,
        sitesProcessed: nextSitesProcessed,
      });
    } else {
      console.log(`[backfill] Complete! Sites: ${nextSitesProcessed}, Jobs: ${totalProcessed + processed}, Created: ${totalCreated + created}, Existed: ${totalExisted + existed}`);
    }
  },
});

/**
 * Start the server-side backfill loop for all sites.
 * This runs entirely on the Convex server - no client polling needed.
 */
export const startBackfillSeenJobUrlsLoop = mutation({
  args: {
    dryRun: v.optional(v.boolean()),
    batchSize: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const dryRun = args.dryRun ?? false;
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 200, 500));

    // Count sites to be processed
    const sites = await ctx.db.query("sites").collect();
    const greenhouseSites = sites.filter((site: AnyDoc) => {
      const siteType = ((site as AnyDoc).type ?? "general").toLowerCase();
      const sourceUrl = ((site as AnyDoc).url ?? "").trim();
      const sourceSlug = extractGreenhouseSlugFromSource(sourceUrl);
      return siteType === "greenhouse" && sourceSlug;
    });

    console.log(`[backfill] Starting backfill loop: ${greenhouseSites.length} greenhouse sites of ${sites.length} total`);

    // Schedule the first iteration
    await ctx.scheduler.runAfter(0, internal.admin.backfillSeenJobUrlsLoop, {
      dryRun,
      siteIndex: 0,
      batchSize,
      totalCreated: 0,
      totalExisted: 0,
      totalProcessed: 0,
      sitesProcessed: 0,
    });

    return {
      started: true,
      dryRun,
      batchSize,
      totalSites: sites.length,
      greenhouseSites: greenhouseSites.length,
      message: "Backfill loop started. Check Convex dashboard logs for progress.",
    };
  },
});

/**
 * Batch patch jobs by URL with posting date fields.
 * Used for backfilling posting dates efficiently.
 */
export const batchPatchPostingDates = mutation({
  args: {
    updates: v.array(
      v.object({
        urls: v.array(v.string()),
        postedAt: v.optional(v.number()),
        postingFirstPublishedAt: v.optional(v.number()),
      })
    ),
  },
  handler: async (ctx, args) => {
    let updated = 0;
    let notFound = 0;

    for (const update of args.updates) {
      let job: AnyDoc | null = null;

      for (const url of update.urls) {
        const found = await ctx.db
          .query("jobs")
          .withIndex("by_url", (q) => q.eq("url", url))
          .first();
        if (found) {
          job = found;
          break;
        }
      }

      if (!job) {
        notFound++;
        continue;
      }

      const patchData: Record<string, any> = {};

      if (typeof update.postedAt === "number") {
        patchData.postedAt = update.postedAt;
        patchData.postedAtUnknown = false;
      }
      if (typeof update.postingFirstPublishedAt === "number") {
        patchData.postingFirstPublishedAt = update.postingFirstPublishedAt;
      }

      if (Object.keys(patchData).length > 0) {
        await ctx.db.patch(job._id, patchData);
        updated++;
      }
    }

    return { updated, notFound };
  },
});
