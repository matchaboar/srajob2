import { mutation, query } from "./_generated/server";
import type { TableNames } from "./_generated/dataModel";
import { v } from "convex/values";
import { normalizeCompanyFilterKey } from "./jobs";
import { deleteDescriptionFromStorage } from "./jobDescriptionStorage";

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
    for (const row of page.page as AnyDoc[]) {
      const companyValue = row.company ?? "";
      const companyKey = row.companyKey ?? "";
      if (!matchesCompany(companyValue) && !matchesCompany(companyKey)) {
        continue;
      }
      deleted += 1;
      if (dryRun) continue;
      const details = await ctx.db
        .query("job_details")
        .withIndex("by_job", (q) => q.eq("jobId", row._id))
        .collect();
      for (const detail of details as AnyDoc[]) {
        await deleteDescriptionFromStorage(ctx, detail.descriptionStorageId);
        await ctx.db.delete(detail._id);
      }
      await ctx.db.delete(row._id);
    }

    return {
      company,
      dryRun,
      batchSize,
      scanned: page.page.length,
      deleted,
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
    const results: Array<{ jobId: string; hadJob: boolean; details: number }> = [];
    let deletedJobs = 0;
    let deletedDetails = 0;

    for (const jobId of args.jobIds) {
      const job = await ctx.db.get(jobId);
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
        if (job) {
          await ctx.db.delete(jobId);
        }
      }

      if (job) {
        deletedJobs += 1;
      }
      deletedDetails += detailCount;
      results.push({ jobId: String(jobId), hadJob: Boolean(job), details: detailCount });
    }

    return {
      dryRun,
      deletedJobs,
      deletedDetails,
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

