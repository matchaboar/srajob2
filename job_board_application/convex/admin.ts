import { mutation, query } from "./_generated/server";
import type { TableNames } from "./_generated/dataModel";
import { v } from "convex/values";

type AnyDoc = Record<string, any>;
type WipeTable =
  | "jobs"
  | "scrapes"
  | "scrape_activity"
  | "scrape_url_queue"
  | "seen_job_urls"
  | "ignored_jobs"
  | "scrape_errors"
  | "run_requests"
  | "workflow_run_sites"
  | "scratchpad_entries";

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
      v.literal("scrape_url_queue"),
      v.literal("seen_job_urls"),
      v.literal("ignored_jobs"),
      v.literal("scrape_errors"),
      v.literal("run_requests"),
      v.literal("workflow_run_sites"),
      v.literal("scratchpad_entries")
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
        case "scrape_url_queue":
          return ctx.db
            .query("scrape_url_queue")
            .withIndex("by_url", (q) => q.gte("url", prefix).lt("url", prefixUpper));
        case "ignored_jobs":
          return ctx.db
            .query("ignored_jobs")
            .withIndex("by_url", (q) => q.gte("url", prefix).lt("url", prefixUpper));
        case "seen_job_urls":
          return ctx.db
            .query("seen_job_urls")
            .withIndex("by_source", (q) => q.gte("sourceUrl", prefix).lt("sourceUrl", prefixUpper));
        case "workflow_run_sites":
          return ctx.db
            .query("workflow_run_sites")
            .withIndex("by_site", (q) => q.gte("siteUrl", prefix).lt("siteUrl", prefixUpper));
        case "scratchpad_entries":
          return ctx.db
            .query("scratchpad_entries")
            .withIndex("by_site", (q) => q.gte("siteUrl", prefix).lt("siteUrl", prefixUpper));
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
        case "scrape_url_queue":
          if (row.siteId && siteIds.has(row.siteId)) return true;
          return matchesUrl(row.url) || matchesUrl(row.sourceUrl);
        case "ignored_jobs":
          return matchesUrl(row.url) || matchesUrl(row.sourceUrl);
        case "seen_job_urls":
          return matchesUrl(row.url) || matchesUrl(row.sourceUrl);
        case "scrape_errors":
          if (row.siteId && siteIdStrings.has(String(row.siteId))) return true;
          return matchesUrl(row.sourceUrl);
        case "run_requests":
          if (row.siteId && siteIds.has(row.siteId)) return true;
          return matchesUrl(row.siteUrl);
        case "workflow_run_sites":
          return matchesUrl(row.siteUrl);
        case "scratchpad_entries":
          if (row.siteId && siteIds.has(row.siteId)) return true;
          return matchesUrl(row.siteUrl);
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
