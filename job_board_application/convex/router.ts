import { httpRouter } from "convex/server";
import { httpAction, internalMutation, mutation, query } from "./_generated/server";
import { v } from "convex/values";
import { api } from "./_generated/api";
import type { Id } from "./_generated/dataModel";
import { splitLocation, formatLocationLabel, deriveLocationFields } from "./location";
import { runFirecrawlCors } from "./middleware/firecrawlCors";
import { parseFirecrawlWebhook } from "./firecrawlWebhookUtil";
import { buildJobInsert } from "./jobRecords";
import { fallbackCompanyNameFromUrl, greenhouseSlugFromUrl, normalizeSiteUrl, siteCanonicalKey } from "./siteUtils";

const http = httpRouter();
const SCRAPE_URL_QUEUE_TTL_MS = 48 * 60 * 60 * 1000; // 48 hours
const JOB_DETAIL_MAX_ATTEMPTS = 3;
const DEFAULT_TIMEZONE = "America/Denver";
const UNKNOWN_COMPENSATION_REASON = "pending markdown structured extraction";
const HEURISTIC_VERSION = 4;
const toSlug = (value: string) =>
  (value || "")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)+/g, "") || "unknown";
const hostFromUrl = (url: string) => {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return "";
  }
};
const baseDomainFromHost = (host: string): string => {
  const parts = host.split(".").filter(Boolean);
  if (parts.length <= 1) return host;
  const last = parts[parts.length - 1];
  const secondLast = parts[parts.length - 2];
  const shouldUseThree = secondLast.length === 2 || last.length === 2;
  if (shouldUseThree && parts.length >= 3) {
    return parts.slice(-3).join(".");
  }
  return parts.slice(-2).join(".");
};
const normalizeCompany = (value: string) => (value || "").toLowerCase().replace(/[^a-z0-9]/g, "");
const fallbackCompanyName = (name: string | undefined | null, url: string | undefined | null) => {
  const trimmed = (name ?? "").trim();
  if (trimmed) return trimmed;
  const host = hostFromUrl(url ?? "");
  if (host) {
    const base = baseDomainFromHost(host);
    const parts = base.split(".");
    if (parts.length > 1) return parts[0];
    return base;
  }
  return fallbackCompanyNameFromUrl(url ?? "");
};
const normalizeDomainInput = (value: string): string => {
  const trimmed = (value || "").trim();
  if (!trimmed) return "";

  try {
    const parsed = new URL(trimmed.includes("://") ? trimmed : `https://${trimmed}`);
    const host = parsed.hostname.toLowerCase();
    const greenhouseSlug = greenhouseSlugFromUrl(parsed.href);
    const greenhouse = greenhouseSlug ? `${greenhouseSlug}.greenhouse.io` : null;
    if (greenhouse) return greenhouse;
    return baseDomainFromHost(host);
  } catch {
    const hostOnly = trimmed.replace(/^https?:\/\//i, "").split("/")[0] || trimmed;
    const host = hostOnly.toLowerCase();
    const greenhouseSlug = greenhouseSlugFromUrl(host);
    const greenhouse = greenhouseSlug ? `${greenhouseSlug}.greenhouse.io` : null;
    if (greenhouse) return greenhouse;
    return baseDomainFromHost(host);
  }
};
const deriveNameFromDomain = (domain: string): string => {
  if (!domain) return "Site";
  return fallbackCompanyName(undefined, `https://${domain}`);
};
const resolveCompanyForUrl = async (
  ctx: any,
  url: string,
  currentCompany: string,
  siteName?: string,
  cache?: Map<string, string | null>
) => {
  const domain = normalizeDomainInput(url);
  const aliasCache = cache ?? new Map<string, string | null>();
  let alias: string | null = null;

  if (domain) {
    if (aliasCache.has(domain)) {
      alias = aliasCache.get(domain) ?? null;
    } else {
      const match = await ctx.db
        .query("domain_aliases")
        .withIndex("by_domain", (q: any) => q.eq("domain", domain))
        .first();
      alias = typeof match?.alias === "string" && match.alias.trim() ? match.alias.trim() : null;
      aliasCache.set(domain, alias);
    }
  }

  const chosen = alias ?? siteName ?? currentCompany;
  return chosen?.trim() || fallbackCompanyName(currentCompany, url);
};
const upsertCompanyProfile = async (
  ctx: any,
  name: string,
  url?: string | null,
  previousName?: string | null
) => {
  const normalizedName = (name || "").trim() || fallbackCompanyName(name, url);
  const slug = toSlug(normalizedName);
  const now = Date.now();
  const domain = baseDomainFromHost(hostFromUrl(url ?? ""));

  const existing = await ctx.db
    .query("company_profiles")
    .withIndex("by_slug", (q: any) => q.eq("slug", slug))
    .first();
  const aliases = new Set<string>((existing?.aliases ?? []).filter(Boolean));
  const domains = new Set<string>((existing?.domains ?? []).filter(Boolean));
  if (previousName && previousName.trim().toLowerCase() !== normalizedName.toLowerCase()) {
    aliases.add(previousName.trim());
  }
  if (domain) domains.add(domain);

  if (existing) {
    await ctx.db.patch(existing._id, {
      name: normalizedName,
      aliases: aliases.size ? Array.from(aliases) : undefined,
      domains: domains.size ? Array.from(domains) : undefined,
      updatedAt: now,
    });
    return existing._id;
  }

  const insertPayload: any = {
    slug,
    name: normalizedName,
    updatedAt: now,
    createdAt: now,
  };
  if (aliases.size) insertPayload.aliases = Array.from(aliases);
  if (domains.size) insertPayload.domains = Array.from(domains);

  return await ctx.db.insert("company_profiles", insertPayload);
};
const _collectRows = async (cursorable: any) => {
  if (!cursorable) return [];
  if (typeof cursorable.collect === "function") {
    return await cursorable.collect();
  }
  if (typeof cursorable.paginate === "function") {
    let cursor: any = null;
    const rows: any[] = [];
    while (true) {
      const { page, isDone, continueCursor } = await cursorable.paginate({ cursor, numItems: 200 });
      rows.push(...(page || []));
      if (isDone || !continueCursor) break;
      cursor = continueCursor;
    }
    return rows;
  }
  return [];
};

const updateJobsCompany = async (ctx: any, oldName: string, nextName: string) => {
  const prev = (oldName || "").trim();
  const next = (nextName || "").trim();
  if (!prev || !next || prev === next) return 0;

  const prevNorm = normalizeCompany(prev);
  const nextNorm = normalizeCompany(next);
  if (!prevNorm || prevNorm === nextNorm) return 0;

  const candidates = new Set<string>();
  candidates.add(prev);
  const lowered = prev.toLowerCase();
  if (lowered) candidates.add(lowered);
  const capitalized = lowered ? lowered.charAt(0).toUpperCase() + lowered.slice(1) : "";
  if (capitalized) candidates.add(capitalized);

  const patchedIds = new Set<string>();

  const patchJob = async (job: any) => {
    const id = String(job?._id ?? "");
    if (!id || patchedIds.has(id)) return;
    const company = (job).company ?? "";
    if (normalizeCompany(company) !== prevNorm) return;
    await ctx.db.patch(job._id, { company: next });
    patchedIds.add(id);
  };

  for (const candidate of candidates) {
    if (!candidate) continue;
    const rows = await _collectRows(
      ctx.db.query("jobs").withIndex("by_company", (q: any) => q.eq("company", candidate))
    );
    for (const job of rows as any[]) {
      await patchJob(job);
    }
  }

  // Fallback: search index to catch mixed-case / spaced variants
  try {
    const searchMatches = await _collectRows(ctx.db.search("jobs", "search_company", prev));
    for (const job of searchMatches as any[]) {
      await patchJob(job);
    }
  } catch {
    // search index unavailable; best-effort
  }

  return patchedIds.size;
};

const updateJobsCompanyByDomain = async (ctx: any, domain: string, nextName: string) => {
  const normalizedDomain = (domain || "").trim();
  const next = (nextName || "").trim();
  if (!normalizedDomain || !next) return 0;
  const nextNorm = normalizeCompany(next);
  if (!nextNorm) return 0;

  const jobs = await _collectRows(ctx.db.query("jobs"));
  let updated = 0;
  for (const job of jobs as any[]) {
    const jobUrl = typeof job?.url === "string" ? job.url : "";
    if (!jobUrl) continue;
    const jobDomain = normalizeDomainInput(jobUrl);
    if (!jobDomain || jobDomain !== normalizedDomain) continue;
    const currentCompany = typeof job?.company === "string" ? job.company : "";
    if (normalizeCompany(currentCompany) === nextNorm) continue;
    await ctx.db.patch(job._id, { company: next });
    updated += 1;
  }
  return updated;
};
const scheduleDay = v.union(
  v.literal("mon"),
  v.literal("tue"),
  v.literal("wed"),
  v.literal("thu"),
  v.literal("fri"),
  v.literal("sat"),
  v.literal("sun")
);
type ScheduleDay = "sun" | "mon" | "tue" | "wed" | "thu" | "fri" | "sat";
const weekdayFromShort: Record<string, ScheduleDay> = {
  Sun: "sun",
  Mon: "mon",
  Tue: "tue",
  Wed: "wed",
  Thu: "thu",
  Fri: "fri",
  Sat: "sat",
};

const parseTimeToMinutes = (value?: string) => {
  const match = (value ?? "").match(/^(\d{2}):(\d{2})$/);
  if (!match) return 0;
  const hours = parseInt(match[1] ?? "0", 10);
  const minutes = parseInt(match[2] ?? "0", 10);
  return Math.max(0, Math.min(23, hours)) * 60 + Math.max(0, Math.min(59, minutes));
};

const zonedParts = (nowMs: number, timeZone: string) => {
  let formatter: Intl.DateTimeFormat;
  try {
    formatter = new Intl.DateTimeFormat("en-US", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
      weekday: "short",
    });
  } catch {
    formatter = new Intl.DateTimeFormat("en-US", {
      timeZone: DEFAULT_TIMEZONE,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
      weekday: "short",
    });
  }

  const parts = formatter.formatToParts(nowMs);
  const get = (type: string) => parts.find((p) => p.type === type)?.value ?? "00";
  const year = parseInt(get("year"), 10);
  const month = parseInt(get("month"), 10);
  const day = parseInt(get("day"), 10);
  const hour = parseInt(get("hour"), 10);
  const minute = parseInt(get("minute"), 10);
  const second = parseInt(get("second"), 10);
  const weekday = weekdayFromShort[get("weekday")] ?? "sun";

  // Calculate offset for this instant in the target timezone.
  const asUtc = Date.UTC(year, month - 1, day, hour, minute, second);
  const offsetMs = nowMs - asUtc;

  return {
    year,
    month,
    day,
    hour,
    minute,
    weekday,
    offsetMs,
  };
};

const latestEligibleTime = (
  schedule:
    | {
        days: ("mon" | "tue" | "wed" | "thu" | "fri" | "sat" | "sun")[];
        startTime?: string | null;
        intervalMinutes?: number | null;
        timezone?: string | null;
      }
    | null
    | undefined,
  nowMs: number
) => {
  if (!schedule) return null;
  const timeZone = schedule.timezone || DEFAULT_TIMEZONE;
  const parts = zonedParts(nowMs, timeZone);
  const dayKey = parts.weekday;
  if (!schedule.days.includes(dayKey)) return null;

  const minutesNow = parts.hour * 60 + parts.minute;
  const startMinutes = parseTimeToMinutes(schedule.startTime ?? "00:00");
  if (minutesNow < startMinutes) return null;

  const interval = Math.max(1, Math.floor(schedule.intervalMinutes ?? 24 * 60));
  const steps = Math.floor((minutesNow - startMinutes) / interval);
  const minutesAtSlot = startMinutes + steps * interval;

  const dayStartUtc = Date.UTC(parts.year, parts.month - 1, parts.day, 0, 0, 0);
  return dayStartUtc + parts.offsetMs + minutesAtSlot * 60 * 1000;
};

/**
 * API endpoint for posting new jobs
 *
 * POST /api/jobs
 * Content-Type: application/json
 * 
 * Body:
 * {
 *   "title": "Software Engineer",
 *   "company": "Tech Corp",
 *   "description": "We are looking for...",
 *   "location": "San Francisco, CA",
 *   "remote": true,
 *   "level": "mid",
 *   "totalCompensation": 150000,
 *   "url": "https://company.com/jobs/123",
 *   // Optional; mark as internal/test so UI can ignore
 *   "test": false
 * }
 * 
 * Response:
 * {
 *   "success": true,
 *   "jobId": "job_id_here"
 * }
 */
http.route({
  path: "/api/jobs",
  method: "POST",
  handler: httpAction(async (ctx, request) => {
    try {
      const body = await request.json();

      // Validate required fields
      const requiredFields = ["title", "company", "description", "location", "remote", "level", "totalCompensation", "url"];
      for (const field of requiredFields) {
        if (!(field in body)) {
          return new Response(
            JSON.stringify({ error: `Missing required field: ${field}` }),
            { status: 400, headers: { "Content-Type": "application/json" } }
          );
        }
      }

      // Validate level enum
      const validLevels = ["junior", "mid", "senior", "staff"];
      if (!validLevels.includes(body.level)) {
        return new Response(
          JSON.stringify({ error: `Invalid level. Must be one of: ${validLevels.join(", ")}` }),
          { status: 400, headers: { "Content-Type": "application/json" } }
        );
      }

      const { city, state } = splitLocation(body.location);
      const locationLabel = formatLocationLabel(city, state, body.location);

      const jobId = await ctx.runMutation(api.router.insertJobRecord, {
        title: body.title,
        company: body.company,
        description: body.description,
        location: locationLabel,
        city,
        state,
        remote: body.remote,
        level: body.level,
        totalCompensation: body.totalCompensation,
        url: body.url,
        test: body.test ?? false,
      });

      return new Response(
        JSON.stringify({ success: true, jobId }),
        { status: 201, headers: { "Content-Type": "application/json" } }
      );
    } catch {
      return new Response(
        JSON.stringify({ error: "Invalid JSON body" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }
  }),
});

/**
 * API endpoint to list sites to scrape
 *
 * GET /api/sites
 * Response: [{ _id, name, url, pattern, enabled, lastRunAt }]
 */
http.route({
  path: "/api/sites",
  method: "GET",
  handler: httpAction(async (ctx, _request) => {
    const sites = await ctx.runQuery(api.router.listSites, { enabledOnly: true });
    return new Response(JSON.stringify(sites), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }),
});

http.route({
  path: "/api/jobs/existing",
  method: "POST",
  handler: httpAction(async (ctx, request) => {
    try {
      const body = await request.json();
      const urls: string[] = Array.isArray(body?.urls)
        ? (body.urls as any[]).filter((u) => typeof u === "string" && u.trim()).map((u) => String(u))
        : [];

      if (urls.length === 0) {
        return new Response(JSON.stringify({ existing: [] }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }

      const res = await ctx.runQuery(api.router.findExistingJobUrls, { urls });
      return new Response(JSON.stringify(res), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    } catch {
      return new Response(
        JSON.stringify({ error: "Invalid JSON body" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }
  }),
});

// HTTP endpoint to fetch previously seen job URLs for a site so scrapers can skip them
http.route({
  path: "/api/sites/skip-urls",
  method: "POST",
  handler: httpAction(async (ctx, request) => {
    const body = await request.json();
    if (!body?.sourceUrl) {
      return new Response(JSON.stringify({ error: "sourceUrl is required" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }

    const res = await ctx.runQuery(api.router.listSeenJobUrlsForSite, {
      sourceUrl: body.sourceUrl,
      pattern: body.pattern ?? undefined,
    });

    return new Response(JSON.stringify(res), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }),
});

http.route({
  path: "/api/sites",
  method: "POST",
  handler: httpAction(async (ctx, request) => {
    try {
      const body = await request.json();
      const id = await ctx.runMutation(api.router.upsertSite, {
        name: body.name ?? undefined,
        url: body.url,
        type: body.type ?? "general",
        pattern: body.pattern ?? undefined,
        scheduleId: body.scheduleId ?? undefined,
        enabled: body.enabled ?? true,
      });
      return new Response(JSON.stringify({ success: true, id }), {
        status: 201,
        headers: { "Content-Type": "application/json" },
      });
    } catch {
      return new Response(
        JSON.stringify({ error: "Invalid JSON body" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }
  }),
});

http.route({
  path: "/api/sites/activity",
  method: "GET",
  handler: httpAction(async (ctx) => {
    // listScrapeActivity may not be present in generated types during CI; cast to any for safety.
    const rows = await ctx.runQuery((api as any).sites.listScrapeActivity, {});
    return new Response(JSON.stringify(rows), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }),
});

export const listSchedules = query({
  args: {},
  returns: v.array(
    v.object({
      _id: v.id("scrape_schedules"),
      name: v.string(),
      days: v.array(scheduleDay),
      startTime: v.string(),
      intervalMinutes: v.number(),
      timezone: v.optional(v.string()),
      createdAt: v.number(),
      updatedAt: v.number(),
      siteCount: v.number(),
    })
  ),
  handler: async (ctx) => {
    const schedules = await ctx.db.query("scrape_schedules").collect();
    const siteCounts = new Map<string, number>();
    const sites = await ctx.db.query("sites").collect();

    for (const site of sites as any[]) {
      const sid = (site).scheduleId as string | undefined;
      if (sid) {
        siteCounts.set(sid, (siteCounts.get(sid) ?? 0) + 1);
      }
    }

    return (schedules as any[])
      .map((s) => ({
        _id: s._id,
        name: s.name,
        days: s.days,
        startTime: s.startTime,
        intervalMinutes: s.intervalMinutes,
        timezone: s.timezone ?? DEFAULT_TIMEZONE,
        createdAt: s.createdAt,
        updatedAt: s.updatedAt,
        siteCount: siteCounts.get((s)._id) ?? 0,
      }))
      .sort((a: any, b: any) => a.name.localeCompare(b.name));
  },
});

export const upsertSchedule = mutation({
  args: {
    id: v.optional(v.id("scrape_schedules")),
    name: v.string(),
    days: v.array(scheduleDay),
    startTime: v.string(),
    intervalMinutes: v.number(),
    timezone: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    if (!args.days.length) {
      throw new Error("At least one day must be selected");
    }
    if (!/^\d{2}:\d{2}$/.test(args.startTime)) {
      throw new Error("Start time must be in HH:MM format");
    }
    const now = Date.now();
    const normalizedName = args.name.trim() || "Untitled schedule";
    const normalizedDays = Array.from(new Set(args.days));
    const interval = Math.max(1, Math.floor(args.intervalMinutes));
    const timezone = (args.timezone || DEFAULT_TIMEZONE).trim() || DEFAULT_TIMEZONE;

    if (args.id) {
      await ctx.db.patch(args.id, {
        name: normalizedName,
        days: normalizedDays,
        startTime: args.startTime,
        intervalMinutes: interval,
        timezone,
        updatedAt: now,
      });
      return args.id;
    }

    return await ctx.db.insert("scrape_schedules", {
      name: normalizedName,
      days: normalizedDays,
      startTime: args.startTime,
      intervalMinutes: interval,
      timezone,
      createdAt: now,
      updatedAt: now,
    });
  },
});

export const deleteSchedule = mutation({
  args: { id: v.id("scrape_schedules") },
  handler: async (ctx, args) => {
    const inUse = await ctx.db
      .query("sites")
      .withIndex("by_schedule", (q) => q.eq("scheduleId", args.id))
      .first();
    if (inUse) {
      throw new Error("Cannot delete a schedule that is assigned to sites");
    }
    await ctx.db.delete(args.id);
    return { success: true };
  },
});

const updateSiteScheduleHandler = async (ctx: any, args: { id: Id<"sites">; scheduleId?: Id<"scrape_schedules"> }) => {
  const site = await ctx.db.get(args.id);
  if (!site) {
    throw new Error("Site not found");
  }

  const updates: Record<string, any> = { scheduleId: args.scheduleId };

  // If a new schedule is attached and its window for today has already started,
  // backdate lastRunAt so the site is eligible immediately.
  if (args.scheduleId && args.scheduleId !== (site).scheduleId) {
    const sched = await ctx.db.get(args.scheduleId);
    if (sched) {
      const eligibleAt = latestEligibleTime(
        {
          days: (sched).days ?? [],
          startTime: (sched).startTime,
          intervalMinutes: (sched).intervalMinutes,
          timezone: (sched).timezone,
        },
        Date.now()
      );
      if (eligibleAt !== null && eligibleAt <= Date.now()) {
        const currentLast = (site).lastRunAt ?? 0;
        const desiredLast = Math.max(0, Math.min(currentLast, eligibleAt - 1));
        if (desiredLast < currentLast) {
          updates.lastRunAt = desiredLast;
        }
      }
    }
  }

  await ctx.db.patch(args.id, updates);
  return args.id;
};

export const updateSiteSchedule = mutation({
  args: {
    id: v.id("sites"),
    scheduleId: v.optional(v.id("scrape_schedules")),
  },
  handler: updateSiteScheduleHandler,
});
(updateSiteSchedule as any).handler = updateSiteScheduleHandler;

export const listSites = query({
  args: { enabledOnly: v.boolean() },
  handler: async (ctx, args) => {
    const q = ctx.db.query("sites");
    if (args.enabledOnly) {
      return await q.withIndex("by_enabled", (q2) => q2.eq("enabled", true)).collect();
    }
    return await q.collect();
  },
});

// Gather previously seen job URLs for a site (from scrapes + ignored) so scrapers can skip them
export const listSeenJobUrlsForSite = query({
  args: {
    sourceUrl: v.string(),
    pattern: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    const seen = new Set<string>();

    const scrapes = await ctx.db
      .query("scrapes")
      .withIndex("by_source", (q) => q.eq("sourceUrl", args.sourceUrl))
      .collect();

    for (const scrape of scrapes as any[]) {
      const jobs = extractJobs((scrape).items);
      for (const job of jobs) {
        if (job.url) seen.add(job.url);
      }
    }

    const matcher = buildUrlMatcher(args.pattern ?? args.sourceUrl);

    const ignored = await ctx.db
      .query("ignored_jobs")
      .withIndex("by_source", (q) => q.eq("sourceUrl", args.sourceUrl))
      .collect();
    for (const row of ignored as any[]) {
      const url = (row).url;
      if (typeof url === "string" && matcher(url)) {
        seen.add(url);
      }
    }

    return { sourceUrl: args.sourceUrl, urls: Array.from(seen) };
  },
});

// Atomically lease the next available site for scraping.
// Excludes completed sites and honors locks.
export const leaseSite = mutation({
  args: {
    workerId: v.string(),
    lockSeconds: v.optional(v.number()),
    siteType: v.optional(v.union(v.literal("general"), v.literal("greenhouse"), v.literal("avature"))),
    scrapeProvider: v.optional(
      v.union(
        v.literal("fetchfox"),
        v.literal("firecrawl"),
        v.literal("spidercloud"),
        v.literal("fetchfox_spidercloud")
      )
    ),
  },
  handler: async (ctx, args) => {
    const now = Date.now();
    const ttlMs = Math.max(1, Math.floor((args.lockSeconds ?? 300) * 1000));
    const requestedType = args.siteType;
    const requestedProvider = args.scrapeProvider;

    // Pull enabled sites and pick the first that is not completed and not locked (or lock expired)
    const candidates = await ctx.db
      .query("sites")
      .withIndex("by_enabled", (q) => q.eq("enabled", true))
      .collect();

    const eligible: any[] = [];
    const scheduleCache = new Map<string, any>();

    for (const site of candidates as any[]) {
      const siteType = (site).type ?? "general";
      const scrapeProvider =
        (site).scrapeProvider ??
        (siteType === "greenhouse" || siteType === "avature" ? "spidercloud" : "fetchfox");
      const hasSchedule = !!(site).scheduleId;
      const lastRun = (site).lastRunAt ?? 0;
      const manualTriggerAt = (site).manualTriggerAt ?? 0;
      if (requestedType && siteType !== requestedType) continue;
      if (requestedProvider && scrapeProvider !== requestedProvider) continue;
      if (site.completed && !hasSchedule) continue;
      if (site.failed) continue;
      if (site.lockExpiresAt && site.lockExpiresAt > now) continue;

      // Manual trigger: bypass schedule/time gating for a short window
      if (manualTriggerAt && manualTriggerAt > now - 15 * 60 * 1000 && manualTriggerAt > lastRun) {
        eligible.push({ site, eligibleAt: manualTriggerAt });
        continue;
      }

      // If a schedule is assigned, ensure the site is currently eligible
      if (site.scheduleId) {
        const cacheKey = site.scheduleId as string;
        let sched = scheduleCache.get(cacheKey);
        if (sched === undefined) {
          sched = await ctx.db.get(site.scheduleId as Id<"scrape_schedules">);
          scheduleCache.set(cacheKey, sched);
        }

        const eligibleAt = latestEligibleTime(sched, now);
        if (!eligibleAt) continue;

        if (lastRun >= eligibleAt) continue;

        eligible.push({ site, eligibleAt });
        continue;
      }

      // No schedule: treat as always eligible
      eligible.push({ site, eligibleAt: lastRun });
    }

    const pick = eligible
      .sort((a, b) => {
        // Prefer sites whose eligible slot is oldest
        return (a.eligibleAt ?? 0) - (b.eligibleAt ?? 0);
      })
      .map((row) => row.site)[0];

    if (!pick) return null;

    await ctx.db.patch(pick._id, {
      lockedBy: args.workerId,
      lockExpiresAt: now + ttlMs,
    });
    // Return minimal fields for the worker
    const fresh = await ctx.db.get(pick._id as Id<"sites">);
    if (!fresh) return null;
    const s = fresh;
    const resolvedProvider =
      (s as any).scrapeProvider ??
      ((s as any).type === "greenhouse" || (s as any).type === "avature"
        ? "spidercloud"
        : "fetchfox");
    return {
      _id: s._id,
      name: s.name,
      url: s.url,
      type: (s as any).type ?? "general",
      scrapeProvider: resolvedProvider,
      pattern: s.pattern,
      scheduleId: s.scheduleId,
      enabled: s.enabled,
      lastRunAt: s.lastRunAt,
      lockedBy: s.lockedBy,
      lockExpiresAt: s.lockExpiresAt,
      completed: s.completed,
      failed: (s as any).failed,
      failCount: (s as any).failCount,
      manualTriggerAt: (s as any).manualTriggerAt,
    };
  },
});

export const insertIgnoredJob = mutation({
  args: {
    url: v.string(),
    sourceUrl: v.optional(v.string()),
    reason: v.optional(v.string()),
    provider: v.optional(v.string()),
    workflowName: v.optional(v.string()),
    details: v.optional(v.any()),
    title: v.optional(v.string()),
    description: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    return await ctx.db.insert("ignored_jobs", {
      url: args.url,
      sourceUrl: args.sourceUrl,
      reason: args.reason,
      provider: args.provider,
      workflowName: args.workflowName,
      details: args.details,
      title: args.title,
      description: args.description,
      createdAt: Date.now(),
    });
  },
});

export const listIgnoredJobs = query({
  args: {
    limit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const limit = Math.max(1, Math.min(args.limit ?? 200, 400));
    const rows = await ctx.db.query("ignored_jobs").order("desc").take(limit);
    return rows.map((row: any) => ({
      _id: row._id,
      url: row.url,
      sourceUrl: row.sourceUrl,
      reason: row.reason,
      provider: row.provider,
      workflowName: row.workflowName,
      details: row.details,
      title: row.title,
      description: row.description,
      createdAt: row.createdAt,
    }));
  },
});

export const clearIgnoredJobsForSource = mutation({
  args: {
    sourceUrl: v.string(),
    reason: v.optional(v.string()),
    provider: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    const rows = await ctx.db
      .query("ignored_jobs")
      .withIndex("by_source", (q) => q.eq("sourceUrl", args.sourceUrl))
      .collect();

    let deleted = 0;
    for (const row of rows as any[]) {
      if (args.reason && row.reason !== args.reason) continue;
      if (args.provider && row.provider !== args.provider) continue;
      await ctx.db.delete(row._id);
      deleted += 1;
    }

    return { deleted };
  },
});

// Mark a leased site as completed and clear its lock.
export const completeSite = mutation({
  args: { id: v.id("sites") },
  handler: async (ctx, args) => {
    const now = Date.now();
    await ctx.db.patch(args.id, {
      completed: true,
      lockedBy: "",
      lockExpiresAt: 0,
      lastRunAt: now,
      // One-off manual triggers should not keep re-leasing after a successful run.
      manualTriggerAt: 0,
    });
    return { success: true };
  },
});

// Clear a lock without completing, e.g., on failure.
export const releaseSite = mutation({
  args: { id: v.id("sites") },
  handler: async (ctx, args) => {
    await ctx.db.patch(args.id, {
      lockedBy: "",
      lockExpiresAt: 0,
    });
    return { success: true };
  },
});

export const listQueuedScrapeUrls = query({
  args: {
    siteId: v.optional(v.id("sites")),
    provider: v.optional(v.string()),
    status: v.optional(
      v.union(v.literal("pending"), v.literal("processing"), v.literal("completed"), v.literal("failed")),
    ),
    limit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const limit = Math.max(1, Math.min(args.limit ?? 200, 500));
    const baseQuery = ctx.db.query("scrape_url_queue");
    const status = args.status;
    const siteId = args.siteId;
    let rows: any[] = [];

    if (siteId && status) {
      rows = await baseQuery
        .withIndex("by_site_status", (qi) => qi.eq("siteId", siteId).eq("status", status))
        .order("asc")
        .take(limit);
    } else if (status) {
      rows = await baseQuery.withIndex("by_status", (qi) => qi.eq("status", status)).order("asc").take(limit);
    } else if (siteId) {
      const statuses: Array<"pending" | "processing" | "completed" | "failed"> = [
        "pending",
        "processing",
        "completed",
        "failed",
      ];
      let remaining = limit;
      for (const statusValue of statuses) {
        if (remaining <= 0) break;
        const batch = await baseQuery
          .withIndex("by_site_status", (qi) => qi.eq("siteId", siteId).eq("status", statusValue))
          .order("asc")
          .take(remaining);
        rows.push(...batch);
        remaining = limit - rows.length;
      }
    } else {
      rows = await baseQuery.order("asc").take(limit);
    }

    return rows
      .filter((row: any) => {
        if (args.siteId && row.siteId !== args.siteId) return false;
        if (args.provider && row.provider !== args.provider) return false;
        return true;
      })
      .map((row) => ({
        _id: row._id,
        url: row.url,
        sourceUrl: row.sourceUrl,
        provider: row.provider,
        siteId: row.siteId,
        pattern: row.pattern,
        status: row.status,
        attempts: row.attempts,
        lastError: row.lastError,
        createdAt: row.createdAt,
        updatedAt: row.updatedAt,
        completedAt: row.completedAt,
      }));
  },
});

// Mark a site to be picked up immediately on the next workflow run
export const runSiteNow = mutation({
  args: { id: v.id("sites") },
  handler: async (ctx, args) => {
    const now = Date.now();
    await ctx.db.patch(args.id, {
      completed: false,
      failed: false,
      lockedBy: "",
      lockExpiresAt: 0,
      lastRunAt: 0,
      lastFailureAt: undefined,
      lastError: undefined,
      // Hint to dashboards + leasing logic to pick up immediately
      manualTriggerAt: now,
    } as any);

    try {
      await ctx.db.insert("run_requests", {
        siteId: args.id,
        siteUrl: (await ctx.db.get(args.id))?.url ?? "",
        status: "pending",
        createdAt: now,
        expectedEta: now + 15_000, // next SiteLease tick (~15s interval)
        completedAt: undefined,
      });
    } catch (err) {
      // best-effort; don't block the manual trigger
      console.error("Failed to record run request", err);
    }
    return { success: true };
  },
});

export const enqueueScrapeUrls = mutation({
  args: {
    urls: v.array(v.string()),
    sourceUrl: v.string(),
    provider: v.string(),
    siteId: v.optional(v.id("sites")),
    pattern: v.optional(v.union(v.string(), v.null())),
  },
  handler: async (ctx, args) => {
    const now = Date.now();
    const queued: string[] = [];
    const seen = new Set<string>();

    for (const rawUrl of args.urls) {
      const url = (rawUrl || "").trim();
      if (!url || seen.has(url)) continue;
      seen.add(url);

      // Skip if already queued
      const existing = await ctx.db
        .query("scrape_url_queue")
        .withIndex("by_url", (q) => q.eq("url", url))
        .first();
      if (existing) {
        const createdAt = (existing as any).createdAt ?? 0;
        if (createdAt && createdAt < now - SCRAPE_URL_QUEUE_TTL_MS) {
          // Mark stale and skip requeue
          await ctx.db.patch(existing._id, {
            status: "failed",
            lastError: "stale (>48h)",
            updatedAt: now,
          });
        }
        continue;
      }

      await ctx.db.insert("scrape_url_queue", {
        url,
        sourceUrl: args.sourceUrl,
        provider: args.provider,
        siteId: args.siteId,
        pattern: args.pattern === null ? undefined : args.pattern,
        status: "pending",
        attempts: 0,
        createdAt: now,
        updatedAt: now,
      });
      queued.push(url);
    }

    return { queued };
  },
});

const leaseScrapeUrlBatchHandler = async (
  ctx: any,
  args: {
    provider?: string;
    limit?: number;
    maxPerMinuteDefault?: number;
    processingExpiryMs?: number;
  }
) => {
  const limit = Math.max(1, Math.min(args.limit ?? 50, 200));
  const now = Date.now();
  const maxPerMinuteDefault = Math.max(1, Math.min(args.maxPerMinuteDefault ?? 50, 1000));
  const processingExpiryMs = Math.max(60_000, Math.min(args.processingExpiryMs ?? 20 * 60_000, 24 * 60 * 60_000));

  const normalizeDomain = (url: string) => {
    try {
      const u = new URL(url);
      return u.hostname.toLowerCase();
    } catch {
      return "";
    }
  };

  const rateLimits = new Map<string, any>();
  const rateRows = await ctx.db.query("job_detail_rate_limits").collect();
  for (const row of rateRows as any[]) {
    const domain = (row.domain || "").toLowerCase();
    if (!domain) continue;
    rateLimits.set(domain, row);
  }

  const applyRateLimit = async (domain: string) => {
    const nowTs = Date.now();
    const existing = rateLimits.get(domain);
    const maxPerMinute = existing?.maxPerMinute ?? maxPerMinuteDefault;
    const windowStart = existing?.lastWindowStart ?? nowTs;
    const sent = existing?.sentInWindow ?? 0;
    const windowMs = 60_000;
    let newWindowStart = windowStart;
    let newSent = sent;
    if (nowTs - windowStart >= windowMs) {
      newWindowStart = nowTs;
      newSent = 0;
    }
    if (newSent >= maxPerMinute) {
      return { allowed: false, maxPerMinute };
    }
    newSent += 1;
    let upsertId = existing?._id;
    if (existing && existing._id) {
      await ctx.db.patch(existing._id, {
        lastWindowStart: newWindowStart,
        sentInWindow: newSent,
      });
    } else {
      const insertedId = await ctx.db.insert("job_detail_rate_limits", {
        domain,
        maxPerMinute,
        lastWindowStart: newWindowStart,
        sentInWindow: newSent,
      });
      upsertId = insertedId;
    }
    rateLimits.set(domain, {
      _id: upsertId,
      domain,
      maxPerMinute,
      lastWindowStart: newWindowStart,
      sentInWindow: newSent,
    });
    return { allowed: true, maxPerMinute };
  };

  // Release stale processing rows back to pending so they can be retried.
  try {
    const cutoff = now - processingExpiryMs;
    const processingRows = await ctx.db
      .query("scrape_url_queue")
      .withIndex("by_status", (q: any) => q.eq("status", "processing"))
      .take(500);
    for (const row of processingRows as any[]) {
      if ((row).updatedAt && (row).updatedAt >= cutoff) continue;
      if (args.provider && row.provider !== args.provider) continue;
      await ctx.db.patch(row._id, {
        status: "pending",
        updatedAt: now,
      });
    }
  } catch (err) {
    console.error("leaseScrapeUrlBatch: failed releasing stale processing", err);
  }

  const baseQuery = ctx.db.query("scrape_url_queue").withIndex("by_status", (q: any) => q.eq("status", "pending"));
  const rows = await baseQuery.order("asc").take(limit * 3);
  const picked: any[] = [];
  for (const row of rows as any[]) {
    if (picked.length >= limit) break;
    if (args.provider && row.provider !== args.provider) continue;
    const createdAt = (row).createdAt ?? 0;
    if (createdAt && createdAt < now - SCRAPE_URL_QUEUE_TTL_MS) {
      // Skip stale (>48h) entries; mark ignored
      await ctx.db.patch(row._id, {
        status: "failed",
        lastError: "stale (>48h)",
        updatedAt: now,
        completedAt: now,
      });
      try {
        await ctx.db.insert("ignored_jobs", {
          url: row.url,
          sourceUrl: row.sourceUrl ?? "",
          provider: row.provider,
          workflowName: "leaseScrapeUrlBatch",
          reason: "stale_scrape_queue_entry",
          details: { siteId: row.siteId, createdAt },
          createdAt: now,
        });
      } catch {
        // best-effort
      }
      continue;
    }
    const domain = normalizeDomain(row.url);
    const rate = await applyRateLimit(domain || "default");
    if (!rate.allowed) continue;
    picked.push(row);
  }

  if (picked.length === 0) return { urls: [] };

  for (const row of picked) {
    await ctx.db.patch(row._id, {
      status: "processing",
      attempts: ((row).attempts ?? 0) + 1,
      updatedAt: now,
    });
  }

  return {
    urls: picked.map((r) => ({
      url: r.url,
      sourceUrl: r.sourceUrl,
      provider: r.provider,
      siteId: r.siteId,
      pattern: r.pattern,
      _id: r._id,
    })),
  };
};

export const leaseScrapeUrlBatch = Object.assign(
  mutation({
    args: {
      provider: v.optional(v.string()),
      limit: v.optional(v.number()),
      maxPerMinuteDefault: v.optional(v.number()),
      processingExpiryMs: v.optional(v.number()),
    },
    handler: leaseScrapeUrlBatchHandler,
  }),
  { handler: leaseScrapeUrlBatchHandler }
);

export const requeueStaleScrapeUrls = mutation({
  args: {
    provider: v.optional(v.string()),
    limit: v.optional(v.number()),
    processingExpiryMs: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const now = Date.now();
    const limit = Math.max(1, Math.min(args.limit ?? 500, 2000));
    const processingExpiryMs = Math.max(
      60_000,
      Math.min(args.processingExpiryMs ?? 20 * 60_000, 24 * 60 * 60_000)
    );
    const cutoff = now - processingExpiryMs;

    const rows = await ctx.db
      .query("scrape_url_queue")
      .withIndex("by_status", (q: any) => q.eq("status", "processing"))
      .take(limit);

    let requeued = 0;
    for (const row of rows as any[]) {
      if (args.provider && row.provider !== args.provider) continue;
      if (!row.updatedAt || row.updatedAt >= cutoff) continue;
      await ctx.db.patch(row._id, {
        status: "pending",
        updatedAt: now,
      });
      requeued += 1;
    }

    return { requeued, checked: rows.length, cutoff };
  },
});

const heuristicPendingReason = "pending markdown structured extraction";

const needsHeuristicVersionUpgrade = (q: any) =>
  q.or(q.eq(q.field("heuristicVersion"), null), q.lt(q.field("heuristicVersion"), HEURISTIC_VERSION));

const heuristicAttemptGate = (q: any, retryCutoff: number) =>
  q.or(
    q.eq(q.field("heuristicAttempts"), null),
    q.lt(q.field("heuristicAttempts"), 3),
    q.lt(q.field("heuristicLastTried"), retryCutoff)
  );

const _heuristicPendingFilter = (q: any, retryCutoff: number) =>
  q.and(
    q.or(
      q.eq(q.field("compensationReason"), heuristicPendingReason),
      q.and(
        q.eq(q.field("compensationUnknown"), true),
        q.or(q.eq(q.field("totalCompensation"), 0), q.eq(q.field("totalCompensation"), null))
      )
    ),
    q.or(heuristicAttemptGate(q, retryCutoff), needsHeuristicVersionUpgrade(q))
  );

export const countPendingJobDetails = query({
  args: {},
  handler: async (_ctx) => {
    return { pending: 0 };
  },
});

export const listPendingJobDetails = query({
  args: { limit: v.optional(v.number()) },
  handler: async (_ctx, _args) => {
    return [];
  },
});

const completeScrapeUrlsHandler = async (
  ctx: any,
  args: { urls: string[]; status: "completed" | "failed"; error?: string }
) => {
  const now = Date.now();
  for (const rawUrl of args.urls) {
    const url = (rawUrl || "").trim();
    if (!url) continue;

    const existing = await ctx.db
      .query("scrape_url_queue")
      .withIndex("by_url", (q: any) => q.eq("url", url))
      .first();
    if (!existing) continue;

    const attempts = ((existing).attempts ?? 0) + 1;
    const shouldIgnore =
      args.status === "failed" &&
      (attempts >= JOB_DETAIL_MAX_ATTEMPTS || (typeof args.error === "string" && args.error.toLowerCase().includes("404")));

    if (shouldIgnore) {
      try {
        await ctx.db.insert("ignored_jobs", {
          url,
          sourceUrl: (existing).sourceUrl ?? "",
          provider: (existing).provider,
          workflowName: "leaseScrapeUrlBatch",
          reason:
            typeof args.error === "string" && args.error.toLowerCase().includes("404")
              ? "http_404"
              : "max_attempts",
          details: { attempts, siteId: (existing).siteId, lastError: args.error },
          createdAt: now,
        });
      } catch (err) {
        console.error("completeScrapeUrls: failed to insert ignored_jobs", err);
      }
      try {
        await ctx.db.delete(existing._id);
      } catch (err) {
        console.error("completeScrapeUrls: failed to delete queue row", err);
      }
      continue;
    }

    await ctx.db.patch(existing._id, {
      status: args.status,
      attempts,
      lastError: args.error,
      updatedAt: now,
      completedAt: args.status === "completed" ? now : undefined,
    });
  }
  return { updated: args.urls.length };
};

export const completeScrapeUrls = Object.assign(
  mutation({
    args: {
      urls: v.array(v.string()),
      status: v.union(v.literal("completed"), v.literal("failed")),
      error: v.optional(v.string()),
    },
    handler: completeScrapeUrlsHandler,
  }),
  { handler: completeScrapeUrlsHandler }
);

export const listJobDetailConfigs = query({
  args: { domain: v.optional(v.string()), field: v.optional(v.string()) },
  handler: async (ctx, args) => {
    const domain = (args.domain || "").toLowerCase();
    const field = (args.field || "").toLowerCase();
    let rows;
    if (domain) {
      rows = await ctx.db.query("job_detail_configs").withIndex("by_domain", (q) => q.eq("domain", domain)).take(200);
    } else {
      rows = await ctx.db.query("job_detail_configs").take(200);
    }
    if (field) {
      rows = rows.filter((row: any) => (row.field || "").toLowerCase() === field);
    }
    rows.sort((a: any, b: any) => (b.successCount ?? 0) - (a.successCount ?? 0));
    return rows.map((row: any) => ({
      _id: row._id,
      domain: row.domain,
      field: row.field,
      regex: row.regex,
      successCount: row.successCount,
      lastSuccessAt: row.lastSuccessAt,
      createdAt: row.createdAt,
    }));
  },
});

export const recordJobDetailHeuristic = mutation({
  args: {
    domain: v.string(),
    field: v.string(),
    regex: v.string(),
  },
  handler: async (ctx, args) => {
    const domain = args.domain.trim().toLowerCase();
    const field = args.field.trim().toLowerCase();
    const regex = args.regex.trim();
    if (!domain || !field || !regex) throw new Error("domain, field, and regex are required");
    const existing = await ctx.db
      .query("job_detail_configs")
      .withIndex("by_domain_field", (q) => q.eq("domain", domain).eq("field", field))
      .filter((q) => q.eq(q.field("regex"), regex))
      .first();
    const now = Date.now();
    if (existing) {
      await ctx.db.patch(existing._id, {
        successCount: (existing as any).successCount + 1,
        lastSuccessAt: now,
      });
      return { updated: true };
    }
    await ctx.db.insert("job_detail_configs", {
      domain,
      field,
      regex,
      successCount: 1,
      lastSuccessAt: now,
      createdAt: now,
    });
    return { created: true };
  },
});

export const updateJobWithHeuristicHandler = async (
  ctx: any,
  args: {
    id: Id<"jobs">;
    location?: string;
    locations?: string[];
    locationStates?: string[];
    locationSearch?: string;
    countries?: string[];
    country?: string;
    description?: string;
    totalCompensation?: number;
    compensationReason?: string;
    compensationUnknown?: boolean;
    remote?: boolean;
    heuristicAttempts?: number;
    heuristicLastTried?: number;
    heuristicVersion?: number;
    currencyCode?: string;
  }
) => {
  const patch: any = {};
  const detailPatch: any = {};
  for (const key of [
    "location",
    "locations",
    "locationStates",
    "locationSearch",
    "countries",
    "country",
    "totalCompensation",
    "compensationReason",
    "compensationUnknown",
    "remote",
    "currencyCode",
  ] as const) {
    if (args[key] !== undefined) {
      patch[key] = args[key] as any;
    }
  }
  for (const key of ["description", "heuristicAttempts", "heuristicLastTried", "heuristicVersion"] as const) {
    if (args[key] !== undefined) {
      detailPatch[key] = args[key] as any;
    }
  }
  if (Object.keys(patch).length === 0 && Object.keys(detailPatch).length === 0) return { updated: false };
  if (Object.keys(patch).length > 0) {
    await ctx.db.patch(args.id, patch);
  }
  if (Object.keys(detailPatch).length > 0) {
    const existing = await ctx.db
      .query("job_details")
      .withIndex("by_job", (q: any) => q.eq("jobId", args.id))
      .first();
    if (existing) {
      await ctx.db.patch(existing._id, detailPatch);
    } else {
      await ctx.db.insert("job_details", { jobId: args.id, ...detailPatch });
    }
  }
  return { updated: true };
};

export const updateJobWithHeuristic = Object.assign(
  mutation({
    args: {
      id: v.id("jobs"),
      location: v.optional(v.string()),
      locations: v.optional(v.array(v.string())),
      locationStates: v.optional(v.array(v.string())),
      locationSearch: v.optional(v.string()),
      countries: v.optional(v.array(v.string())),
      country: v.optional(v.string()),
      description: v.optional(v.string()),
      totalCompensation: v.optional(v.number()),
      compensationReason: v.optional(v.string()),
      compensationUnknown: v.optional(v.boolean()),
      remote: v.optional(v.boolean()),
      heuristicAttempts: v.optional(v.number()),
      heuristicLastTried: v.optional(v.number()),
      heuristicVersion: v.optional(v.number()),
      currencyCode: v.optional(v.string()),
    },
    handler: updateJobWithHeuristicHandler,
  }),
  { handler: updateJobWithHeuristicHandler }
);

export const clearStaleScrapeQueue = internalMutation({
  args: {},
  handler: async (ctx) => {
    const cutoff = Date.now() - SCRAPE_URL_QUEUE_TTL_MS;
    let removed = 0;

    // Only pending/processing entries need cleanup; keep completed for audit until other cleanup.
    const statuses: ("pending" | "processing")[] = ["pending", "processing"];
    for (const status of statuses) {
      const stale = await ctx.db
        .query("scrape_url_queue")
        .withIndex("by_status", (q) => q.eq("status", status))
        .filter((q) => q.lt(q.field("createdAt"), cutoff))
        .take(200);

      for (const row of stale) {
        await ctx.db.delete(row._id);
        removed++;
      }
    }

    return { removed };
  },
});

export const resetScrapeUrlProcessing = mutation({
  args: {
    provider: v.optional(v.string()),
    siteId: v.optional(v.id("sites")),
  },
  handler: async (ctx, args) => {
    const base = ctx.db.query("scrape_url_queue").withIndex("by_status", (q) => q.eq("status", "processing"));
    const rows = await base.take(500);
    let updated = 0;
    for (const row of rows as any[]) {
      if (args.provider && row.provider !== args.provider) continue;
      if (args.siteId && row.siteId !== args.siteId) continue;
      await ctx.db.patch(row._id, { status: "pending", updatedAt: Date.now() });
      updated += 1;
    }
    return { updated };
  },
});

// Move completed/failed job-detail URLs back to pending for reprocessing.
export const resetScrapeUrlsByStatus = mutation({
  args: {
    provider: v.optional(v.string()),
    siteId: v.optional(v.id("sites")),
    status: v.optional(v.union(v.literal("completed"), v.literal("failed"))),
    limit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const status = args.status ?? "completed";
    const limit = Math.max(1, Math.min(args.limit ?? 500, 2000));
    const rows = await ctx.db
      .query("scrape_url_queue")
      .withIndex("by_status", (q) => q.eq("status", status))
      .take(limit);

    let updated = 0;
    const now = Date.now();
    for (const row of rows as any[]) {
      if (args.provider && row.provider !== args.provider) continue;
      if (args.siteId && row.siteId !== args.siteId) continue;
      await ctx.db.patch(row._id, {
        status: "pending",
        updatedAt: now,
        completedAt: undefined,
        lastError: status === "failed" ? undefined : row.lastError,
      });
      updated += 1;
    }
    return { updated };
  },
});

export const resetTodayAndRunAllScheduled = mutation({
  args: {
    batchSize: v.optional(v.number()),
    windowStart: v.optional(v.number()),
    windowEnd: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const now = Date.now();
    const start = new Date();
    start.setHours(0, 0, 0, 0);
    const dayMs = 24 * 60 * 60 * 1000;
    const defaultStartOfDay = start.getTime();
    const startOfDay = args.windowStart ?? defaultStartOfDay;
    const endOfDay = args.windowEnd ?? startOfDay + dayMs;
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 25, 200));

    const deleteJobsScrapedToday = async () => {
      const page = await ctx.db
        .query("jobs")
        .withIndex("by_scraped_at", (q: any) => q.gte("scrapedAt", startOfDay).lt("scrapedAt", endOfDay))
        .take(batchSize);

      let deleted = 0;
      for (const job of page as any[]) {
        const detail = await ctx.db
          .query("job_details")
          .withIndex("by_job", (q: any) => q.eq("jobId", job._id))
          .first();
        if (detail) {
          await ctx.db.delete(detail._id);
        }
        await ctx.db.delete(job._id);
        deleted += 1;
      }

      return { deleted, hasMore: page.length === batchSize };
    };

    const deleteScrapesByRange = async (indexName: "by_completedAt" | "by_startedAt", field: "completedAt" | "startedAt") => {
      const page = await ctx.db
        .query("scrapes")
        .withIndex(indexName, (q: any) => q.gte(field, startOfDay).lt(field, endOfDay))
        .take(batchSize);

      let deleted = 0;
      for (const row of page as any[]) {
        await ctx.db.delete(row._id);
        deleted += 1;
      }

      return { deleted, hasMore: page.length === batchSize };
    };

    const deleteScrapesToday = async () => {
      const completed = await deleteScrapesByRange("by_completedAt", "completedAt");
      const started = await deleteScrapesByRange("by_startedAt", "startedAt");
      return {
        deleted: completed.deleted + started.deleted,
        hasMore: completed.hasMore || started.hasMore,
      };
    };

    const deleteQueuedScrapeUrls = async () => {
      const rows = await ctx.db.query("scrape_url_queue").take(batchSize);
      let deleted = 0;
      for (const row of rows as any[]) {
        await ctx.db.delete(row._id);
        deleted += 1;
      }
      return { deleted, hasMore: rows.length === batchSize };
    };

    const deleteSkippedJobsToday = async () => {
      const page = await ctx.db
        .query("ignored_jobs")
        .withIndex("by_created_at", (q: any) => q.gte("createdAt", startOfDay).lt("createdAt", endOfDay))
        .take(batchSize);

      let deleted = 0;
      for (const row of page as any[]) {
        await ctx.db.delete(row._id);
        deleted += 1;
      }

      return { deleted, hasMore: page.length === batchSize };
    };

    const triggerScheduledSites = async () => {
      const enabledSites = await ctx.db
        .query("sites")
        .withIndex("by_enabled", (q: any) => q.eq("enabled", true))
        .collect();

      let triggered = 0;
      for (const site of enabledSites as any[]) {
        if (!site.scheduleId) continue;
        const siteId = site._id as Id<"sites">;
        await ctx.db.patch(siteId, {
          completed: false,
          failed: false,
          lockedBy: "",
          lockExpiresAt: 0,
          lastRunAt: 0,
          lastFailureAt: undefined,
          lastError: undefined,
          manualTriggerAt: now,
        } as any);

        try {
          await ctx.db.insert("run_requests", {
            siteId,
            siteUrl: site.url ?? "",
            status: "pending",
            createdAt: now,
            expectedEta: now + 15_000,
            completedAt: undefined,
          });
        } catch (err) {
          console.error("resetTodayAndRunAllScheduled: failed to record run_request", err);
        }

        triggered += 1;
      }
      return triggered;
    };

    const jobsResult = await deleteJobsScrapedToday();
    const scrapesResult = await deleteScrapesToday();
    const queueResult = await deleteQueuedScrapeUrls();
    const skippedResult = await deleteSkippedJobsToday();
    const hasMore = jobsResult.hasMore || scrapesResult.hasMore || queueResult.hasMore || skippedResult.hasMore;
    const sitesTriggered = hasMore ? 0 : await triggerScheduledSites();

    return {
      jobsDeleted: jobsResult.deleted,
      scrapesDeleted: scrapesResult.deleted,
      queueDeleted: queueResult.deleted,
      skippedDeleted: skippedResult.deleted,
      sitesTriggered,
      hasMore,
      batchSize,
      windowStart: startOfDay,
      windowEnd: endOfDay,
    };
  },
});

export const listJobDetailRateLimits = query({
  args: {},
  handler: async (ctx) => {
    const rows = await ctx.db.query("job_detail_rate_limits").order("asc").take(200);
    return rows.map((row: any) => ({
      _id: row._id,
      domain: row.domain,
      maxPerMinute: row.maxPerMinute,
      lastWindowStart: row.lastWindowStart,
      sentInWindow: row.sentInWindow,
    }));
  },
});

export const upsertJobDetailRateLimit = mutation({
  args: {
    domain: v.string(),
    maxPerMinute: v.number(),
  },
  handler: async (ctx, args) => {
    const domain = args.domain.trim().toLowerCase();
    if (!domain) throw new Error("domain is required");
    const existing = await ctx.db.query("job_detail_rate_limits").withIndex("by_domain", (q) => q.eq("domain", domain)).first();
    const now = Date.now();
    if (existing) {
      await ctx.db.patch(existing._id, {
        maxPerMinute: args.maxPerMinute,
        lastWindowStart: existing.lastWindowStart ?? now,
        sentInWindow: existing.sentInWindow ?? 0,
      });
      return { updated: true };
    }
    await ctx.db.insert("job_detail_rate_limits", {
      domain,
      maxPerMinute: args.maxPerMinute,
      lastWindowStart: now,
      sentInWindow: 0,
    });
    return { created: true };
  },
});

export const deleteJobDetailRateLimit = mutation({
  args: { id: v.id("job_detail_rate_limits") },
  handler: async (ctx, args) => {
    await ctx.db.delete(args.id);
    return { deleted: true };
  },
});

// Record a failure and release the lock so it can be retried later
export const failSite = mutation({
  args: {
    id: v.id("sites"),
    error: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    const cur = await ctx.db.get(args.id);
    const count = (cur as any)?.failCount ?? 0;
    const now = Date.now();
    await ctx.db.patch(args.id, {
      failCount: count + 1,
      lastFailureAt: now,
      // Track a "last run" timestamp even on failure so dashboards show recent attempts
      lastRunAt: now,
      lastError: args.error,
      failed: true,
      lockedBy: "",
      lockExpiresAt: 0,
      // Consume any manual trigger so it doesn't repeatedly lease failures.
      manualTriggerAt: 0,
    });
    return { success: true };
  },
});

export const listRunRequests = query({
  args: { limit: v.optional(v.number()) },
  handler: async (ctx, args) => {
    const lim = Math.max(1, Math.min(args.limit ?? 50, 200));
    return await ctx.db
      .query("run_requests")
      .withIndex("by_created", (q) => q.gte("createdAt", 0))
      .order("desc")
      .take(lim);
  },
});

export const resetActiveSites = mutation({
  args: {},
  handler: async (ctx) => {
    const sites = await ctx.db
      .query("sites")
      .withIndex("by_enabled", (q) => q.eq("enabled", true))
      .collect();

    for (const site of sites as any[]) {
      await ctx.db.patch(site._id, {
        completed: false,
        failed: false,
        lockedBy: "",
        lockExpiresAt: 0,
      });
    }

    return { reset: sites.length };
  },
});

// HTTP endpoint to lease next site
http.route({
  path: "/api/sites/lease",
  method: "POST",
  handler: httpAction(async (ctx, request) => {
    const body = await request.json();
    const site = await ctx.runMutation(api.router.leaseSite, {
      workerId: body.workerId,
      lockSeconds: body.lockSeconds ?? 300,
      siteType: body.siteType ?? undefined,
    });
    return new Response(JSON.stringify(site), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }),
});

http.route({
  path: "/api/sites/reset",
  method: "POST",
  handler: httpAction(async (ctx) => {
    const res = await ctx.runMutation(api.router.resetActiveSites, {});
    return new Response(JSON.stringify(res), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }),
});

// HTTP endpoint to mark site completed
http.route({
  path: "/api/sites/complete",
  method: "POST",
  handler: httpAction(async (ctx, request) => {
    const body = await request.json();
    const res = await ctx.runMutation(api.router.completeSite, { id: body.id });
    return new Response(JSON.stringify(res), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }),
});

// HTTP endpoint to release a lock (optional)
http.route({
  path: "/api/sites/release",
  method: "POST",
  handler: httpAction(async (ctx, request) => {
    const body = await request.json();
    const res = await ctx.runMutation(api.router.releaseSite, { id: body.id });
    return new Response(JSON.stringify(res), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }),
});

// HTTP endpoint to mark a site as failed and release
http.route({
  path: "/api/sites/fail",
  method: "POST",
  handler: httpAction(async (ctx, request) => {
    const body = await request.json();
    const res = await ctx.runMutation(api.router.failSite, { id: body.id, error: body.error });
    return new Response(JSON.stringify(res), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }),
});

export const upsertSite = mutation({
  args: {
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
    pattern: v.optional(v.string()),
    scheduleId: v.optional(v.id("scrape_schedules")),
    enabled: v.boolean(),
  },
  handler: async (ctx, args) => {
    // For simplicity, just insert a new record
    const siteType = args.type ?? "general";
    const scrapeProvider =
      args.scrapeProvider ??
      (siteType === "greenhouse" || siteType === "avature" ? "spidercloud" : "fetchfox");
    const normalizedUrl = normalizeSiteUrl(args.url, siteType);
    const resolvedName = fallbackCompanyName(args.name, normalizedUrl);
    const key = siteCanonicalKey(normalizedUrl, siteType);

    const sites = await ctx.db.query("sites").collect();
    const existing = (sites as any[]).find(
      (s: any) => siteCanonicalKey(s.url, (s).type) === key
    );

    const payload = {
      name: args.name ?? resolvedName,
      url: normalizedUrl,
      type: siteType,
      scrapeProvider,
      pattern: args.pattern,
      scheduleId: args.scheduleId,
      enabled: args.enabled,
    };

    if (existing) {
      await ctx.db.patch(existing._id, payload);
      await upsertCompanyProfile(ctx, resolvedName, normalizedUrl, args.name);
      return existing._id;
    }

    const id = await ctx.db.insert("sites", {
      ...payload,
      // New sites should be leased immediately; keep lastRunAt at 0
      lastRunAt: 0,
    });

    await upsertCompanyProfile(ctx, resolvedName, args.url, args.name);
    return id;
  },
});

export const updateSiteEnabled = mutation({
  args: {
    id: v.id("sites"),
    enabled: v.boolean(),
  },
  handler: async (ctx, args) => {
    await ctx.db.patch(args.id, { enabled: args.enabled });
    return args.id;
  },
});

export const deleteSite = mutation({
  args: {
    id: v.id("sites"),
  },
  handler: async (ctx, args) => {
    const site = await ctx.db.get(args.id);
    if (!site) {
      throw new Error("Site not found");
    }

    const queuedUrls = await ctx.db
      .query("scrape_url_queue")
      .withIndex("by_site_status", (q) => q.eq("siteId", args.id))
      .collect();

    for (const row of queuedUrls as any[]) {
      await ctx.db.delete(row._id);
    }

    await ctx.db.delete(args.id);
    return { id: args.id, queuedDeleted: queuedUrls.length };
  },
});

const updateSiteNameHandler = async (ctx: any, args: { id: Id<"sites">; name: string }) => {
  const name = (args.name || "").trim();
  if (!name) {
    throw new Error("Name is required");
  }
  const site = await ctx.db.get(args.id);
  if (!site) {
    throw new Error("Site not found");
  }
  await ctx.db.patch(args.id, { name });
  await upsertCompanyProfile(ctx, name, (site).url, (site).name ?? undefined);

  // Retag jobs even if the visible name was already the desired value by
  // trying common legacy variants derived from the site URL.
  const prevName = (site).name ?? "";
  const urlDerived = fallbackCompanyName(undefined, (site).url);
  const prevVariants = Array.from(
    new Set(
      [prevName, urlDerived, fallbackCompanyName(prevName, (site).url)]
        .filter((val): val is string => typeof val === "string" && val.trim().length > 0)
    )
  );

  let updatedJobs = 0;
  try {
    for (const prev of prevVariants) {
      if (prev === name) continue;
      updatedJobs += await updateJobsCompany(ctx, prev, name);
    }
    const domain = normalizeDomainInput((site).url);
    if (domain) {
      updatedJobs += await updateJobsCompanyByDomain(ctx, domain, name);
    }
  } catch (err) {
    console.error("updateSiteName: failed retagging jobs", err);
    // Continue returning success so the admin UI doesn't block; jobs can be retagged manually later.
  }

  return { id: args.id, updatedJobs };
};

export const updateSiteName = mutation({
  args: {
    id: v.id("sites"),
    name: v.string(),
  },
  handler: updateSiteNameHandler,
});
(updateSiteName as any).handler = updateSiteNameHandler;

export const bulkUpsertSites = mutation({
  args: {
    sites: v.array(
      v.object({
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
        pattern: v.optional(v.string()),
        scheduleId: v.optional(v.id("scrape_schedules")),
        enabled: v.boolean(),
      })
    ),
  },
  handler: async (ctx, args) => {
    const ids = [];
    const existingSites = await ctx.db.query("sites").collect();
    for (const site of args.sites) {
      const siteType = site.type ?? "general";
      const scrapeProvider =
        site.scrapeProvider ??
        (siteType === "greenhouse" || siteType === "avature" ? "spidercloud" : "fetchfox");
      const normalizedUrl = normalizeSiteUrl(site.url, siteType);
      const resolvedName = fallbackCompanyName(site.name, normalizedUrl);
      const key = siteCanonicalKey(normalizedUrl, siteType);
      const existing = (existingSites as any[]).find(
        (s: any) => siteCanonicalKey(s.url, (s).type) === key
      );

      const payload = {
        ...site,
        name: site.name ?? resolvedName,
        url: normalizedUrl,
        type: siteType,
        scrapeProvider,
      };

      if (existing) {
        await ctx.db.patch(existing._id, payload);
        await upsertCompanyProfile(ctx, resolvedName, normalizedUrl, site.name ?? undefined);
        ids.push(existing._id);
        continue;
      }

      const id = await ctx.db.insert("sites", {
        ...payload,
        // Same behavior as single add: make new sites immediately leaseable
        lastRunAt: 0,
      });
      await upsertCompanyProfile(ctx, resolvedName, normalizedUrl, site.name ?? undefined);
      ids.push(id);
    }
    return ids;
  },
});

export const listDomainAliases = query({
  args: {},
  returns: v.array(
    v.object({
      domain: v.string(),
      derivedName: v.string(),
      alias: v.optional(v.string()),
      siteName: v.optional(v.string()),
      siteUrl: v.optional(v.string()),
      updatedAt: v.optional(v.number()),
    })
  ),
  handler: async (ctx) => {
    const sites = await ctx.db.query("sites").collect();
    const aliases = await ctx.db.query("domain_aliases").collect();
    const byDomain = new Map<
      string,
      {
        domain: string;
        derivedName: string;
        alias?: string;
        siteName?: string;
        siteUrl?: string;
        updatedAt?: number;
      }
    >();

    for (const row of aliases) {
      const domain = (row as any).domain ?? "";
      if (!domain) continue;
      byDomain.set(domain, {
        domain,
        derivedName: (row as any).derivedName ?? deriveNameFromDomain(domain),
        alias: (row as any).alias ?? undefined,
        updatedAt: (row as any).updatedAt ?? (row as any).createdAt,
      });
    }

    for (const site of sites) {
      const domain = normalizeDomainInput((site as any).url);
      if (!domain) continue;
      const existing = byDomain.get(domain);
      const derivedName = fallbackCompanyName(undefined, (site as any).url);
      if (existing) {
        existing.siteName = existing.siteName ?? (site as any).name;
        existing.siteUrl = existing.siteUrl ?? (site as any).url;
        if (!existing.derivedName && derivedName) {
          existing.derivedName = derivedName;
        }
        continue;
      }
      byDomain.set(domain, {
        domain,
        derivedName,
        alias: undefined,
        siteName: (site as any).name,
        siteUrl: (site as any).url,
        updatedAt: undefined,
      });
    }

    return Array.from(byDomain.values()).sort((a, b) => a.domain.localeCompare(b.domain));
  },
});

export const setDomainAlias = mutation({
  args: {
    domainOrUrl: v.string(),
    alias: v.string(),
  },
  returns: v.object({
    domain: v.string(),
    alias: v.string(),
    derivedName: v.string(),
    updatedJobs: v.number(),
    updatedSites: v.number(),
  }),
  handler: async (ctx, args) => {
    const domain = normalizeDomainInput(args.domainOrUrl);
    const alias = (args.alias || "").trim();
    if (!domain) {
      throw new Error("Domain is required");
    }
    if (!alias) {
      throw new Error("Alias is required");
    }

    const sites = await ctx.db.query("sites").collect();
    const matchingSites = sites.filter((site: any) => normalizeDomainInput(site.url) === domain);
    const sampleSite = matchingSites[0];
    const derivedName = fallbackCompanyName(undefined, sampleSite?.url ?? `https://${domain}`);
    const now = Date.now();

    const existing = await ctx.db
      .query("domain_aliases")
      .withIndex("by_domain", (q) => q.eq("domain", domain))
      .first();
    const previousAlias = existing?.alias;

    if (existing) {
      await ctx.db.patch(existing._id, {
        alias,
        derivedName,
        updatedAt: now,
      });
    } else {
      await ctx.db.insert("domain_aliases", {
        domain,
        alias,
        derivedName,
        createdAt: now,
        updatedAt: now,
      });
    }

    await upsertCompanyProfile(ctx, alias, sampleSite?.url ?? `https://${domain}`, derivedName);

    let updatedJobs = 0;
    const previousNames = new Set<string>();
    if (derivedName) previousNames.add(derivedName);
    if (previousAlias) previousNames.add(previousAlias);
    matchingSites.forEach((site: any) => {
      if (site?.name) previousNames.add(site.name);
    });

    for (const prev of Array.from(previousNames)) {
      if (prev && prev !== alias) {
        updatedJobs += await updateJobsCompany(ctx, prev, alias);
      }
    }

    // Also retag any jobs from this domain, even if their scraped company
    // doesn't match the derived/previous names (fixes hostname-like company values).
    updatedJobs += await updateJobsCompanyByDomain(ctx, domain, alias);

    let updatedSites = 0;
    for (const site of matchingSites) {
      if ((site as any).name !== alias) {
        await ctx.db.patch(site._id, { name: alias });
        updatedSites += 1;
      }
    }

    return { domain, alias, derivedName, updatedJobs, updatedSites };
  },
});

export const renameCompany = mutation({
  args: {
    oldName: v.string(),
    newName: v.string(),
  },
  returns: v.object({ updatedJobs: v.number() }),
  handler: async (ctx, args) => {
    const oldName = (args.oldName || "").trim();
    const newName = (args.newName || "").trim();
    if (!oldName) {
      throw new Error("Old company name is required");
    }
    if (!newName) {
      throw new Error("New company name is required");
    }

    const updatedJobs = await updateJobsCompany(ctx, oldName, newName);
    await upsertCompanyProfile(ctx, newName, null, oldName);

    return { updatedJobs };
  },
});

// Test helper: insert a dummy scrape row
export const insertDummyScrape = mutation({
  args: {},
  handler: async (ctx) => {
    const now = Date.now();
    return await ctx.db.insert("scrapes", {
      sourceUrl: "https://example.com/jobs",
      pattern: "https://example.com/jobs/**",
      startedAt: now,
      completedAt: now,
      items: { results: { hits: ["https://example.com/jobs"], items: [{ job_title: "N/A" }] } },
    });
  },
});

export const insertJobRecord = mutation({
  args: {
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
    url: v.string(),
    test: v.optional(v.boolean()),
  },
  handler: async (ctx, args) => {
    const resolvedCompany = await resolveCompanyForUrl(ctx, args.url, args.company);
    const { description, ...jobArgs } = args;
    const jobId = await ctx.db.insert(
      "jobs",
      buildJobInsert({
        ...jobArgs,
        company: resolvedCompany,
        compensationUnknown: args.compensationUnknown ?? false,
        compensationReason: args.compensationReason,
        postedAt: Date.now(),
      })
    );
    await ctx.db.insert("job_details", {
      jobId,
      description,
    });
    return jobId;
  },
});

export const findExistingJobUrls = query({
  args: {
    urls: v.array(v.string()),
  },
  returns: v.object({ existing: v.array(v.string()) }),
  handler: async (ctx, args) => {
    const existing: string[] = [];
    const unique = Array.from(new Set(args.urls));

    for (const url of unique) {
      const dup = await ctx.db
        .query("jobs")
        .withIndex("by_url", (q) => q.eq("url", url))
        .first();
      if (dup) existing.push(url);
    }

    return { existing };
  },
});

const wildcardToRegex = (pattern: string) => {
  const escaped = pattern.replace(/[-/\\^$+?.()|[\]{}]/g, "\\$&");
  const withWildcards = escaped.replace(/\\\*\\\*/g, ".*").replace(/\\\*/g, "[^/]*");
  return new RegExp(`^${withWildcards}$`);
};

const buildUrlMatcher = (patternOrPrefix: string) => {
  const value = (patternOrPrefix ?? "").trim();
  if (!value) return (_url: string) => false;

  if (value.includes("*")) {
    try {
      const regex = wildcardToRegex(value);
      return (url: string) => regex.test(url);
    } catch {
      return (url: string) => url.startsWith(value.replace(/\*/g, ""));
    }
  }

  return (url: string) => url.startsWith(value);
};

/**
 * API endpoint for storing raw scrape results
 *
 * POST /api/scrapes
 * Content-Type: application/json
 * Body: { sourceUrl: string, pattern?: string, items: any, startedAt?: number, completedAt?: number }
 */
http.route({
  path: "/api/scrapes",
  method: "POST",
  handler: httpAction(async (ctx, request) => {
    try {
      const body = await request.json();
      const now = Date.now();
      const scrapeId = await ctx.runMutation(api.router.insertScrapeRecord, {
        sourceUrl: body.sourceUrl,
        pattern: body.pattern ?? undefined,
        startedAt: body.startedAt ?? now,
        completedAt: body.completedAt ?? now,
        items: body.items,
        provider: body.provider ?? body.items?.provider,
        workflowName: body.workflowName,
        costMilliCents: body.costMilliCents ?? (typeof body.costCents === "number" ? Math.round(body.costCents * 1000) : undefined),
        request: body.request ?? body.requestData ?? body.items?.request,
      });

      // Opportunistically ingest jobs into jobs table for UI
      try {
        const jobs = extractJobs(body.items, {
          seedListingLogContext: {
            sourceUrl: body.sourceUrl,
            provider: body.provider ?? body.items?.provider,
            workflowName: body.workflowName,
          },
        });
        if (jobs.length > 0) {
          await ctx.runMutation(api.router.ingestJobsFromScrape, {
            jobs: jobs.map((j) => ({
              ...j,
              postedAt: j.postedAt ?? now,
              scrapedAt: body.completedAt ?? now,
              scrapedWith: body.provider ?? body.items?.provider,
              workflowName: body.workflowName,
              scrapedCostMilliCents:
                typeof body.costMilliCents === "number"
                  ? Math.floor(body.costMilliCents / Math.max(jobs.length, 1))
                  : undefined,
            })),
          });
        }
      } catch (err: any) {
        console.error("Failed to ingest jobs from scrape", err?.message ?? err);
      }

      return new Response(
        JSON.stringify({ success: true, scrapeId }),
        { status: 201, headers: { "Content-Type": "application/json" } }
      );
    } catch {
      return new Response(
        JSON.stringify({ error: "Invalid JSON body" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }
  }),
});

/**
 * API endpoint to store a user's resume
 *
 * POST /api/resume
 * Body: resume object
 */
http.route({
  path: "/api/resume",
  method: "POST",
  handler: httpAction(async (ctx, request) => {
    const resume = await request.json();
    await ctx.runMutation(api.formFiller.storeResume, { resume });
    return new Response(JSON.stringify({ success: true }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }),
});

/**
 * API endpoint to queue a job application for form filling
 *
 * POST /api/form-fill/queue
 * Body: { jobUrl: string }
 */
http.route({
  path: "/api/form-fill/queue",
  method: "POST",
  handler: httpAction(async (ctx, request) => {
    const body = await request.json();
    await ctx.runMutation(api.formFiller.queueApplication, { jobUrl: body.jobUrl });
    return new Response(JSON.stringify({ success: true }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }),
});

/**
 * API endpoint to fetch the next queued job application
 *
 * GET /api/form-fill/next
 */
http.route({
  path: "/api/form-fill/next",
  method: "GET",
  handler: httpAction(async (ctx) => {
    const next = await ctx.runQuery(api.formFiller.nextApplication, {});
    return new Response(JSON.stringify(next), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }),
});

// Firecrawl webhook receiver (uses .convex.site domain)
http.route({
  path: "/api/firecrawl/webhook",
  method: "OPTIONS",
  handler: httpAction(async (_ctx, request) => {
    const { preflight, headers, originAllowed } = await runFirecrawlCors(request);
    if (!originAllowed) return new Response(null, { status: 403 });
    if (preflight) return preflight;
    return new Response(null, { status: 204, headers });
  }),
});

http.route({
  path: "/api/firecrawl/webhook",
  method: "POST",
  handler: httpAction(async (ctx, request) => {
    const { headers: corsHeaders, originAllowed } = await runFirecrawlCors(request);
    const withCors = (headers?: HeadersInit) => ({ ...corsHeaders, ...headers });

    const origin = request.headers.get("Origin");
    if (origin && !originAllowed) {
      return new Response(JSON.stringify({ error: "Origin not allowed" }), {
        status: 403,
        headers: withCors({ "Content-Type": "application/json" }),
      });
    }

    const parsed = await parseFirecrawlWebhook(request);
    if (!parsed.ok) {
      return new Response(
        JSON.stringify({ error: parsed.error, detail: parsed.detail }),
        { status: parsed.status, headers: withCors({ "Content-Type": "application/json" }) }
      );
    }

    const body = parsed.body;

    const now = Date.now();
    const event = typeof body?.type === "string" ? body.type : typeof body?.event === "string" ? body.event : "unknown";
    const jobId =
      typeof body?.id === "string"
        ? body.id
        : typeof body?.jobId === "string"
          ? body.jobId
          : typeof body?.crawl_id === "string"
            ? body.crawl_id
            : typeof body?.batchId === "string"
              ? body.batchId
              : "unknown";
    const status = typeof body?.status === "string" ? body.status : undefined;
    const success = typeof body?.success === "boolean" ? body.success : undefined;
    const statusUrl =
      typeof body?.status_url === "string"
        ? body.status_url
        : typeof body?.statusUrl === "string"
          ? body.statusUrl
          : undefined;

    const metadataCandidate = body?.metadata;
    const metadata =
      metadataCandidate && typeof metadataCandidate === "object" && !Array.isArray(metadataCandidate)
        ? (metadataCandidate)
        : {};
    const dataArray = Array.isArray(body?.data) ? (body.data) : [];
    const firstData = dataArray.find((item) => item && typeof item === "object");
    const dataMetadata =
      firstData && typeof firstData.metadata === "object" && !Array.isArray(firstData.metadata)
        ? (firstData.metadata as Record<string, any>)
        : undefined;

    const combinedMetadata = { ...(dataMetadata ?? {}), ...metadata };

    const sourceUrl =
      typeof combinedMetadata?.url === "string"
        ? combinedMetadata.url
        : typeof combinedMetadata?.sourceUrl === "string"
          ? combinedMetadata.sourceUrl
          : typeof combinedMetadata?.sourceURL === "string"
            ? combinedMetadata.sourceURL
            : typeof body?.url === "string"
              ? body.url
              : typeof firstData?.url === "string"
                ? firstData.url
                : undefined;
    const siteId =
      typeof combinedMetadata?.siteId === "string"
        ? combinedMetadata.siteId
        : typeof dataMetadata?.siteId === "string"
          ? dataMetadata.siteId
          : undefined;

    await ctx.runMutation(api.router.insertFirecrawlWebhookEvent, {
      jobId,
      event,
      status,
      success,
      sourceUrl,
      siteId,
      statusUrl,
      metadata: combinedMetadata,
      payload: body,
      receivedAt: now,
    });

    return new Response(JSON.stringify({ success: true }), {
      status: 200,
      headers: withCors({ "Content-Type": "application/json" }),
    });
  }),
});

export const insertFirecrawlWebhookEvent = mutation({
  args: {
    jobId: v.string(),
    event: v.string(),
    status: v.optional(v.string()),
    success: v.optional(v.boolean()),
    sourceUrl: v.optional(v.string()),
    siteId: v.optional(v.string()),
    statusUrl: v.optional(v.string()),
    metadata: v.optional(v.any()),
    payload: v.any(),
    receivedAt: v.number(),
  },
  handler: async (ctx, args) => {
    const jobRows = await ctx.db
      .query("firecrawl_webhooks")
      .withIndex("by_job", (q) => q.eq("jobId", args.jobId))
      .collect();

    const existing = jobRows.find((row: any) => row.event === args.event);
    const pending = jobRows.find((row: any) => row.event === "pending");
    const processedRow = jobRows.find((row: any) => row.processed === true);

    const base = {
      jobId: args.jobId,
      event: args.event,
      status: args.status,
      success: args.success,
      sourceUrl: args.sourceUrl,
      siteId: args.siteId,
      statusUrl: args.statusUrl,
      metadata: args.metadata,
      payload: args.payload,
      receivedAt: args.receivedAt,
    };

    const markProcessed = args.event !== "pending" && Boolean(processedRow);

    if (pending && args.event !== "pending" && !pending.processed) {
      await ctx.db.patch(pending._id as Id<"firecrawl_webhooks">, {
        processed: true,
        processedAt: Date.now(),
        error: args.event,
      });
    }

    if (existing) {
      await ctx.db.patch(existing._id as Id<"firecrawl_webhooks">, {
        ...base,
        processed: markProcessed ? true : existing.processed ?? false,
        processedAt: markProcessed ? Date.now() : existing.processedAt,
        error: existing.error,
      });
      return existing._id;
    }

    return await ctx.db.insert("firecrawl_webhooks", {
      ...base,
      processed: markProcessed,
      processedAt: markProcessed ? Date.now() : undefined,
      error: markProcessed ? "already_processed" : undefined,
    });
  },
});

export const markFirecrawlWebhookProcessed = mutation({
  args: {
    id: v.id("firecrawl_webhooks"),
    error: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    await ctx.db.patch(args.id, {
      processed: true,
      processedAt: Date.now(),
      error: args.error,
    });
    return { success: true };
  },
});

export const getFirecrawlWebhookStatus = query({
  args: {
    jobId: v.string(),
  },
  handler: async (ctx, args) => {
    const rows = await ctx.db
      .query("firecrawl_webhooks")
      .withIndex("by_job", (q) => q.eq("jobId", args.jobId))
      .collect();

    const pending = rows.find((row: any) => row.event === "pending");
    const realEvents = rows.filter((row: any) => row.event !== "pending");
    const processed =
      realEvents.find((row: any) => row.processed) ?? (pending?.processed ? pending : undefined);
    const unprocessed = realEvents.find((row: any) => !row.processed);

    return {
      hasProcessed: Boolean(processed),
      hasRealEvent: Boolean(processed || unprocessed),
      pendingProcessed: pending ? Boolean((pending as any).processed) : false,
      pendingId: pending?._id,
    };
  },
});

export const insertScrapeError = mutation({
  args: {
    jobId: v.optional(v.string()),
    sourceUrl: v.optional(v.string()),
    siteId: v.optional(v.string()),
    event: v.optional(v.string()),
    status: v.optional(v.string()),
    error: v.string(),
    metadata: v.optional(v.any()),
    payload: v.optional(v.any()),
    createdAt: v.number(),
  },
  handler: async (ctx, args) => {
    return await ctx.db.insert("scrape_errors", args);
  },
});

export const listScrapeErrors = query({
  args: {
    limit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const lim = Math.max(1, Math.min(args.limit ?? 50, 200));
    const rows = await ctx.db
      .query("scrape_errors")
      .withIndex("by_created", (q) => q.gte("createdAt", 0))
      .order("desc")
      .take(lim);
    return rows;
  },
});

export const listPendingFirecrawlWebhooks = query({
  args: {
    limit: v.optional(v.number()),
    event: v.optional(v.string()),
    receivedBefore: v.optional(v.number()),
    excludePending: v.optional(v.boolean()),
  },
  handler: async (ctx, args) => {
    const lim = Math.max(1, Math.min(args.limit ?? 25, 200));
    let q = ctx.db
      .query("firecrawl_webhooks")
      .withIndex("by_processed", (idx) => idx.eq("processed", false));
    if (args.event) {
      q = q.filter((f) => f.eq(f.field("event"), args.event));
    }
    if (args.excludePending) {
      q = q.filter((f) => f.neq(f.field("event"), "pending"));
    }
    if (args.receivedBefore !== undefined) {
      q = q.filter((f) => f.lte(f.field("receivedAt"), args.receivedBefore as number));
    }
    return await q.take(lim);
  },
});

const normalizeUrlKey = (url: any) => {
  if (typeof url !== "string") return "";
  const trimmed = url.trim();
  if (!trimmed) return "";
  return trimmed.replace(/\/+$/, "");
};

const clampRequestSnapshot = (value: any) => {
  if (!value || typeof value !== "object") return value;
  if (!("body" in value) && !("headers" in value) && !("method" in value) && !("url" in value)) {
    return value;
  }
  const snapshot: Record<string, any> = {};
  if ((value).method) snapshot.method = (value).method;
  if ((value).url) snapshot.url = (value).url;
  if ((value).headers) snapshot.headers = (value).headers;
  if ("body" in value) {
    try {
      const bodyStr = JSON.stringify((value).body);
      snapshot.body =
        bodyStr.length > 900 ? `${bodyStr.slice(0, 900)}… (+${bodyStr.length - 900} chars)` : (value).body;
    } catch {
      snapshot.body = (value).body;
    }
  }
  return snapshot;
};

const sanitizeForLog = (value: any) => {
  if (value === null || value === undefined) return undefined;
  value = clampRequestSnapshot(value);
  try {
    const serialized = JSON.stringify(value);
    if (serialized.length <= 1200) return value;
    return `${serialized.slice(0, 1200)}… (+${serialized.length - 1200} chars)`;
  } catch {
    const str = String(value);
    return str.length > 1200 ? `${str.slice(0, 1200)}… (+${str.length - 1200} chars)` : str;
  }
};

const urlFromJob = (job: any) => {
  if (!job || typeof job !== "object") return null;
  const candidates = [
    (job).url,
    (job).job_url,
    (job).jobUrl,
    (job)._url,
    (job).link,
    (job).href,
    (job)._rawUrl,
  ];
  const url = candidates.find((u) => typeof u === "string" && u.trim());
  return url ? String(url) : null;
};

const normalizedFromItems = (items: any): any[] => {
  if (!items) return [];
  if (Array.isArray(items.normalized)) return items.normalized;
  if (Array.isArray(items.results?.items)) return items.results.items;
  if (Array.isArray(items.items)) return items.items;
  return [];
};

const rawJobUrlsFromItems = (items: any): string[] => {
  if (!items || typeof items !== "object") return [];
  return Array.isArray(items.raw?.job_urls)
    ? (items.raw.job_urls as any[]).filter((u) => typeof u === "string" && u.trim())
    : [];
};

const batchIdFromScrape = (scrape: any): string | undefined => {
  const candidates = [
    scrape?.batchId,
    scrape?.items?.batchId,
    scrape?.items?.jobId,
    scrape?.items?.request?.batchId,
    scrape?.items?.request?.jobId,
    scrape?.items?.request?.id,
    scrape?.items?.request?.idempotencyKey,
    scrape?.items?.raw?.batchId,
    scrape?.items?.raw?.batch_id,
    scrape?.items?.raw?.jobId,
    scrape?.items?.raw?.job_id,
    scrape?.items?.raw?.id,
    scrape?.response?.batchId,
    scrape?.response?.jobId,
    scrape?.asyncResponse?.batchId,
    scrape?.asyncResponse?.jobId,
  ];
  const found = candidates.find((id) => typeof id === "string" && id.trim());
  return found ? String(found).trim() : undefined;
};

const firstDefined = (...values: any[]) => values.find((value) => value !== undefined && value !== null);

const stripUndefined = (value: Record<string, any>) => {
  const next: Record<string, any> = {};
  for (const [key, val] of Object.entries(value)) {
    if (val === undefined || val === null) continue;
    if (Array.isArray(val) && val.length === 0) continue;
    next[key] = val;
  }
  return next;
};

const buildUrlLogEntriesForScrape = (
  scrape: any,
  {
    existingUrls,
    jobByUrl,
  }: {
    existingUrls: Set<string>;
    jobByUrl: Map<string, any>;
  }
) => {
  const logs: any[] = [];
  const provider = scrape.provider ?? scrape.items?.provider ?? "unknown";
  const workflow = scrape.workflowName ?? scrape.workflowType;
  const workflowId = firstDefined(
    scrape.workflowId,
    scrape.items?.workflowId,
    scrape.items?.raw?.workflowId,
    scrape.items?.request?.workflowId,
    scrape.items?.request?.workflow_id,
    scrape.items?.raw?.workflow_id
  ) as string | undefined;
  const batchId = batchIdFromScrape(scrape);
  const timestamp = scrape.completedAt ?? scrape.startedAt ?? scrape._creationTime ?? Date.now();
  const normalized = normalizedFromItems(scrape.items);
  const rawJobUrls = rawJobUrlsFromItems(scrape.items);
  const normalizedCount = normalized.length;
  const rawUrlCount = rawJobUrls.length;
  const seedUrls = Array.isArray(scrape.items?.seedUrls)
    ? (scrape.items.seedUrls as any[]).filter((u) => typeof u === "string" && u.trim())
    : [];

  const requestSnapshot = firstDefined(
    scrape.request,
    scrape.items?.request,
    scrape.items?.requestData,
    scrape.items?.raw?.request,
    scrape.items?.raw?.request_data,
    scrape.items?.raw?.requestData,
    scrape.items?.raw?.requestBody,
    scrape.items?.raw?.input,
    scrape.items?.raw?.payload?.request,
    scrape.items?.raw?.payload?.request_data
  );

  const providerRequest = firstDefined(
    (scrape).providerRequest,
    scrape.items?.providerRequest,
    scrape.items?.raw?.providerRequest,
    scrape.items?.raw?.provider_request
  );

  const baseRequest: Record<string, any> = {};
  if (scrape.sourceUrl) baseRequest.sourceUrl = scrape.sourceUrl;
  if (scrape.pattern) baseRequest.pattern = scrape.pattern;
  if (seedUrls.length > 0) baseRequest.seedUrls = seedUrls;
  const requestId = scrape.items?.raw?.jobId ?? scrape.items?.jobId ?? scrape.jobId;
  if (requestId) baseRequest.jobId = requestId;
  if (workflow) baseRequest.workflow = workflow;
  const statusValue = firstDefined(scrape.items?.status, scrape.status);
  const statusUrl = firstDefined(scrape.items?.statusUrl, scrape.statusUrl, scrape.items?.raw?.statusUrl);
  const webhookId = firstDefined(scrape.items?.webhookId, scrape.webhookId);
  const requestedFormat = firstDefined(scrape.requestedFormat, scrape.items?.requestedFormat);
  const asyncState = firstDefined(scrape.asyncState, scrape.items?.asyncState);

  const requestPayload = stripUndefined({
    ...baseRequest,
    provider,
    workflowId,
    batchId,
    status: statusValue,
    statusUrl,
    webhookId,
    asyncState,
    requestedFormat,
    request: requestSnapshot ?? undefined,
    providerRequest: providerRequest ?? undefined,
  });

  const sanitizedRequest = sanitizeForLog(Object.keys(requestPayload).length > 0 ? requestPayload : requestSnapshot ?? baseRequest);

  const responseCandidate = firstDefined(
    scrape.response,
    scrape.items?.response,
    scrape.items?.raw?.response,
    scrape.items?.raw?.result,
    scrape.items?.raw,
    scrape.items?.rawPreview
  );

  const responseFallback =
    responseCandidate === undefined && normalizedCount > 0 ? { normalizedCount } : responseCandidate;
  const response = sanitizeForLog(responseFallback);

  const asyncCandidate = firstDefined(
    scrape.asyncResponse,
    scrape.items?.asyncResponse,
    scrape.items?.raw?.asyncResponse,
    scrape.items?.raw?.payload?.asyncResponse
  );

  const asyncFallback = asyncCandidate ?? (Object.keys(stripUndefined({ asyncState, status: statusValue, statusUrl, webhookId, batchId })).length > 0
    ? stripUndefined({ asyncState, status: statusValue, statusUrl, webhookId, batchId })
    : undefined);
  const asyncResponse = sanitizeForLog(asyncFallback);

  const pushEntry = (url: string | null, reason?: string) => {
    const trimmedUrl = url?.trim() || scrape.sourceUrl;
    const normalizedUrl = normalizeUrlKey(trimmedUrl);
    const existing = normalizedUrl ? existingUrls.has(normalizedUrl) : false;
    const matchedJob = normalizedUrl ? jobByUrl.get(normalizedUrl) : undefined;
    const resolvedReason = reason === "no_items" && existing ? "no_items_existing_job" : reason ?? (existing ? "already_saved" : undefined);
    const skipped = resolvedReason === "already_saved" || resolvedReason === "listing_only" || resolvedReason === "no_items" || resolvedReason === "no_items_existing_job" || existing;
    logs.push({
      url: trimmedUrl ?? "unknown",
      reason: resolvedReason,
      action: skipped ? "skipped" : "scraped",
      provider,
      workflow,
      batchId,
      workflowId,
      requestData: sanitizedRequest,
      response,
      asyncResponse,
      timestamp,
      jobId: matchedJob?._id,
      jobTitle: matchedJob?.title,
      jobCompany: matchedJob?.company,
      jobUrl: matchedJob?.url,
      normalizedCount,
      rawUrlCount,
    });
  };

  if (normalized.length > 0) {
    for (const job of normalized) {
      const url = urlFromJob(job);
      pushEntry(url, !url ? "missing_url" : undefined);
    }
  } else if (rawJobUrls.length > 0) {
    for (const url of rawJobUrls) {
      pushEntry(url, "listing_only");
    }
  } else {
    pushEntry(scrape.sourceUrl, "no_items");
  }

  return logs;
};

const collectCandidateUrls = (scrape: any): string[] => {
  const normalized = normalizedFromItems(scrape.items);
  const urls: string[] = [];
  for (const job of normalized) {
    const url = urlFromJob(job);
    if (url) urls.push(url);
  }
  urls.push(...rawJobUrlsFromItems(scrape.items));
  if (typeof scrape.sourceUrl === "string" && scrape.sourceUrl.trim()) {
    urls.push(scrape.sourceUrl.trim());
  }
  return urls;
};

const buildExistingJobLookupForScrapes = async (ctx: any, scrapes: any[]) => {
  const existingUrls = new Set<string>();
  const jobByUrl = new Map<string, any>();
  const seenCandidates = new Set<string>();

  for (const scrape of scrapes) {
    const candidates = collectCandidateUrls(scrape);
    for (const candidate of candidates) {
      const trimmed = typeof candidate === "string" ? candidate.trim() : "";
      if (!trimmed) continue;
      const queryValues = [trimmed];
      const withoutTrailing = trimmed.replace(/\/+$/, "");
      if (withoutTrailing && withoutTrailing !== trimmed) {
        queryValues.push(withoutTrailing);
      }

      let found: any = null;
      for (const value of queryValues) {
        if (seenCandidates.has(value)) continue;
        seenCandidates.add(value);
        const match = await ctx.db.query("jobs").withIndex("by_url", (q: any) => q.eq("url", value)).first();
        if (match) {
          found = match;
          break;
        }
      }

      if (!found) continue;
      const key = normalizeUrlKey((found).url || trimmed);
      if (key) {
        existingUrls.add(key);
        if (!jobByUrl.has(key)) {
          jobByUrl.set(key, found);
        }
      }
    }
  }

  return { existingUrls, jobByUrl };
};

const _buildExistingJobLookupForScrape = async (ctx: any, scrape: any) => {
  const existingUrls = new Set<string>();
  const jobByUrl = new Map<string, any>();
  const seenCandidates = new Set<string>();
  const candidates = collectCandidateUrls(scrape);

  for (const candidate of candidates) {
    const trimmed = typeof candidate === "string" ? candidate.trim() : "";
    if (!trimmed) continue;
    const normalizedKey = normalizeUrlKey(trimmed);
    if (normalizedKey && jobByUrl.has(normalizedKey)) {
      existingUrls.add(normalizedKey);
      continue;
    }

    const queryValues = [trimmed];
    const withoutTrailing = trimmed.replace(/\/+$/, "");
    if (withoutTrailing && withoutTrailing !== trimmed) {
      queryValues.push(withoutTrailing);
    }

    let found: any = null;
    for (const value of queryValues) {
      if (seenCandidates.has(value)) continue;
      seenCandidates.add(value);
      const match = await ctx.db.query("jobs").withIndex("by_url", (q: any) => q.eq("url", value)).first();
      if (match) {
        found = match;
        break;
      }
    }

    if (!found) continue;
    const key = normalizeUrlKey((found).url || trimmed);
    if (key) {
      existingUrls.add(key);
      if (!jobByUrl.has(key)) {
        jobByUrl.set(key, found);
      }
    }
  }

  return { existingUrls, jobByUrl };
};

export const insertScrapeRecord = mutation({
  args: {
    sourceUrl: v.string(),
    pattern: v.optional(v.string()),
    startedAt: v.number(),
    completedAt: v.number(),
    items: v.any(),
    provider: v.optional(v.string()),
    siteId: v.optional(v.id("sites")),
    workflowName: v.optional(v.string()),
    costMilliCents: v.optional(v.number()),
    jobBoardJobId: v.optional(v.string()),
    batchId: v.optional(v.string()),
    workflowId: v.optional(v.string()),
    workflowType: v.optional(v.string()),
    response: v.optional(v.any()),
    asyncState: v.optional(v.string()),
    asyncResponse: v.optional(v.any()),
    subUrls: v.optional(v.array(v.string())),
    request: v.optional(v.any()),
    providerRequest: v.optional(v.any()),
  },
  handler: async (ctx, args) => {
    const id = await ctx.db.insert("scrapes", args);
    return id;
  },
});

export const listScrapes = query({
  args: {
    limit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const lim = Math.max(1, Math.min(args.limit ?? 50, 200));
    const rows = await ctx.db
      .query("scrapes")
      .withIndex("by_source", (q) => q.gt("sourceUrl", ""))
      .order("desc")
      .take(lim);

    return rows.map((row: any) => ({
      _id: row._id,
      sourceUrl: row.sourceUrl,
      provider: row.provider ?? row.items?.provider ?? "unknown",
      workflowName: row.workflowName,
      workflowId: row.workflowId,
      workflowType: row.workflowType,
      startedAt: row.startedAt,
      completedAt: row.completedAt,
      batchId: row.batchId,
      jobBoardJobId: row.jobBoardJobId,
      response: row.response,
      asyncState: row.asyncState,
      asyncResponse: row.asyncResponse,
      subUrls: row.subUrls ?? row.items?.seedUrls ?? [],
      type: row.items?.kind ?? row.workflowName ?? row.provider,
    }));
  },
});

export const listUrlScrapeLogs = query({
  args: {
    limit: v.optional(v.number()),
    includeJobLookup: v.optional(v.boolean()),
  },
  handler: async (ctx, args) => {
    const limit = Math.max(1, Math.min(args.limit ?? 200, 400));
    const scrapes = await ctx.db.query("scrapes").order("desc").take(limit * 2);
    const includeJobLookup = args.includeJobLookup ?? false;
    let existingUrls = new Set<string>();
    let jobByUrl = new Map<string, any>();
    if (includeJobLookup) {
      const lookup = await buildExistingJobLookupForScrapes(ctx, scrapes);
      existingUrls = lookup.existingUrls;
      jobByUrl = lookup.jobByUrl;
    }

    const logs: any[] = [];

    for (const scrape of scrapes as any[]) {
      logs.push(...buildUrlLogEntriesForScrape(scrape, { existingUrls, jobByUrl }));
    }

    return logs
      .sort((a, b) => (b.timestamp ?? 0) - (a.timestamp ?? 0))
      .slice(0, limit);
  },
});

export const ingestJobsFromScrape = mutation({
  args: {
    jobs: v.array(
      v.object({
        title: v.string(),
        company: v.string(),
        description: v.string(),
        location: v.string(),
        locations: v.optional(v.array(v.string())),
        city: v.optional(v.string()),
        state: v.optional(v.string()),
        countries: v.optional(v.array(v.string())),
        country: v.optional(v.string()),
        locationStates: v.optional(v.array(v.string())),
        locationSearch: v.optional(v.string()),
        remote: v.boolean(),
        level: v.union(v.literal("junior"), v.literal("mid"), v.literal("senior"), v.literal("staff")),
        totalCompensation: v.number(),
        url: v.string(),
        postedAt: v.number(),
        scrapedAt: v.optional(v.number()),
        scrapedWith: v.optional(v.string()),
        workflowName: v.optional(v.string()),
        scrapedCostMilliCents: v.optional(v.number()),
        compensationUnknown: v.optional(v.boolean()),
        compensationReason: v.optional(v.string()),
        currencyCode: v.optional(v.string()),
        heuristicAttempts: v.optional(v.number()),
        heuristicLastTried: v.optional(v.number()),
        heuristicVersion: v.optional(v.number()),
      })
    ),
    siteId: v.optional(v.id("sites")),
  },
  handler: async (ctx, args) => {
    let companyOverride: string | undefined;
    if (args.siteId) {
      const site = await ctx.db.get(args.siteId);
      if (site && typeof (site as any).name === "string") {
        companyOverride = (site as any).name;
      }
    }
    const aliasCache = new Map<string, string | null>();

    let inserted = 0;
    for (const job of args.jobs) {
      const dup = await ctx.db
        .query("jobs")
        .withIndex("by_url", (q) => q.eq("url", job.url))
        .first();
      if (dup) continue;

      const locationSeed = job.locations ?? [job.location];
      const locationInfo = deriveLocationFields({ locations: locationSeed, location: job.location });
      const { city, state } = splitLocation(job.city ?? job.state ? `${job.city ?? ""}, ${job.state ?? ""}` : locationInfo.primaryLocation);
      const compensationUnknown = job.compensationUnknown === true;
      const compensationReason =
        typeof job.compensationReason === "string" && job.compensationReason.trim()
          ? job.compensationReason.trim()
          : compensationUnknown
            ? UNKNOWN_COMPENSATION_REASON
            : job.scrapedWith
              ? `${job.scrapedWith} extracted compensation`
              : "compensation provided in scrape payload";
      const resolvedCompany = await resolveCompanyForUrl(
        ctx,
        job.url,
        job.company,
        companyOverride,
        aliasCache
      );
      const {
        description,
        scrapedWith,
        workflowName,
        scrapedCostMilliCents,
        heuristicAttempts,
        heuristicLastTried,
        heuristicVersion,
        ...jobFields
      } = job;
      const jobId = await ctx.db.insert("jobs", {
        ...jobFields,
        company: resolvedCompany,
        city: job.city ?? city,
        state: job.state ?? state,
        location: formatLocationLabel(job.city ?? city, job.state ?? state, locationInfo.primaryLocation),
        locations: locationInfo.locations,
        countries: locationInfo.countries,
        country: locationInfo.country,
        locationStates: locationInfo.locationStates,
        locationSearch: locationInfo.locationSearch,
        scrapedAt: job.scrapedAt ?? Date.now(),
        compensationUnknown,
        compensationReason,
      });
      const detailRow: any = { jobId };
      if (description !== undefined) detailRow.description = description;
      if (scrapedWith !== undefined) detailRow.scrapedWith = scrapedWith;
      if (workflowName !== undefined) detailRow.workflowName = workflowName;
      if (scrapedCostMilliCents !== undefined) detailRow.scrapedCostMilliCents = scrapedCostMilliCents;
      if (heuristicAttempts !== undefined) detailRow.heuristicAttempts = heuristicAttempts;
      if (heuristicLastTried !== undefined) detailRow.heuristicLastTried = heuristicLastTried;
      if (heuristicVersion !== undefined) detailRow.heuristicVersion = heuristicVersion;
      await ctx.db.insert("job_details", detailRow);
      inserted += 1;
    }
    return { inserted };
  },
});

const decodeHtmlEntities = (value: string) =>
  value
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'");

const stripHtml = (value: string) =>
  decodeHtmlEntities(
    value
      .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, " ")
      .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, " ")
      .replace(/<[^>]+>/g, " ")
  );

const toSafeString = (value: unknown): string => {
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (value && typeof value === "object") {
    try {
      return JSON.stringify(value);
    } catch {
      return "";
    }
  }
  return "";
};

const cleanScrapedText = (value: unknown): string => {
  if (value === null || value === undefined) return "";
  const asString = toSafeString(value);
  return stripHtml(asString).replace(/\s+/g, " ").trim();
};

const extractJsonField = (blob: string, field: string): string | null => {
  const preMatch = blob.match(/<pre[^>]*>([\s\S]*?)<\/pre>/i);
  const candidate = preMatch ? preMatch[1] : blob;

  try {
    const parsed = JSON.parse(candidate);
    const value = (parsed)?.[field];
    if (typeof value === "string") return value;
  } catch {
    // ignore JSON parse failures; we will try regex next
  }

  const regex = new RegExp(`"${field}"\\s*:\\s*"([^"\\\\]{1,400})"`);
  const match = candidate.match(regex);
  return match ? match[1] : null;
};

const normalizeTitle = (raw: unknown): string => {
  const rawString = toSafeString(raw);
  const fromJson = extractJsonField(rawString, "title");
  const cleaned = cleanScrapedText(fromJson ?? rawString);
  if (!cleaned) return "Untitled";
  const MAX_LEN = 140;
  return cleaned.length > MAX_LEN ? `${cleaned.slice(0, MAX_LEN - 3)}...` : cleaned;
};

const collectSeedUrlKeys = (items: any) => {
  const keys = new Set<string>();
  if (!items || typeof items !== "object" || Array.isArray(items)) return keys;
  const seedUrls = (items).seedUrls ?? (items).seed_urls;
  if (Array.isArray(seedUrls)) {
    for (const seed of seedUrls) {
      const normalized = normalizeUrlKey(seed);
      if (normalized) keys.add(normalized);
    }
  }
  return keys;
};

const LISTING_URL_PATTERNS = [
  /\/jobs\/?$/i,
  /\/careers\/?$/i,
  /\/openings\/?$/i,
  /\/job-openings\/?$/i,
  /\/all-jobs\/?$/i,
  /\/positions\/?$/i,
  /\/opportunities\/?$/i,
];

const looksLikeListingUrl = (url: string) => {
  const trimmed = (url ?? "").trim();
  if (!trimmed) return false;
  const withoutQuery = trimmed.split("?")[0];
  return LISTING_URL_PATTERNS.some((pattern) => pattern.test(withoutQuery));
};

const LISTING_TEXT_PATTERNS = [
  /job openings/i,
  /jobs at/i,
  /careers at/i,
  /all locations/i,
  /all teams/i,
  /view openings/i,
  /open positions/i,
  /open roles/i,
];

const looksLikeListingText = (title: string, description: string) => {
  const sample = `${title ?? ""} ${description ?? ""}`;
  return LISTING_TEXT_PATTERNS.some((pattern) => pattern.test(sample));
};

const filterSeedListingJobs = <T extends { title: string; description: string; url: string }>(
  jobs: T[],
  seedUrlKeys: Set<string>
) => {
  if (!seedUrlKeys.size) return { jobs, dropped: [] as { url: string; title: string }[] };
  const kept: T[] = [];
  const dropped: { url: string; title: string }[] = [];
  for (const job of jobs) {
    const key = normalizeUrlKey(job.url);
    const isSeed = key && seedUrlKeys.has(key);
    const isListing = isSeed && (looksLikeListingUrl(job.url) || looksLikeListingText(job.title, job.description));
    if (isListing) {
      dropped.push({ url: job.url, title: job.title });
      continue;
    }
    kept.push(job);
  }
  return { jobs: kept, dropped };
};

// Normalize a scrape payload into a list of job-like objects
export function extractJobs(
  items: any,
  options?: {
    includeSeedListings?: boolean;
    seedListingLogContext?: {
      sourceUrl?: string;
      provider?: string;
      workflowName?: string;
    };
  }
): {
  title: string;
  company: string;
  description: string;
  location: string;
  remote: boolean;
  level: "junior" | "mid" | "senior" | "staff";
  totalCompensation: number;
  compensationUnknown?: boolean;
  compensationReason?: string;
  url: string;
  postedAt?: number;
}[] {
  const rawList: any[] = [];
  const seedUrlKeys = collectSeedUrlKeys(items);

  const DEFAULT_TOTAL_COMPENSATION = 0;

  if (Array.isArray(items)) {
    rawList.push(...items);
  } else if (items && typeof items === "object") {
    if (Array.isArray((items).normalized)) rawList.push(...(items).normalized);
    if (Array.isArray((items).items)) rawList.push(...(items).items);
    if (Array.isArray((items).results)) rawList.push(...(items).results);
    if ((items).results && Array.isArray((items).results.items)) {
      rawList.push(...(items).results.items);
    }
    if ((items).raw && Array.isArray((items).raw.items)) {
      rawList.push(...(items).raw.items);
    }
  }

  const coerceBool = (val: any, location: string, title: string) => {
    if (typeof val === "boolean") return val;
    if (typeof val === "string") {
      const lowered = val.toLowerCase();
      if (["true", "yes", "1", "remote", "hybrid", "fully remote"].includes(lowered)) return true;
    }
    const loc = (location || "").toLowerCase();
    const ttl = (title || "").toLowerCase();
    return loc.includes("remote") || ttl.includes("remote");
  };
  const coerceLevel = (val: any, title: string): "junior" | "mid" | "senior" | "staff" => {
    const norm = typeof val === "string" ? val.toLowerCase() : "";
    const titleNorm = title.toLowerCase();
    const merged = norm || titleNorm;
    if (merged.includes("staff") || merged.includes("principal")) return "staff";
    if (
      merged.includes("senior") ||
      merged.includes("sr ") ||
      merged.includes("sr.") ||
      merged.includes("sr-") ||
      merged.includes("lead") ||
      merged.includes("manager") ||
      merged.includes("director") ||
      merged.includes("vp") ||
      merged.includes("chief")
    )
      return "senior";
    if (merged.includes("jr") || merged.includes("junior") || merged.includes("intern")) return "junior";
    return "mid";
  };
const parseComp = (val: any): { value: number; unknown: boolean } => {
    const parseRangeObj = (obj: any): number | null => {
      if (!obj || typeof obj !== "object") return null;
      const minRaw = (obj).min_value ?? (obj).min;
      const maxRaw = (obj).max_value ?? (obj).max;
      const toNum = (v: any) => {
        if (typeof v === "number" && Number.isFinite(v)) return v;
        if (typeof v === "string") {
          const parsed = Number(v.replace(/,/g, ""));
          if (Number.isFinite(parsed)) return parsed;
        }
        return null;
      };
      const max = toNum(maxRaw);
      const min = toNum(minRaw);
      if (max !== null && max > 0) return max;
      if (min !== null && min > 0) return min;
      return null;
    };

    if (typeof val === "number" && Number.isFinite(val) && val > 0) return { value: val, unknown: false };
    const rangeValue = parseRangeObj(val);
    if (rangeValue !== null) return { value: rangeValue, unknown: false };
    if (typeof val === "string") {
      const matches = val.replace(/\u00a0/g, " ").match(/[0-9][0-9,.]+/g);
      if (matches && matches.length) {
        const parsed = matches
          .map((m) => Number(m.replace(/,/g, "")))
          .filter((n) => Number.isFinite(n) && n > 0);
        if (parsed.length) {
          return { value: Math.max(...parsed), unknown: false };
        }
      }
    }
    return { value: DEFAULT_TOTAL_COMPENSATION, unknown: true };
  };
  const parsePostedAt = (val: any, fallback: number): number => {
    if (typeof val === "number" && Number.isFinite(val)) {
      if (val > 1e12) return val;
      if (val > 1e9) return Math.floor(val * 1000);
    }
    if (typeof val === "string") {
      const parsed = Date.parse(val);
      if (!Number.isNaN(parsed)) return parsed;
    }
    return fallback;
  };

  const jobs = rawList
    .map((row: any) => {
      const rawTitle =
        (row && typeof row === "object"
          ? row.job_title ?? row.title ?? row.heading ?? row.position
          : undefined) ?? row;
      const title = normalizeTitle(rawTitle);

      const rawCompanyFromJson =
        typeof rawTitle === "string"
          ? extractJsonField(rawTitle, "company_name") ?? extractJsonField(rawTitle, "company")
          : null;

      const rawCompany =
        typeof row?.company === "string"
          ? row.company
          : typeof row?.company_name === "string"
            ? row.company_name
            : typeof row?.employer === "string"
              ? row.employer
              : typeof row?.organization === "string"
                ? row.organization
                : rawCompanyFromJson ?? "Unknown";

      const url = String(row?.url || row?.link || row?.href || row?.absolute_url || "").trim();

      const rawLocation =
        typeof row?.location === "string"
          ? row.location
          : typeof row?.location?.name === "string"
            ? row.location.name
            : typeof row?.city === "string"
              ? row.city
              : "Unknown";
      const location = cleanScrapedText(rawLocation) || "Unknown";
      const { city, state } = splitLocation(location);
      const company = rawCompany || fallbackCompanyName(rawCompany, url);
      const locationLabel = formatLocationLabel(city, state, location);
      const remote = coerceBool(row.remote, locationLabel, title);
      const description =
        typeof row?.description === "string"
          ? cleanScrapedText(row.description)
          : typeof row?.content === "string"
            ? cleanScrapedText(row.content)
            : typeof row === "string"
              ? cleanScrapedText(row)
              : JSON.stringify(row, null, 2).slice(0, 4000);
      // Prefer structured pay range from Greenhouse metadata when present
      const compensationSource: any =
        (Array.isArray((row).metadata)
          ? (row).metadata.find?.((m: any) => m?.value_type === "currency_range" && m?.value) ?? null
          : null)?.value;

      const { value: totalCompensation, unknown: compensationUnknown } = parseComp(
        compensationSource ??
          (row).totalCompensation ??
          (row).total_compensation ??
          (row).salary ??
          (row).compensation
      );
      const postedAt = parsePostedAt((row).postedAt ?? (row).posted_at, Date.now());
      const compensationReason =
        typeof (row).compensationReason === "string" && (row).compensationReason.trim()
          ? (row).compensationReason.trim()
          : typeof (row).compensation_reason === "string" && (row).compensation_reason.trim()
            ? (row).compensation_reason.trim()
            : compensationSource
              ? "pay range provided in metadata"
              : compensationUnknown
              ? UNKNOWN_COMPENSATION_REASON
              : "compensation provided in scrape payload";

      return {
        title: title || "Untitled",
        company: company || "Unknown",
        description,
        location: locationLabel || "Unknown",
        city,
        state,
        remote,
        level: coerceLevel((row).level, title),
        totalCompensation,
        compensationUnknown,
        compensationReason,
        url: url || "",
        postedAt,
      };
    })
    .filter((j) => j.url); // require a URL to keep signal

  if (options?.includeSeedListings) return jobs;
  if (!seedUrlKeys.size) return jobs;

  const filtered = filterSeedListingJobs(jobs, seedUrlKeys);
  if (filtered.dropped.length && options?.seedListingLogContext) {
    const sample = filtered.dropped.slice(0, 5);
    console.warn("Skipping seed listing URLs in scrape ingest", {
      reason: "seed_listing_url",
      count: filtered.dropped.length,
      sample,
      ...options.seedListingLogContext,
    });
  }
  return filtered.jobs;
}

/**
 * API endpoint to update Temporal status
 *
 * POST /api/temporal/status
 * Body: { 
 *   workerId: string,
 *   hostname: string,
 *   temporalAddress: string,
 *   temporalNamespace: string,
 *   taskQueue: string,
 *   workflows: [...],
 *   noWorkflowsReason?: string
 * }
 */
http.route({
  path: "/api/temporal/status",
  method: "POST",
  handler: httpAction(async (ctx, request) => {
    try {
      const body = await request.json();
      await ctx.runMutation(api.temporal.updateStatus, {
        workerId: body.workerId,
        hostname: body.hostname,
        temporalAddress: body.temporalAddress,
        temporalNamespace: body.temporalNamespace,
        taskQueue: body.taskQueue,
        workflows: body.workflows,
        noWorkflowsReason: body.noWorkflowsReason,
      });
      return new Response(JSON.stringify({ success: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    } catch {
      return new Response(
        JSON.stringify({ error: "Invalid JSON body" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }
  }),
});

http.route({
  path: "/api/temporal/workflow-runs",
  method: "GET",
  handler: httpAction(async (ctx) => {
    const runs = await ctx.runQuery(api.temporal.listWorkflowRuns, { limit: 50 });
    return new Response(JSON.stringify(runs), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }),
});

http.route({
  path: "/api/temporal/workflow-run",
  method: "POST",
  handler: httpAction(async (ctx, request) => {
    try {
      const body = await request.json();
      await ctx.runMutation(api.temporal.recordWorkflowRun, body);
      return new Response(JSON.stringify({ success: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    } catch {
      return new Response(
        JSON.stringify({ error: "Invalid JSON body" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }
  }),
});

http.route({
  path: "/api/temporal/schedule",
  method: "GET",
  handler: httpAction(async (ctx) => {
    const info = await ctx.runQuery(api.temporal.getScrapeSchedule, {});
    return new Response(JSON.stringify(info), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }),
});

export default http;
