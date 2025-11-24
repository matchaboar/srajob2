import { httpRouter } from "convex/server";
import { httpAction, mutation, query } from "./_generated/server";
import { v } from "convex/values";
import { api } from "./_generated/api";
import type { Id, Doc } from "./_generated/dataModel";
import { splitLocation, formatLocationLabel } from "./location";

const http = httpRouter();
const DEFAULT_TIMEZONE = "America/Denver";
const scheduleDay = v.union(
  v.literal("mon"),
  v.literal("tue"),
  v.literal("wed"),
  v.literal("thu"),
  v.literal("fri"),
  v.literal("sat"),
  v.literal("sun")
);
const scheduleDayOrder: ("sun" | "mon" | "tue" | "wed" | "thu" | "fri" | "sat")[] = [
  "sun",
  "mon",
  "tue",
  "wed",
  "thu",
  "fri",
  "sat",
];
const weekdayFromShort: Record<string, (typeof scheduleDayOrder)[number]> = {
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
    } catch (error) {
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
        pattern: body.pattern ?? undefined,
        scheduleId: body.scheduleId ?? undefined,
        enabled: body.enabled ?? true,
      });
      return new Response(JSON.stringify({ success: true, id }), {
        status: 201,
        headers: { "Content-Type": "application/json" },
      });
    } catch (error) {
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
    const rows = await ctx.runQuery(api.sites.listScrapeActivity, {});
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
      const sid = (site as any).scheduleId as string | undefined;
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
        siteCount: siteCounts.get((s as any)._id as any) ?? 0,
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

export const updateSiteSchedule = mutation({
  args: {
    id: v.id("sites"),
    scheduleId: v.optional(v.id("scrape_schedules")),
  },
  handler: async (ctx, args) => {
    await ctx.db.patch(args.id, { scheduleId: args.scheduleId });
    return args.id;
  },
});

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

// Gather every job URL we've already stored for a site so the scraper can avoid re-visiting them
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
      const jobs = extractJobs((scrape as any).items);
      for (const job of jobs) {
        if (job.url) seen.add(job.url);
      }
    }

    const matcher = buildUrlMatcher(args.pattern ?? args.sourceUrl);
    const jobs = await ctx.db.query("jobs").collect();
    for (const job of jobs as any[]) {
      const url = (job as any).url;
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
  },
  handler: async (ctx, args) => {
    const now = Date.now();
    const ttlMs = Math.max(1, Math.floor((args.lockSeconds ?? 300) * 1000));

    // Pull enabled sites and pick the first that is not completed and not locked (or lock expired)
    const candidates = await ctx.db
      .query("sites")
      .withIndex("by_enabled", (q) => q.eq("enabled", true))
      .collect();

    const eligible: any[] = [];
    const scheduleCache = new Map<string, any>();

    for (const site of candidates as any[]) {
      if (site.completed) continue;
      if (site.failed) continue;
      if (site.lockExpiresAt && site.lockExpiresAt > now) continue;

      // Manual trigger: bypass schedule/time gating for a short window
      if (site.manualTriggerAt && site.manualTriggerAt > now - 15 * 60 * 1000) {
        eligible.push({ site, eligibleAt: site.manualTriggerAt });
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

        const lastRun = site.lastRunAt ?? 0;
        if (lastRun >= eligibleAt) continue;

        eligible.push({ site, eligibleAt });
        continue;
      }

      // No schedule: treat as always eligible
      eligible.push({ site, eligibleAt: site.lastRunAt ?? 0 });
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
    const s = fresh as Doc<"sites">;
    return {
      _id: s._id,
      name: s.name,
      url: s.url,
      pattern: s.pattern,
      scheduleId: s.scheduleId,
      enabled: s.enabled,
      lastRunAt: s.lastRunAt,
      lockedBy: s.lockedBy,
      lockExpiresAt: s.lockExpiresAt,
      completed: s.completed,
    };
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
    return { success: true };
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
    });
    return { success: true };
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
    pattern: v.optional(v.string()),
    scheduleId: v.optional(v.id("scrape_schedules")),
    enabled: v.boolean(),
  },
  handler: async (ctx, args) => {
    // For simplicity, just insert a new record
    return await ctx.db.insert("sites", {
      ...args,
      // New sites should be leased immediately; keep lastRunAt at 0
      lastRunAt: 0,
    });
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

export const bulkUpsertSites = mutation({
  args: {
    sites: v.array(
      v.object({
        name: v.optional(v.string()),
        url: v.string(),
        pattern: v.optional(v.string()),
        scheduleId: v.optional(v.id("scrape_schedules")),
        enabled: v.boolean(),
      })
    ),
  },
  handler: async (ctx, args) => {
    const ids = [];
    for (const site of args.sites) {
      const id = await ctx.db.insert("sites", {
        ...site,
        // Same behavior as single add: make new sites immediately leaseable
        lastRunAt: 0,
      });
      ids.push(id);
    }
    return ids;
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
    url: v.string(),
    test: v.optional(v.boolean()),
  },
  handler: async (ctx, args) => {
    const parsed = splitLocation(args.location);
    const city = args.city ?? parsed.city;
    const state = args.state ?? parsed.state;
    const locationLabel = formatLocationLabel(city, state, args.location);
    const jobId = await ctx.db.insert("jobs", {
      ...args,
      location: locationLabel,
      city,
      state,
      postedAt: Date.now(),
    });
    return jobId;
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
      });

      // Opportunistically ingest jobs into jobs table for UI
      try {
        const jobs = extractJobs(body.items);
        if (jobs.length > 0) {
          await ctx.runMutation(api.router.ingestJobsFromScrape, {
            jobs: jobs.map((j) => ({ ...j, postedAt: j.postedAt ?? now })),
          });
        }
      } catch (err: any) {
        console.error("Failed to ingest jobs from scrape", err?.message ?? err);
      }

      return new Response(
        JSON.stringify({ success: true, scrapeId }),
        { status: 201, headers: { "Content-Type": "application/json" } }
      );
    } catch (error) {
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

export const insertScrapeRecord = mutation({
  args: {
    sourceUrl: v.string(),
    pattern: v.optional(v.string()),
    startedAt: v.number(),
    completedAt: v.number(),
    items: v.any(),
  },
  handler: async (ctx, args) => {
    const id = await ctx.db.insert("scrapes", args);
    return id;
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
        city: v.optional(v.string()),
        state: v.optional(v.string()),
        remote: v.boolean(),
        level: v.union(v.literal("junior"), v.literal("mid"), v.literal("senior"), v.literal("staff")),
        totalCompensation: v.number(),
        url: v.string(),
        postedAt: v.number(),
      })
    ),
  },
  handler: async (ctx, args) => {
    let inserted = 0;
    for (const job of args.jobs) {
      const dup = await ctx.db
        .query("jobs")
        .filter((q) => q.eq(q.field("url"), job.url))
        .first();
      if (dup) continue;

      const { city, state } = splitLocation(job.city ?? job.state ? `${job.city ?? ""}, ${job.state ?? ""}` : job.location);
      await ctx.db.insert("jobs", {
        ...job,
        city: job.city ?? city,
        state: job.state ?? state,
        location: formatLocationLabel(job.city ?? city, job.state ?? state, job.location),
      });
      inserted += 1;
    }
    return { inserted };
  },
});

// Normalize a scrape payload into a list of job-like objects
function extractJobs(items: any): {
  title: string;
  company: string;
  description: string;
  location: string;
  remote: boolean;
  level: "junior" | "mid" | "senior" | "staff";
  totalCompensation: number;
  url: string;
  postedAt?: number;
}[] {
  const rawList: any[] = [];

  const DEFAULT_TOTAL_COMPENSATION = 151000;
  const maybeArray = (val: any) => (Array.isArray(val) ? val : []);

  if (Array.isArray(items)) {
    rawList.push(...items);
  } else if (items && typeof items === "object") {
    if (Array.isArray((items as any).normalized)) rawList.push(...(items as any).normalized);
    if (Array.isArray((items as any).items)) rawList.push(...(items as any).items);
    if (Array.isArray((items as any).results)) rawList.push(...(items as any).results);
    if ((items as any).results && Array.isArray((items as any).results.items)) {
      rawList.push(...(items as any).results.items);
    }
    if ((items as any).raw && Array.isArray((items as any).raw.items)) {
      rawList.push(...(items as any).raw.items);
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
  const parseComp = (val: any): number => {
    if (typeof val === "number" && Number.isFinite(val) && val > 0) return val;
    if (typeof val === "string") {
      const matches = val.replace(/\u00a0/g, " ").match(/[0-9][0-9,.]+/g);
      if (matches && matches.length) {
        const parsed = matches
          .map((m) => Number(m.replace(/,/g, "")))
          .filter((n) => Number.isFinite(n) && n > 0);
        if (parsed.length) return Math.max(...parsed);
      }
    }
    return DEFAULT_TOTAL_COMPENSATION;
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

  return rawList
    .map((row: any) => {
      const title = String(row.job_title || row.title || "Untitled").trim();
      const company = String(row.company || row.employer || "Unknown").trim();
      const url = String(row.url || row.link || row.href || "").trim();
      const location = String(row.location || row.city || "Unknown").trim();
      const { city, state } = splitLocation(location);
      const locationLabel = formatLocationLabel(city, state, location);
      const remote = coerceBool(row.remote, locationLabel, title);
      const description =
        typeof row.description === "string"
          ? row.description
          : JSON.stringify(row, null, 2).slice(0, 4000);
      const totalCompensation = parseComp((row as any).totalCompensation ?? (row as any).total_compensation ?? (row as any).salary ?? (row as any).compensation);
      const postedAt = parsePostedAt((row as any).postedAt ?? (row as any).posted_at, Date.now());

      return {
        title: title || "Untitled",
        company: company || "Unknown",
        description,
        location: locationLabel || "Unknown",
        city,
        state,
        remote,
        level: coerceLevel((row as any).level, title),
        totalCompensation,
        url: url || "",
        postedAt,
      };
    })
    .filter((j) => j.url); // require a URL to keep signal
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
    } catch (error) {
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
    } catch (error) {
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
