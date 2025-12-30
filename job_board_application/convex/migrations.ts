import { internal } from "./_generated/api";
import { formatLocationLabel, deriveLocationFields } from "./location";
import { Migrations } from "@convex-dev/migrations";
import { components } from "./_generated/api.js";
import { DataModel, Id } from "./_generated/dataModel.js";
import { normalizeSiteUrl, siteCanonicalKey, fallbackCompanyNameFromUrl, greenhouseSlugFromUrl } from "./siteUtils";
import { deriveCompanyKey, deriveEngineerFlag } from "./jobs";
import { internalMutation } from "./_generated/server";
import { syncSiteSchedulesFromYaml } from "./siteScheduleSync";
import { countJobs } from "./lib/scrapeCounts";

export const migrations = new Migrations<DataModel>(components.migrations);
export const run = migrations.runner();
export const runScrapeActivityBackfill = migrations.runner(internal.migrations.backfillScrapeActivity);

type JobId = Id<"jobs">;

const isJobRow = (row: any): row is { _id: JobId; title: string; company: string } =>
  row && typeof row.title === "string" && typeof row.company === "string";

const resolveJobId = async (ctx: any, candidate: string): Promise<JobId | null> => {
  if (!candidate) return null;
  const direct = await ctx.db.get(candidate as JobId);
  if (isJobRow(direct)) return candidate as JobId;
  if (direct && typeof (direct).jobId === "string") {
    const nested = await ctx.db.get((direct).jobId as JobId);
    if (isJobRow(nested)) return (direct).jobId as JobId;
  }
  return null;
};

export const fixJobLocations = migrations.define({
  table: "jobs",
  migrateOne: async (ctx, doc) => {
    const locationInfo = deriveLocationFields({ locations: (doc as any).locations, location: doc.location });
    const { city, state, primaryLocation } = locationInfo;
    const location = formatLocationLabel(city, state, primaryLocation);

    const update: Record<string, any> = {};
    if (doc.city !== city) update.city = city;
    if (doc.state !== state) update.state = state;
    if (doc.location !== location) update.location = location;
    if (!Array.isArray((doc as any).locations) || JSON.stringify((doc as any).locations) !== JSON.stringify(locationInfo.locations)) {
      update.locations = locationInfo.locations;
    }
    if (!Array.isArray((doc as any).countries) || JSON.stringify((doc as any).countries) !== JSON.stringify(locationInfo.countries)) {
      update.countries = locationInfo.countries;
    }
    if ((doc as any).country !== locationInfo.country) {
      update.country = locationInfo.country;
    }
    if (!Array.isArray((doc as any).locationStates) || JSON.stringify((doc as any).locationStates) !== JSON.stringify(locationInfo.locationStates)) {
      update.locationStates = locationInfo.locationStates;
    }
    if ((doc as any).locationSearch !== locationInfo.locationSearch) {
      update.locationSearch = locationInfo.locationSearch;
    }

    if (Object.keys(update).length > 0) {
      await ctx.db.patch(doc._id, update);
    }
  },
});

export const backfillScrapeMetadata = migrations.define({
  table: "jobs",
  migrateOne: async (ctx, doc) => {
    const update: Record<string, any> = {};
    if (doc.scrapedAt === undefined) {
      update.scrapedAt = doc.postedAt ?? Date.now();
    }
    if (Object.keys(update).length > 0) {
      await ctx.db.patch(doc._id, update);
    }
  },
});

export const backfillEngineerFlag = migrations.define({
  table: "jobs",
  migrateOne: async (ctx, doc) => {
    const desired = deriveEngineerFlag(doc.title);
    if ((doc as any).engineer !== desired) {
      await ctx.db.patch(doc._id, { engineer: desired });
    }
  },
});

export const moveJobDetails = migrations.define({
  table: "jobs",
  migrateOne: async (ctx, doc) => {
    const raw: any = doc as any;
    const detailPayload: Record<string, any> = {};
    if (raw.description !== undefined) detailPayload.description = raw.description;
    if (raw.scrapedWith !== undefined) detailPayload.scrapedWith = raw.scrapedWith;
    if (raw.workflowName !== undefined) detailPayload.workflowName = raw.workflowName;
    if (raw.scrapedCostMilliCents !== undefined) detailPayload.scrapedCostMilliCents = raw.scrapedCostMilliCents;
    if (raw.heuristicAttempts !== undefined) detailPayload.heuristicAttempts = raw.heuristicAttempts;
    if (raw.heuristicLastTried !== undefined) detailPayload.heuristicLastTried = raw.heuristicLastTried;
    if (raw.heuristicVersion !== undefined) detailPayload.heuristicVersion = raw.heuristicVersion;

    if (Object.keys(detailPayload).length > 0) {
      const existing = await ctx.db
        .query("job_details")
        .withIndex("by_job", (q) => q.eq("jobId", doc._id))
        .first();
      if (existing) {
        await ctx.db.patch(existing._id, detailPayload);
      } else {
        await ctx.db.insert("job_details", { jobId: doc._id, ...detailPayload });
      }
    }

    const cleanup: Record<string, any> = {};
    for (const field of [
      "description",
      "scrapedWith",
      "workflowName",
      "scrapedCostMilliCents",
      "heuristicAttempts",
      "heuristicLastTried",
      "heuristicVersion",
    ]) {
      if (raw[field] !== undefined) {
        cleanup[field] = undefined;
      }
    }
    if (Object.keys(cleanup).length > 0) {
      await ctx.db.patch(doc._id, cleanup);
    }
  },
});

export const backfillScrapeRecords = migrations.define({
  table: "scrapes",
  migrateOne: async (ctx, doc) => {
    const update = buildScrapeRecordPatch(doc);
    if (Object.keys(update).length > 0) {
      await ctx.db.patch(doc._id, update);
    }
  },
});

export const backfillScrapeActivity = migrations.define({
  table: "scrapes",
  migrateOne: async (ctx, doc) => {
    const sourceUrl = (doc as any).sourceUrl;
    const completedAt = (doc as any).completedAt;
    const startedAt = (doc as any).startedAt;
    if (typeof sourceUrl !== "string" || !sourceUrl.trim()) return;
    if (typeof completedAt !== "number" || typeof startedAt !== "number") return;

    const existing = await ctx.db
      .query("scrape_activity")
      .withIndex("by_source_completed", (q: any) => q.eq("sourceUrl", sourceUrl).eq("completedAt", completedAt))
      .collect();

    const siteId = (doc as any).siteId ?? undefined;
    const hasMatch = existing.some(
      (row: any) => row.startedAt === startedAt && (row.siteId ?? null) === (siteId ?? null)
    );
    if (hasMatch) return;

    await ctx.db.insert("scrape_activity", {
      sourceUrl,
      siteId,
      startedAt,
      completedAt,
      jobCount: countJobs((doc as any).items),
    });
  },
});

export const deriveScrapeQueueScheduledAt = (doc: any): number => {
  if (typeof doc?.createdAt === "number") return doc.createdAt;
  if (typeof doc?.updatedAt === "number") return doc.updatedAt;
  return Date.now();
};

export const backfillScrapeQueueScheduledAt = migrations.define({
  table: "scrape_url_queue",
  migrateOne: async (ctx, doc) => {
    if (doc.scheduledAt === undefined || doc.scheduledAt === null) {
      const scheduledAt = deriveScrapeQueueScheduledAt(doc);
      await ctx.db.patch(doc._id, { scheduledAt });
    }
  },
});

export const repairJobDetailJobIds = migrations.define({
  table: "job_details",
  migrateOne: async (ctx, doc) => {
    const resolved = await resolveJobId(ctx, (doc as any).jobId);
    if (!resolved) {
      await ctx.db.delete(doc._id);
      return;
    }
    if (resolved !== (doc as any).jobId) {
      await ctx.db.patch(doc._id, { jobId: resolved });
    }
  },
});

export const repairApplicationJobIds = migrations.define({
  table: "applications",
  migrateOne: async (ctx, doc) => {
    const resolved = await resolveJobId(ctx, (doc as any).jobId);
    if (!resolved) {
      await ctx.db.delete(doc._id);
      return;
    }
    if (resolved !== (doc as any).jobId) {
      await ctx.db.patch(doc._id, { jobId: resolved });
    }
  },
});

export const repairJobIdReferencesImpl = async (ctx: any) => {
  const jobDetails = await ctx.db.query("job_details").collect();
  const applications = await ctx.db.query("applications").collect();
  let jobDetailsFixed = 0;
  let jobDetailsDeleted = 0;
  let applicationsFixed = 0;
  let applicationsDeleted = 0;

  for (const row of jobDetails as any[]) {
    const resolved = await resolveJobId(ctx, row.jobId);
    if (!resolved) {
      await ctx.db.delete(row._id);
      jobDetailsDeleted += 1;
      continue;
    }
    if (resolved !== row.jobId) {
      await ctx.db.patch(row._id, { jobId: resolved });
      jobDetailsFixed += 1;
    }
  }

  for (const row of applications as any[]) {
    const resolved = await resolveJobId(ctx, row.jobId);
    if (!resolved) {
      await ctx.db.delete(row._id);
      applicationsDeleted += 1;
      continue;
    }
    if (resolved !== row.jobId) {
      await ctx.db.patch(row._id, { jobId: resolved });
      applicationsFixed += 1;
    }
  }

  return {
    jobDetailsFixed,
    jobDetailsDeleted,
    applicationsFixed,
    applicationsDeleted,
  };
};

export const dedupeSitesImpl = async (ctx: any) => {
  const rows = await ctx.db.query("sites").collect();
  const byKey = new Map<string, any[]>();

  for (const row of rows as any[]) {
    const normalizedUrl = normalizeSiteUrl(row.url, row.type);
    const key = siteCanonicalKey(normalizedUrl, row.type);
    const arr = byKey.get(key) ?? [];
    arr.push({ ...row, _normalizedUrl: normalizedUrl });
    byKey.set(key, arr);
  }

  const score = (site: any) => {
    return [
      site.enabled ? 1 : 0,
      site.failed ? 0 : 1,
      site.completed ? 0 : 1,
      typeof site.lastRunAt === "number" ? site.lastRunAt : 0,
      typeof site._creationTime === "number" ? site._creationTime : 0,
    ];
  };

  for (const [, sites] of byKey.entries()) {
    if (!sites.length) continue;
    const sorted = sites.slice().sort((a, b) => {
      const scoreA = score(a);
      const scoreB = score(b);
      for (let i = 0; i < scoreA.length; i++) {
        if (scoreA[i] !== scoreB[i]) return scoreB[i] - scoreA[i];
      }
      return 0;
    });

    const keep = sorted[0];
    const keepPatch: Record<string, any> = {};
    if (keep.url !== keep._normalizedUrl) keepPatch.url = keep._normalizedUrl;
    if (!keep.name) {
      keepPatch.name = fallbackCompanyNameFromUrl(keep._normalizedUrl);
    }
    if (Object.keys(keepPatch).length > 0) {
      await ctx.db.patch(keep._id, keepPatch);
    }

    for (const dup of sorted.slice(1)) {
      const patch: Record<string, any> = {
        enabled: false,
        completed: true,
        failed: true,
        lockExpiresAt: 0,
        lockedBy: "",
        manualTriggerAt: 0,
        scheduleId: undefined,
        lastError: `duplicate_of:${keep._id}`,
        url: keep._normalizedUrl,
      };
      await ctx.db.patch(dup._id, patch);
    }
  }
};

export const dedupeSites = migrations.define({
  table: "sites",
  migrateOne: (() => {
    let ran = false;
    return async (ctx) => {
      if (ran) return;
      ran = true;
      await dedupeSitesImpl(ctx);
    };
  })(),
});

export const retagGreenhouseJobsImpl = async (ctx: any) => {
  const aliasRows = await ctx.db.query("domain_aliases").collect();
  const aliasMap = new Map<string, string>();
  for (const row of aliasRows as any[]) {
    if (typeof row.domain === "string" && typeof row.alias === "string") {
      aliasMap.set(row.domain, row.alias);
    }
  }

  const jobs = await ctx.db.query("jobs").collect();
  for (const job of jobs as any[]) {
    if (!job.url || typeof job.url !== "string") continue;
    const slug = greenhouseSlugFromUrl(job.url);
    if (!slug) continue;
    const domain = `${slug}.greenhouse.io`;
    const desired = aliasMap.get(domain) ?? fallbackCompanyNameFromUrl(normalizeSiteUrl(job.url, "greenhouse"));
    if (desired && desired !== job.company) {
      await ctx.db.patch(job._id, { company: desired, companyKey: deriveCompanyKey(desired) });
    }
  }
};

export const retagGreenhouseJobs = migrations.define({
  table: "jobs",
  migrateOne: (() => {
    let ran = false;
    return async (ctx) => {
      if (ran) return;
      ran = true;
      await retagGreenhouseJobsImpl(ctx);
    };
  })(),
});

export const backfillCompanyKey = migrations.define({
  table: "jobs",
  migrateOne: async (ctx, doc) => {
    const desired = deriveCompanyKey(doc.company);
    if (!desired || (doc as any).companyKey === desired) return;
    await ctx.db.patch(doc._id, { companyKey: desired });
  },
});

export const syncSiteSchedules = migrations.define({
  table: "sites",
  migrateOne: (() => {
    let ran = false;
    return async (ctx) => {
      if (ran) return;
      ran = true;
      await syncSiteSchedulesFromYaml(ctx);
    };
  })(),
});

export const runAll = internalMutation({
  args: {},
  handler: async (ctx): Promise<any> => {
    return await migrations.runSerially(ctx, [
      internal.migrations.fixJobLocations,
      internal.migrations.backfillScrapeMetadata,
      internal.migrations.backfillEngineerFlag,
      internal.migrations.backfillCompanyKey,
      internal.migrations.moveJobDetails,
      internal.migrations.backfillScrapeRecords,
      internal.migrations.backfillScrapeActivity,
      internal.migrations.repairJobDetailJobIds,
      internal.migrations.repairApplicationJobIds,
      internal.migrations.syncSiteSchedules,
      internal.migrations.dedupeSites,
      internal.migrations.retagGreenhouseJobs,
    ]);
  },
});

export const deriveCostMilliCents = (doc: any): number => {
  const costVal = doc?.costMilliCents;
  if (typeof costVal === "number") return costVal;
  const fromItems = doc?.items?.costMilliCents;
  if (typeof fromItems === "number") return fromItems;
  return 0;
};

export const deriveProvider = (doc: any): string => {
  const val = doc?.provider;
  if (typeof val === "string" && val.trim()) return val.trim();
  const fromItems = doc?.items?.provider;
  if (typeof fromItems === "string" && fromItems.trim()) return fromItems.trim();
  return "unknown";
};

export const buildScrapeRecordPatch = (doc: any): Record<string, any> => {
  const update: Record<string, any> = {};
  const provider = deriveProvider(doc);
  if (provider !== (doc).provider) {
    update.provider = provider;
  }
  if (doc.workflowName === null) {
    update.workflowName = undefined;
  }
  const costVal = deriveCostMilliCents(doc);
  if (costVal !== (doc).costMilliCents) {
    update.costMilliCents = costVal;
  }
  return update;
};
