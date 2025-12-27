import YAML from "yaml";
import type { Id } from "./_generated/dataModel";
import { fallbackCompanyNameFromUrl, normalizeSiteUrl, siteCanonicalKey } from "./siteUtils";
import devSchedules from "./site_schedules.dev.json";
import prodSchedules from "./site_schedules.prod.json";
import { SPIDER_CLOUD_DEFAULT_SITE_TYPES, type SiteType } from "./siteTypes";

const DEFAULT_TIMEZONE = "America/Denver";
const VALID_DAYS = new Set(["sun", "mon", "tue", "wed", "thu", "fri", "sat"]);

export type SiteScheduleEntry = {
  url: string;
  name?: string;
  enabled?: boolean;
  type?: SiteType;
  scrapeProvider?: "fetchfox" | "firecrawl" | "spidercloud" | "fetchfox_spidercloud";
  pattern?: string;
  schedule?: {
    name: string;
    days: string[];
    startTime: string;
    intervalMinutes: number;
    timezone?: string;
  } | null;
};

export type NormalizedSiteSchedule = {
  url: string;
  name?: string;
  enabled: boolean;
  type?: SiteType;
  scrapeProvider?: "fetchfox" | "firecrawl" | "spidercloud" | "fetchfox_spidercloud";
  pattern?: string;
  schedule?: {
    name: string;
    days: string[];
    startTime: string;
    intervalMinutes: number;
    timezone: string;
  };
};

const resolveScheduleEnv = (): "dev" | "prod" => {
  const deployment = process.env.CONVEX_DEPLOYMENT ?? "";
  if (deployment.startsWith("prod:")) return "prod";
  if (deployment.startsWith("dev:")) return "dev";
  if (process.env.NODE_ENV === "production") return "prod";
  return "dev";
};
type RawScheduleFile = { site_schedules?: SiteScheduleEntry[] };

const scheduleFiles: Record<"dev" | "prod", RawScheduleFile> = {
  dev: devSchedules as RawScheduleFile,
  prod: prodSchedules as RawScheduleFile,
};

export const parseSiteScheduleYamlText = (text: string): SiteScheduleEntry[] => {
  try {
    const parsed = YAML.parse(text);
    const raw = parsed?.site_schedules;
    return Array.isArray(raw) ? (raw as SiteScheduleEntry[]) : [];
  } catch {
    return [];
  }
};

export const normalizeSiteScheduleEntries = (entries: SiteScheduleEntry[]): NormalizedSiteSchedule[] => {
  const results: NormalizedSiteSchedule[] = [];
  const seen = new Set<string>();

  for (const entry of entries) {
    if (!entry || typeof entry.url !== "string" || !entry.url.trim()) {
      continue;
    }
    const type = entry.type;
    const normalizedUrl = normalizeSiteUrl(entry.url, type);
    const key = siteCanonicalKey(normalizedUrl, type);
    if (seen.has(key)) continue;
    seen.add(key);

    const enabled = entry.enabled !== false;
    const schedule = normalizeSchedule(entry.schedule ?? undefined);

    results.push({
      url: normalizedUrl,
      name: typeof entry.name === "string" && entry.name.trim() ? entry.name.trim() : undefined,
      enabled,
      type,
      scrapeProvider: entry.scrapeProvider,
      pattern: typeof entry.pattern === "string" && entry.pattern.trim() ? entry.pattern.trim() : undefined,
      schedule: schedule ?? undefined,
    });
  }

  return results;
};

const normalizeSchedule = (
  raw?: SiteScheduleEntry["schedule"] | undefined
): NormalizedSiteSchedule["schedule"] | null => {
  if (!raw || typeof raw !== "object") return null;
  const name = typeof raw.name === "string" ? raw.name.trim() : "";
  if (!name) return null;
  const days = Array.isArray(raw.days)
    ? Array.from(new Set(raw.days.map((d) => String(d).toLowerCase()).filter((d) => VALID_DAYS.has(d))))
    : [];
  if (days.length === 0) return null;
  const startTime = typeof raw.startTime === "string" ? raw.startTime.trim() : "";
  if (!/^\d{2}:\d{2}$/.test(startTime)) return null;
  const intervalMinutes = Math.max(1, Math.floor(Number(raw.intervalMinutes ?? 0)));
  const timezoneRaw = typeof raw.timezone === "string" ? raw.timezone.trim() : "";
  const timezone = timezoneRaw || DEFAULT_TIMEZONE;

  return { name, days, startTime, intervalMinutes, timezone };
};

export const loadSiteScheduleEntries = (): NormalizedSiteSchedule[] => {
  const env = resolveScheduleEnv();
  const raw = scheduleFiles[env]?.site_schedules ?? [];
  return normalizeSiteScheduleEntries(raw);
};

type SyncResult = {
  addedSites: number;
  createdSchedules: number;
  skippedExisting: number;
  skippedInvalid: number;
};

export const syncSiteSchedulesFromEntries = async (
  ctx: any,
  entries: NormalizedSiteSchedule[],
  nowMs: number = Date.now()
): Promise<SyncResult> => {
  const result: SyncResult = {
    addedSites: 0,
    createdSchedules: 0,
    skippedExisting: 0,
    skippedInvalid: 0,
  };

  if (!entries.length) return result;

  const existingSites = await ctx.db.query("sites").collect();
  const existingSchedules = await ctx.db.query("scrape_schedules").collect();

  const scheduleByKey = new Map<string, any>();
  for (const sched of existingSchedules as any[]) {
    const name = typeof (sched as any).name === "string" ? (sched as any).name.trim() : "";
    if (!name) continue;
    scheduleByKey.set(name.toLowerCase(), sched);
  }

  const siteKeys = new Set<string>();
  for (const site of existingSites as any[]) {
    const key = siteCanonicalKey((site as any).url, (site as any).type);
    siteKeys.add(key);
  }

  for (const entry of entries) {
    const key = siteCanonicalKey(entry.url, entry.type);
    if (siteKeys.has(key)) {
      result.skippedExisting += 1;
      continue;
    }

    let scheduleId: Id<"scrape_schedules"> | undefined;
    if (entry.schedule) {
      const scheduleKey = entry.schedule.name.toLowerCase();
      let schedule = scheduleByKey.get(scheduleKey);
      if (!schedule) {
        schedule = await ctx.db.insert("scrape_schedules", {
          name: entry.schedule.name,
          days: entry.schedule.days,
          startTime: entry.schedule.startTime,
          intervalMinutes: entry.schedule.intervalMinutes,
          timezone: entry.schedule.timezone,
          createdAt: nowMs,
          updatedAt: nowMs,
        });
        result.createdSchedules += 1;
        scheduleByKey.set(scheduleKey, { _id: schedule, name: entry.schedule.name });
      }
      scheduleId = (schedule as any)._id ?? schedule;
    }

    const siteType = entry.type ?? "general";
    const scrapeProvider =
      entry.scrapeProvider ??
      (SPIDER_CLOUD_DEFAULT_SITE_TYPES.has(siteType) ? "spidercloud" : "fetchfox");

    const name = entry.name ?? fallbackCompanyNameFromUrl(entry.url);

    await ctx.db.insert("sites", {
      name,
      url: entry.url,
      type: siteType,
      scrapeProvider,
      pattern: entry.pattern,
      scheduleId: scheduleId ?? undefined,
      enabled: entry.enabled,
      lastRunAt: 0,
    });
    siteKeys.add(key);
    result.addedSites += 1;
  }

  return result;
};

export const syncSiteSchedulesFromYaml = async (ctx: any): Promise<SyncResult> => {
  const entries = loadSiteScheduleEntries();
  return await syncSiteSchedulesFromEntries(ctx, entries);
};
