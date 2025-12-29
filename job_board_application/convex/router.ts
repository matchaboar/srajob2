import { httpRouter } from "convex/server";
import { httpAction, internalMutation, mutation, query } from "./_generated/server";
import { v } from "convex/values";
import { api } from "./_generated/api";
import type { Id } from "./_generated/dataModel";
import { splitLocation, formatLocationLabel, deriveLocationFields } from "./location";
import { runFirecrawlCors } from "./middleware/firecrawlCors";
import { parseFirecrawlWebhook } from "./firecrawlWebhookUtil";
import { buildJobInsert } from "./jobRecords";
import {
  ashbySlugFromUrl,
  fallbackCompanyNameFromUrl,
  greenhouseSlugFromUrl,
  normalizeSiteUrl,
  siteCanonicalKey,
} from "./siteUtils";
import { SITE_TYPES, SPIDER_CLOUD_DEFAULT_SITE_TYPES, type SiteType } from "./siteTypes";
import { deriveCompanyKey, deriveEngineerFlag, matchesCompanyFilters } from "./jobs";

const http = httpRouter();
const SCRAPE_URL_QUEUE_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days
const JOB_DETAIL_MAX_ATTEMPTS = 3;
const DEFAULT_TIMEZONE = "America/Denver";
const UNKNOWN_COMPENSATION_REASON = "pending markdown structured extraction";
const HEURISTIC_VERSION = 4;
const JOB_BOARD_NAME = "JobBoard";
const JOB_BOARD_LOGO_PATH = "/share/jobboard-logo.svg";
const JOB_BOARD_LOGO_SVG = `<svg xmlns="http://www.w3.org/2000/svg" width="256" height="256" viewBox="0 0 256 256"><rect width="256" height="256" rx="56" fill="#0F172A"/><rect x="28" y="28" width="200" height="200" rx="44" fill="#111827"/><text x="50%" y="56%" text-anchor="middle" font-family="Arial, sans-serif" font-size="96" font-weight="700" fill="#34D399">JB</text></svg>`;
const BRAND_FETCH_CLIENT = "1idXaGHc5cKcElppzC7";
const BRANDFETCH_LOGO_OVERRIDES: Record<string, string> = {
  mithril: "https://cdn.brandfetch.io/idZPhPbkaC/w/432/h/432/theme/dark/logo.png?c=1bxid64Mup7aczewSAYMX&t=1759798646882",
  together: "https://cdn.brandfetch.io/idgEzjThpb/w/400/h/400/theme/dark/icon.jpeg?c=1bxid64Mup7aczewSAYMX&t=1764613007905",
  togetherai: "https://cdn.brandfetch.io/idgEzjThpb/w/400/h/400/theme/dark/icon.jpeg?c=1bxid64Mup7aczewSAYMX&t=1764613007905",
  togetherdotai: "https://cdn.brandfetch.io/idgEzjThpb/w/400/h/400/theme/dark/icon.jpeg?c=1bxid64Mup7aczewSAYMX&t=1764613007905",
};
const BRANDFETCH_DOMAIN_OVERRIDES: Record<string, string> = {
  oscar: "hioscar.com",
  serval: "serval.com",
};
const LOGO_SLUG_CHAR_MAP: Record<string, string> = {
  "+": "plus",
  ".": "dot",
  "&": "and",
  "đ": "d",
  "ħ": "h",
  "ı": "i",
  "ĸ": "k",
  "ŀ": "l",
  "ł": "l",
  "ß": "ss",
  "ŧ": "t",
  "ø": "o",
};
const literalSiteType = <T extends SiteType>(siteType: T) => v.literal(siteType);
const SITE_TYPE_VALIDATORS = SITE_TYPES.map(literalSiteType);
const COMMON_SUBDOMAIN_PREFIXES = new Set([
  "www",
  "jobs",
  "careers",
  "boards",
  "board",
  "apply",
  "app",
  "join",
  "team",
  "teams",
  "work",
]);
const RESERVED_PATH_SEGMENTS = new Set([
  "boards",
  "jobs",
  "careers",
  "jobdetail",
  "apply",
  "application",
  "applications",
  "openings",
  "positions",
  "roles",
  "role",
  "departments",
  "teams",
  "en",
  "en-us",
  "en-gb",
  "en-au",
  "v1",
  "v2",
  "api",
]);
const HOSTED_JOB_DOMAINS = [
  "avature.net",
  "avature.com",
  "searchjobs.com",
  "greenhouse.io",
  "ashbyhq.com",
  "lever.co",
  "workable.com",
  "smartrecruiters.com",
  "myworkdayjobs.com",
  "icims.com",
  "jobvite.com",
  "bamboohr.com",
];
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
const toLogoSlug = (company: string) => {
  const lowered = (company || "").toLowerCase();
  const replaced = lowered.replace(/[+.&đħıĸŀłßŧø]/g, (char) => LOGO_SLUG_CHAR_MAP[char] ?? "");
  const normalized = replaced.normalize("NFD").replace(/[^a-z0-9]/g, "");
  return normalized || null;
};
const extractCompanySlug = (pathname: string) => {
  const parts = pathname.split("/").filter(Boolean);
  for (const part of parts) {
    const cleaned = part.toLowerCase();
    if (cleaned === "jobdetail" || cleaned === "job-details" || cleaned === "jobdetails") {
      break;
    }
    if (RESERVED_PATH_SEGMENTS.has(cleaned)) continue;
    if (/^\d+$/.test(cleaned)) continue;
    if (!/^[a-z0-9-]+$/.test(cleaned)) continue;
    return cleaned;
  }
  return null;
};
const resolveHostedJobsDomain = (host: string) =>
  HOSTED_JOB_DOMAINS.find((domain) => host === domain || host.endsWith(`.${domain}`)) ?? null;
const extractCompanySlugFromHost = (host: string, hostedDomain: string) => {
  const hostParts = host.split(".").filter(Boolean);
  const domainParts = hostedDomain.split(".").filter(Boolean);
  if (hostParts.length <= domainParts.length) return null;
  const subdomains = hostParts.slice(0, hostParts.length - domainParts.length);
  for (let i = subdomains.length - 1; i >= 0; i -= 1) {
    const candidate = subdomains[i]?.toLowerCase() ?? "";
    if (!candidate) continue;
    if (COMMON_SUBDOMAIN_PREFIXES.has(candidate)) continue;
    if (!/^[a-z0-9-]+$/.test(candidate)) continue;
    return candidate;
  }
  return null;
};
const deriveBrandfetchDomain = (company: string, url?: string | null) => {
  const trimmedCompany = (company || "").trim();
  const companySlug = toLogoSlug(trimmedCompany);
  const domainOverride = companySlug ? BRANDFETCH_DOMAIN_OVERRIDES[companySlug] ?? null : null;
  if (domainOverride) {
    return domainOverride;
  }
  const fallbackCompanyDomain = () => {
    if (trimmedCompany.includes(".")) {
      return trimmedCompany.toLowerCase();
    }
    return companySlug ? `${companySlug}.com` : null;
  };
  if (url) {
    try {
      const parsed = new URL(url.includes("://") ? url : `https://${url}`);
      const host = parsed.hostname.toLowerCase();
      const hostedDomain = resolveHostedJobsDomain(host);
      if (hostedDomain) {
        const slugFromPath = extractCompanySlug(parsed.pathname);
        if (slugFromPath) {
          return `${slugFromPath}.com`;
        }
        const hostSlug = extractCompanySlugFromHost(host, hostedDomain);
        if (hostSlug) {
          return `${hostSlug}.com`;
        }
        const fallback = fallbackCompanyDomain();
        if (fallback) {
          return fallback;
        }
      }
      return baseDomainFromHost(host);
    } catch {
      // fall through to company fallback
    }
  }
  return fallbackCompanyDomain();
};
const resolveCompanyLogoUrl = (company: string, url: string | null | undefined, fallbackLogoUrl: string) => {
  const slug = toLogoSlug(company);
  if (slug && BRANDFETCH_LOGO_OVERRIDES[slug]) {
    return BRANDFETCH_LOGO_OVERRIDES[slug] as string;
  }
  const brandfetchDomain = deriveBrandfetchDomain(company, url);
  if (brandfetchDomain) {
    return `https://cdn.brandfetch.io/${brandfetchDomain}?c=${BRAND_FETCH_CLIENT}`;
  }
  if (slug) {
    return `https://cdn.simpleicons.org/${slug}`;
  }
  return fallbackLogoUrl;
};
const normalizeWhitespace = (value: string) => value.replace(/\s+/g, " ").trim();
const truncateText = (value: string, max = 220) => {
  const cleaned = normalizeWhitespace(value);
  if (cleaned.length <= max) return cleaned;
  const clipped = cleaned.slice(0, Math.max(0, max - 3)).trimEnd();
  const withoutPartial = clipped.replace(/\s+\S*$/, "");
  const base = withoutPartial || clipped;
  return `${base}...`;
};
const escapeHtml = (value: string) =>
  value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
const parseAppOrigin = (value: string | null) => {
  if (!value) return null;
  try {
    const parsed = new URL(value);
    if (parsed.protocol === "http:" || parsed.protocol === "https:") {
      return parsed.origin;
    }
  } catch {
    // ignore invalid app params
  }
  return null;
};
const buildShareDescription = (raw: string | null | undefined) => {
  const cleaned = stripEmbeddedJson(cleanScrapedText(raw));
  if (!cleaned) return "Job details available on JobBoard.";
  return truncateText(cleaned, 240);
};
const normalizeCompany = (value: string) => (value || "").toLowerCase().replace(/[^a-z0-9]/g, "");
const toTitleCaseSlug = (value: string) => {
  const cleaned = value.replace(/[^a-z0-9]+/gi, " ").trim();
  if (!cleaned) return "";
  return cleaned
    .split(" ")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
};
const isVersionLabel = (value: string) => /^v\d+$/i.test((value || "").trim());
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
const normalizeCompanyKey = (value?: string | null) => (value ?? "").trim().toLowerCase();
const isUnknownLabel = (value?: string | null) => {
  const normalized = (value ?? "").trim().toLowerCase();
  return (
    !normalized ||
    normalized === "unknown" ||
    normalized === "n/a" ||
    normalized === "na" ||
    normalized === "unspecified" ||
    normalized === "not available"
  );
};
const shouldReplaceText = (next?: string | null, prev?: string | null) => {
  const trimmedNext = (next ?? "").trim();
  if (!trimmedNext) return false;
  const nextLower = trimmedNext.toLowerCase();
  const prevLower = (prev ?? "").trim().toLowerCase();
  const nextUnknown = isUnknownLabel(trimmedNext) || nextLower === "untitled";
  const prevUnknown = isUnknownLabel(prevLower) || prevLower === "untitled";
  if (nextUnknown && !prevUnknown) return false;
  return trimmedNext !== (prev ?? "").trim();
};
const arraysEqual = (a?: unknown[] | null, b?: unknown[] | null) => JSON.stringify(a ?? []) === JSON.stringify(b ?? []);
const normalizeDomainInput = (value: string): string => {
  const trimmed = (value || "").trim();
  if (!trimmed) return "";

  try {
    const parsed = new URL(trimmed.includes("://") ? trimmed : `https://${trimmed}`);
    const host = parsed.hostname.toLowerCase();
    const greenhouseSlug = greenhouseSlugFromUrl(parsed.href);
    const greenhouse = greenhouseSlug ? `${greenhouseSlug}.greenhouse.io` : null;
    if (greenhouse) return greenhouse;
    const ashbySlug = ashbySlugFromUrl(parsed.href);
    const ashby = ashbySlug ? `${ashbySlug}.ashbyhq.com` : null;
    if (ashby) return ashby;
    return baseDomainFromHost(host);
  } catch {
    const hostOnly = trimmed.replace(/^https?:\/\//i, "").split("/")[0] || trimmed;
    const host = hostOnly.toLowerCase();
    const greenhouseSlug = greenhouseSlugFromUrl(host);
    const greenhouse = greenhouseSlug ? `${greenhouseSlug}.greenhouse.io` : null;
    if (greenhouse) return greenhouse;
    const ashbySlug = ashbySlugFromUrl(trimmed);
    const ashby = ashbySlug ? `${ashbySlug}.ashbyhq.com` : null;
    if (ashby) return ashby;
    return baseDomainFromHost(host);
  }
};
const deriveNameFromDomain = (domain: string): string => {
  if (!domain) return "Site";
  return fallbackCompanyName(undefined, `https://${domain}`);
};
const resolveCompanyFilterSet = async (ctx: any, input: string) => {
  const trimmed = (input ?? "").trim();
  if (!trimmed) {
    throw new Error("Company name is required.");
  }

  const inputKey = normalizeCompanyKey(trimmed);
  const profiles = await ctx.db.query("company_profiles").collect();
  let nameMatch: any = null;
  let aliasMatch: any = null;

  for (const profile of profiles as any[]) {
    const name = (profile?.name ?? "").trim();
    if (name && normalizeCompanyKey(name) === inputKey) {
      nameMatch = profile;
      break;
    }
    if (!aliasMatch && Array.isArray(profile?.aliases)) {
      if ((profile.aliases as any[]).some((alias) => normalizeCompanyKey(alias) === inputKey)) {
        aliasMatch = profile;
      }
    }
  }

  const matched = nameMatch ?? aliasMatch;
  const names = new Set<string>();
  if (matched) {
    if (typeof matched.name === "string" && matched.name.trim()) {
      names.add(matched.name.trim());
    }
    if (Array.isArray(matched.aliases)) {
      for (const alias of matched.aliases) {
        if (typeof alias === "string" && alias.trim()) {
          names.add(alias.trim());
        }
      }
    }
  }
  names.add(trimmed);

  const normalized = new Set<string>();
  for (const name of names) {
    const key = normalizeCompanyKey(name);
    if (key) normalized.add(key);
  }

  return {
    resolvedName: matched?.name ?? trimmed,
    names: Array.from(names),
    normalized,
  };
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
  const trimmedCurrent = (currentCompany ?? "").trim();
  const greenhouseSlug = greenhouseSlugFromUrl(url);
  const greenhouseName = greenhouseSlug ? toTitleCaseSlug(greenhouseSlug) : "";
  const safeCurrent = greenhouseSlug && isVersionLabel(trimmedCurrent) ? "" : trimmedCurrent;

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

  const chosen = alias ?? siteName ?? (safeCurrent || greenhouseName);
  return chosen?.trim() || fallbackCompanyName(safeCurrent, url);
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
    const seen = new Set<string | null>();
    const normalizeCursor = (value: any) => (value === null || value === undefined ? null : String(value));
    let pages = 0;
    const maxPages = 1000;
    while (true) {
      const { page, isDone, continueCursor } = await cursorable.paginate({ cursor, numItems: 200 });
      rows.push(...(page || []));
      if (isDone || !continueCursor) break;
      const nextKey = normalizeCursor(continueCursor);
      const currentKey = normalizeCursor(cursor);
      if (!page?.length || nextKey === currentKey || seen.has(nextKey) || pages >= maxPages) break;
      seen.add(nextKey);
      cursor = continueCursor;
      pages += 1;
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
    await ctx.db.patch(job._id, { company: next, companyKey: deriveCompanyKey(next) });
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
    await ctx.db.patch(job._id, { company: next, companyKey: deriveCompanyKey(next) });
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

http.route({
  path: JOB_BOARD_LOGO_PATH,
  method: "GET",
  handler: httpAction(async () => {
    return new Response(JOB_BOARD_LOGO_SVG, {
      status: 200,
      headers: {
        "Content-Type": "image/svg+xml",
        "Cache-Control": "public, max-age=86400",
      },
    });
  }),
});

http.route({
  path: "/share/job",
  method: "GET",
  handler: httpAction(async (ctx, request) => {
    const url = new URL(request.url);
    const jobId = (url.searchParams.get("id") ?? "").trim();
    if (!jobId) {
      return new Response("Missing job id.", { status: 400, headers: { "Content-Type": "text/plain" } });
    }

    const job = await ctx.runQuery(api.jobs.getJobById, { id: jobId as Id<"jobs"> });
    if (!job) {
      return new Response("Job not found.", { status: 404, headers: { "Content-Type": "text/plain" } });
    }

    const companyName = (job.company ?? "Unknown company").trim() || "Unknown company";
    const jobTitle = (job.title ?? "Job details").trim() || "Job details";
    const shareTitle = companyName ? `${jobTitle} at ${companyName}` : jobTitle;
    const shortDescription = buildShareDescription(job.description);
    const jobBoardLogoUrl = new URL(JOB_BOARD_LOGO_PATH, url).toString();
    const companyLogoUrl = resolveCompanyLogoUrl(companyName, job.url ?? null, jobBoardLogoUrl);
    const oembedUrl = new URL("/share/job/oembed", url);
    oembedUrl.searchParams.set("id", jobId);
    const appParam = url.searchParams.get("app");
    if (appParam) {
      oembedUrl.searchParams.set("app", appParam);
    }
    const appOrigin = parseAppOrigin(appParam);
    const openInAppUrl = appOrigin ? `${appOrigin}/#job-details-${jobId}` : null;
    const shareUrl = url.toString();

    const metaParts = [
      job.location ? String(job.location) : null,
      job.remote === true ? "Remote" : job.remote === false ? "On-site" : null,
      job.level ? `Level: ${job.level}` : null,
    ].filter(Boolean) as string[];
    const metaLine = metaParts.length ? metaParts.join(" • ") : "Job details";

    const html = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>${escapeHtml(shareTitle)} | ${JOB_BOARD_NAME}</title>
    <meta name="description" content="${escapeHtml(shortDescription)}">
    <meta property="og:type" content="website">
    <meta property="og:site_name" content="${JOB_BOARD_NAME}">
    <meta property="og:title" content="${escapeHtml(shareTitle)}">
    <meta property="og:description" content="${escapeHtml(shortDescription)}">
    <meta property="og:image" content="${escapeHtml(companyLogoUrl)}">
    <meta property="og:image:alt" content="${escapeHtml(`${companyName} logo`)}">
    <meta property="og:url" content="${escapeHtml(shareUrl)}">
    <meta name="twitter:card" content="summary_large_image">
    <meta name="twitter:title" content="${escapeHtml(shareTitle)}">
    <meta name="twitter:description" content="${escapeHtml(shortDescription)}">
    <meta name="twitter:image" content="${escapeHtml(companyLogoUrl)}">
    <link rel="alternate" type="application/json+oembed" href="${escapeHtml(oembedUrl.toString())}" title="${escapeHtml(shareTitle)}">
    <link rel="icon" href="${escapeHtml(jobBoardLogoUrl)}" type="image/svg+xml">
    <style>
      :root { color-scheme: dark; }
      body { margin: 0; font-family: "Segoe UI", "Helvetica Neue", Arial, sans-serif; background: #0b1220; color: #e2e8f0; }
      .page { min-height: 100vh; display: flex; flex-direction: column; align-items: center; padding: 32px 16px 48px; gap: 20px; }
      .brand { display: flex; align-items: center; gap: 10px; font-weight: 700; letter-spacing: 0.04em; text-transform: uppercase; font-size: 12px; color: #94a3b8; }
      .brand img { width: 28px; height: 28px; border-radius: 8px; }
      .card { width: min(720px, 100%); background: #111827; border: 1px solid #1f2937; border-radius: 20px; padding: 24px; box-shadow: 0 20px 60px rgba(15, 23, 42, 0.35); }
      .header { display: flex; gap: 16px; align-items: center; }
      .header img { width: 72px; height: 72px; border-radius: 16px; background: #0f172a; border: 1px solid #1f2937; object-fit: contain; }
      .title { margin: 0; font-size: 24px; font-weight: 700; color: #f8fafc; }
      .company { margin: 4px 0 0; font-size: 14px; font-weight: 600; color: #38bdf8; }
      .meta { margin-top: 6px; font-size: 12px; color: #94a3b8; }
      .description { margin: 18px 0; font-size: 15px; line-height: 1.6; color: #cbd5f5; }
      .actions { display: flex; flex-wrap: wrap; gap: 12px; }
      .btn { display: inline-flex; align-items: center; justify-content: center; padding: 10px 16px; border-radius: 999px; background: #34d399; color: #0f172a; font-weight: 700; text-decoration: none; font-size: 13px; }
      .btn.secondary { background: transparent; color: #93c5fd; border: 1px solid #334155; }
      .footer { font-size: 11px; text-transform: uppercase; letter-spacing: 0.2em; color: #64748b; }
    </style>
  </head>
  <body>
    <div class="page">
      <div class="brand">
        <img src="${escapeHtml(jobBoardLogoUrl)}" alt="${JOB_BOARD_NAME} logo">
        <span>${JOB_BOARD_NAME}</span>
      </div>
      <div class="card">
        <div class="header">
          <img src="${escapeHtml(companyLogoUrl)}" alt="${escapeHtml(`${companyName} logo`)}">
          <div>
            <p class="company">${escapeHtml(companyName)}</p>
            <h1 class="title">${escapeHtml(jobTitle)}</h1>
            <div class="meta">${escapeHtml(metaLine)}</div>
          </div>
        </div>
        <p class="description">${escapeHtml(shortDescription)}</p>
        <div class="actions">
          ${openInAppUrl ? `<a class="btn" href="${escapeHtml(openInAppUrl)}" target="_blank" rel="noreferrer">Open in JobBoard</a>` : ""}
          ${job.url ? `<a class="btn secondary" href="${escapeHtml(job.url)}" target="_blank" rel="noreferrer">View job source</a>` : ""}
        </div>
      </div>
      <div class="footer">Shared via ${JOB_BOARD_NAME}</div>
    </div>
  </body>
</html>`;

    return new Response(html, {
      status: 200,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }),
});

http.route({
  path: "/share/job/oembed",
  method: "GET",
  handler: httpAction(async (ctx, request) => {
    const url = new URL(request.url);
    const jobId = (url.searchParams.get("id") ?? "").trim();
    if (!jobId) {
      return new Response(JSON.stringify({ error: "Missing job id." }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }

    const job = await ctx.runQuery(api.jobs.getJobById, { id: jobId as Id<"jobs"> });
    if (!job) {
      return new Response(JSON.stringify({ error: "Job not found." }), {
        status: 404,
        headers: { "Content-Type": "application/json" },
      });
    }

    const companyName = (job.company ?? "Unknown company").trim() || "Unknown company";
    const jobTitle = (job.title ?? "Job details").trim() || "Job details";
    const shareTitle = companyName ? `${jobTitle} at ${companyName}` : jobTitle;
    const shortDescription = buildShareDescription(job.description);
    const jobBoardLogoUrl = new URL(JOB_BOARD_LOGO_PATH, url).toString();
    const companyLogoUrl = resolveCompanyLogoUrl(companyName, job.url ?? null, jobBoardLogoUrl);
    const appOrigin = parseAppOrigin(url.searchParams.get("app"));

    const payload: Record<string, unknown> = {
      version: "1.0",
      type: "link",
      title: shareTitle,
      provider_name: JOB_BOARD_NAME,
      provider_url: appOrigin ?? url.origin,
      author_name: companyName,
      author_url: job.url ?? undefined,
      thumbnail_url: companyLogoUrl,
      thumbnail_width: 256,
      thumbnail_height: 256,
      description: shortDescription,
    };

    return new Response(JSON.stringify(payload), {
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

const recordSeenJobUrl = async (ctx: any, sourceUrl?: string, url?: string) => {
  const cleanedSource = (sourceUrl ?? "").trim();
  const cleanedUrl = (url ?? "").trim();
  if (!cleanedSource || !cleanedUrl) return;

  const existing = await ctx.db
    .query("seen_job_urls")
    .withIndex("by_source_url", (q: any) => q.eq("sourceUrl", cleanedSource).eq("url", cleanedUrl))
    .first();
  if (existing) return;

  await ctx.db.insert("seen_job_urls", {
    sourceUrl: cleanedSource,
    url: cleanedUrl,
    createdAt: Date.now(),
  });
};

// Gather previously seen job URLs for a site (from seen + ignored) so scrapers can skip them
export const listSeenJobUrlsForSite = query({
  args: {
    sourceUrl: v.string(),
    pattern: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    const seen = new Set<string>();

    const rows = await ctx.db
      .query("seen_job_urls")
      .withIndex("by_source", (q: any) => q.eq("sourceUrl", args.sourceUrl))
      .collect();
    for (const row of rows as any[]) {
      const url = (row).url;
      if (typeof url === "string") {
        seen.add(url);
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
    siteType: v.optional(
      v.union(...SITE_TYPE_VALIDATORS)
    ),
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
        (SPIDER_CLOUD_DEFAULT_SITE_TYPES.has(siteType as SiteType) ? "spidercloud" : "fetchfox");
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
    try {
      const pendingRequest = await ctx.db
        .query("run_requests")
        .withIndex("by_site_status_created", (q) => q.eq("siteId", pick._id).eq("status", "pending"))
        .order("desc")
        .first();
      if (pendingRequest) {
        await ctx.db.patch(pendingRequest._id, {
          status: "processing",
          createdAt: now,
          expectedEta: undefined,
        });
      }
    } catch (err) {
      console.error("leaseSite: failed to update run_request status", err);
    }
    // Return minimal fields for the worker
    const fresh = await ctx.db.get(pick._id as Id<"sites">);
    if (!fresh) return null;
    const s = fresh;
    const resolvedProvider =
      (s as any).scrapeProvider ??
      ((s as any).type === "greenhouse" ||
      (s as any).type === "avature" ||
      (s as any).type === "workday" ||
      (s as any).type === "netflix"
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
    const now = Date.now();
    await recordSeenJobUrl(ctx, args.sourceUrl, args.url);
    return await ctx.db.insert("ignored_jobs", {
      url: args.url,
      sourceUrl: args.sourceUrl,
      reason: args.reason,
      provider: args.provider,
      workflowName: args.workflowName,
      details: args.details,
      title: args.title,
      description: args.description,
      createdAt: now,
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
      v.union(
        v.literal("pending"),
        v.literal("processing"),
        v.literal("completed"),
        v.literal("failed"),
        v.literal("invalid")
      ),
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
      const statuses: Array<"pending" | "processing" | "completed" | "failed" | "invalid"> = [
        "pending",
        "processing",
        "completed",
        "failed",
        "invalid",
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
        scheduledAt: row.scheduledAt,
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
    delaysMs: v.optional(v.array(v.number())),
  },
  handler: async (ctx, args) => {
    const now = Date.now();
    const queued: string[] = [];
    const seen = new Set<string>();

    for (const [index, rawUrl] of args.urls.entries()) {
      const url = (rawUrl || "").trim();
      if (!url || seen.has(url)) continue;
      seen.add(url);
      const delayMs = args.delaysMs?.[index];
      const scheduledAt =
        typeof delayMs === "number" && Number.isFinite(delayMs) && delayMs > 0
          ? now + Math.floor(delayMs)
          : now;

      // Skip if already queued
      const existing = await ctx.db
        .query("scrape_url_queue")
        .withIndex("by_url", (q) => q.eq("url", url))
        .first();
      if (existing) {
        const createdAt = (existing as any).createdAt ?? 0;
        const updatedAt = (existing as any).updatedAt ?? createdAt;
        const status = (existing as any).status as string | undefined;
        const isStale =
          (createdAt && createdAt < now - SCRAPE_URL_QUEUE_TTL_MS) ||
          (updatedAt && updatedAt < now - SCRAPE_URL_QUEUE_TTL_MS);
        const shouldRequeue =
          isStale || status === "failed" || status === "completed" || status === "invalid";

        if (shouldRequeue) {
          await ctx.db.patch(existing._id, {
            sourceUrl: args.sourceUrl,
            provider: args.provider,
            siteId: args.siteId,
            pattern: args.pattern === null ? undefined : args.pattern,
            status: "pending",
            attempts: 0,
            lastError: undefined,
            completedAt: undefined,
            updatedAt: now,
            scheduledAt,
          });
          queued.push(url);
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
        scheduledAt,
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

  const baseQuery = ctx.db
    .query("scrape_url_queue")
    .withIndex("by_status_and_scheduled_at", (q: any) => q.eq("status", "pending").lte("scheduledAt", now));
  let rows = await baseQuery.order("asc").take(limit * 3);
  if (rows.length < limit) {
    const legacyRows = await ctx.db
      .query("scrape_url_queue")
      .withIndex("by_status", (q: any) => q.eq("status", "pending"))
      .order("asc")
      .take(limit * 2);
    const seenIds = new Set(rows.map((row: any) => row._id));
    for (const row of legacyRows as any[]) {
      if (!row.scheduledAt && !seenIds.has(row._id)) {
        rows.push(row);
      }
      if (rows.length >= limit * 3) break;
    }
  }
  const picked: any[] = [];
  for (const row of rows as any[]) {
    if (picked.length >= limit) break;
    if (args.provider && row.provider !== args.provider) continue;
    const createdAt = (row).createdAt ?? 0;
    if (createdAt && createdAt < now - SCRAPE_URL_QUEUE_TTL_MS) {
      // Skip stale (>7d) entries; mark ignored
      await ctx.db.patch(row._id, {
        status: "failed",
        lastError: "stale (>7d)",
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
    const scheduledAt = (row).scheduledAt ?? 0;
    if (scheduledAt && scheduledAt > now) {
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
  args: { urls: string[]; status: "completed" | "failed" | "invalid"; error?: string }
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
    await recordSeenJobUrl(ctx, (existing).sourceUrl, url);

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
      status: v.union(v.literal("completed"), v.literal("failed"), v.literal("invalid")),
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
    metadata?: string;
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
  for (const key of ["description", "metadata", "heuristicAttempts", "heuristicLastTried", "heuristicVersion"] as const) {
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
      metadata: v.optional(v.string()),
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
    status: v.optional(v.union(v.literal("completed"), v.literal("failed"), v.literal("invalid"))),
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
        lastError: status === "failed" || status === "invalid" ? undefined : row.lastError,
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
  args: { limit: v.optional(v.number()), status: v.optional(v.union(v.literal("pending"), v.literal("processing"), v.literal("done"))) },
  handler: async (ctx, args) => {
    const lim = Math.max(1, Math.min(args.limit ?? 50, 200));
    const rows = await (args.status
      ? ctx.db
        .query("run_requests")
        .withIndex("by_status_created", (q) => q.eq("status", args.status!).gte("createdAt", 0))
        .order("desc")
        .take(lim)
      : ctx.db
        .query("run_requests")
        .withIndex("by_created", (q) => q.gte("createdAt", 0))
        .order("desc")
        .take(lim));
    const aliasCache = new Map<string, string | null>();
    const results = [];
    for (const row of rows as any[]) {
      const site = await ctx.db.get(row.siteId);
      const siteUrl = row.siteUrl || (site as any)?.url || "";
      const siteName = (site as any)?.name || "";
      const companyName = siteUrl
        ? await resolveCompanyForUrl(ctx, siteUrl, siteName, siteName, aliasCache)
        : fallbackCompanyName(siteName, siteUrl);
      results.push({
        ...row,
        siteUrl,
        companyName,
      });
    }
    return results;
  },
});

export const resetActiveSites = mutation({
  args: { respectSchedule: v.optional(v.boolean()) },
  handler: async (ctx, args) => {
    const now = Date.now();
    const respectSchedule = args.respectSchedule ?? false;
    const sites = await ctx.db
      .query("sites")
      .withIndex("by_enabled", (q) => q.eq("enabled", true))
      .collect();

    for (const site of sites as any[]) {
      const patch: Record<string, any> = {
        completed: false,
        failed: false,
        lockedBy: "",
        lockExpiresAt: 0,
        lastFailureAt: undefined,
        lastError: undefined,
      };

      if (!respectSchedule) {
        patch.lastRunAt = 0;
        patch.manualTriggerAt = now;
      }

      await ctx.db.patch(site._id, patch);

      if (!respectSchedule) {
        try {
          await ctx.db.insert("run_requests", {
            siteId: site._id,
            siteUrl: site.url ?? "",
            status: "pending",
            createdAt: now,
            expectedEta: now + 15_000,
            completedAt: undefined,
          });
        } catch (err) {
          console.error("resetActiveSites: failed to record run_request", err);
        }
      }
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
  handler: httpAction(async (ctx, request) => {
    let body: any = {};
    try {
      body = await request.json();
    } catch {
      body = {};
    }
    const res = await ctx.runMutation(api.router.resetActiveSites, {
      respectSchedule: body?.respectSchedule ?? false,
    });
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
    type: v.optional(
      v.union(...SITE_TYPE_VALIDATORS)
    ),
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
      (SPIDER_CLOUD_DEFAULT_SITE_TYPES.has(siteType as SiteType) ? "spidercloud" : "fetchfox");
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
        type: v.optional(
          v.union(...SITE_TYPE_VALIDATORS)
        ),
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
        (SPIDER_CLOUD_DEFAULT_SITE_TYPES.has(siteType as SiteType) ? "spidercloud" : "fetchfox");
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
    engineer: v.optional(v.boolean()),
  },
  handler: async (ctx, args) => {
    const resolvedCompany = await resolveCompanyForUrl(ctx, args.url, args.company);
    const { description, ...jobArgs } = args;
    const engineer = typeof args.engineer === "boolean" ? args.engineer : deriveEngineerFlag(args.title);
    const jobId = await ctx.db.insert(
      "jobs",
      buildJobInsert({
        ...jobArgs,
        engineer,
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
          sourceUrl: body.sourceUrl,
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
        metadata: v.optional(v.string()),
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
        engineer: v.optional(v.boolean()),
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
    let sourceUrlForSeen: string | undefined;
    if (args.siteId) {
      const site = await ctx.db.get(args.siteId);
      if (site && typeof (site as any).name === "string") {
        companyOverride = (site as any).name;
      }
      if (site && typeof (site as any).url === "string") {
        sourceUrlForSeen = (site as any).url;
      }
    }
    const aliasCache = new Map<string, string | null>();

    let inserted = 0;
    for (const job of args.jobs) {
      if (sourceUrlForSeen) {
        await recordSeenJobUrl(ctx, sourceUrlForSeen, job.url);
      }
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
        metadata,
        scrapedWith,
        workflowName,
        scrapedCostMilliCents,
        heuristicAttempts,
        heuristicLastTried,
        heuristicVersion,
        engineer: jobEngineer,
        ...jobFields
      } = job;
      const engineer = typeof jobEngineer === "boolean" ? jobEngineer : deriveEngineerFlag(job.title);
      const jobId = await ctx.db.insert("jobs", {
        ...jobFields,
        engineer,
        company: resolvedCompany,
        companyKey: deriveCompanyKey(resolvedCompany),
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
      if (metadata !== undefined) detailRow.metadata = metadata;
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

const stripEmbeddedJson = (value: string): string => {
  const markers = ["\"themeOptions\"", "\"customTheme\"", "\"varTheme\"", "\"micrositeConfig\""];
  let output = value;
  for (let pass = 0; pass < 3; pass += 1) {
    const markerIndex = markers.reduce((idx, marker) => {
      if (idx !== -1) return idx;
      return output.indexOf(marker);
    }, -1);
    if (markerIndex === -1) break;
    const start = output.lastIndexOf("{", markerIndex);
    if (start === -1) break;
    let depth = 0;
    let inString = false;
    let escaped = false;
    let end = -1;
    for (let i = start; i < output.length; i += 1) {
      const char = output[i];
      if (inString) {
        if (escaped) {
          escaped = false;
        } else if (char === "\\") {
          escaped = true;
        } else if (char === "\"") {
          inString = false;
        }
        continue;
      }
      if (char === "\"") {
        inString = true;
        continue;
      }
      if (char === "{") depth += 1;
      if (char === "}") {
        depth -= 1;
        if (depth === 0) {
          end = i;
          break;
        }
      }
    }
    if (end === -1) break;
    output = `${output.slice(0, start)} ${output.slice(end + 1)}`;
  }
  return output.replace(/\s+/g, " ").trim();
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

const TITLE_PLACEHOLDERS = new Set(["page_title", "title", "job_title", "untitled", "unknown", "application"]);
const NOISY_TITLE_PATTERNS = [/apply with ai/i, /direct apply/i, /select an option/i, /automated source picker/i];

const looksLikeUuidish = (value: string) => {
  const compact = value.replace(/[\s-]/g, "");
  return /^[0-9a-f]{32}$/i.test(compact);
};

const looksLikeNumericId = (value: string) => /^\d{3,}$/.test(value);

const looksLikeNoisyTitle = (value: string) => {
  const trimmed = (value ?? "").trim();
  if (!trimmed) return true;
  const lowered = trimmed.toLowerCase();
  if (TITLE_PLACEHOLDERS.has(lowered)) return true;
  if (NOISY_TITLE_PATTERNS.some((pattern) => pattern.test(lowered))) return true;
  if (/https?:\/\//i.test(trimmed)) return true;
  if (/\\{3,}/.test(trimmed) || /\/{6,}/.test(trimmed)) return true;
  const wordCount = trimmed.split(/\s+/).length;
  if (wordCount >= 8 && (lowered.includes("posted") || lowered.includes("apply"))) return true;
  return false;
};

const LIST_ITEM_RE = /^([-*]|\u2022|\d+[.)])\s+/;

const looksLikeSentenceLine = (value: string) => {
  const trimmed = value.trim();
  if (!trimmed) return false;
  const words = trimmed.split(/\s+/);
  if (words.length >= 15) return true;
  if (/[.!?]$/.test(trimmed) && words.length >= 8) return true;
  return false;
};

const extractTitleFromListingBlob = (raw: unknown): string | null => {
  if (typeof raw !== "string") return null;
  if (raw.length < 200) return null;
  if (!/\bdescription\b/i.test(raw) || !/\bposted\b/i.test(raw)) return null;

  const lines = raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length) return null;

  const descriptionIndex = lines.findIndex((line) => /^description\b/i.test(line));
  const startIndex = descriptionIndex >= 0 ? descriptionIndex + 1 : Math.floor(lines.length * 0.6);

  for (let i = startIndex; i < lines.length; i += 1) {
    const line = lines[i];
    const lower = line.toLowerCase();
    if (lower.startsWith("description")) continue;
    if (lower.startsWith("posted ") || lower.startsWith("direct apply") || lower.startsWith("apply with")) continue;
    if (/\bwords\b/.test(lower) && /\d/.test(lower)) continue;
    if (/^https?:\/\//i.test(line)) continue;
    if (/:$/.test(line)) continue;
    if (LIST_ITEM_RE.test(line)) continue;
    if (looksLikeSentenceLine(line)) continue;

    let cleaned = cleanScrapedText(line);
    if (!cleaned) continue;
    if (cleaned.length > 140) continue;
    if (looksLikeNoisyTitle(cleaned)) continue;
    if (cleaned.split(/\s+/).length < 2) continue;

    const atMatch = cleaned.match(/^(.+?)\s+@\s+.+$/);
    if (atMatch) cleaned = atMatch[1].trim();
    const dashMatch = cleaned.match(/^(.+?)\s+[-\u2013\u2014]\s+(.+)$/);
    if (dashMatch) {
      const suffix = dashMatch[2].trim();
      const suffixLower = suffix.toLowerCase();
      const stripSuffix =
        /\b(remote|hybrid|on[- ]?site|anywhere)\b/.test(suffixLower) ||
        /\b(usa|us|u\.s\.|uk|u\.k\.|eu|emea|apac|latam)\b/.test(suffixLower) ||
        /\b(inc|llc|ltd|corp|co|company|plc|gmbh|s\.a\.|sarl|pte|pty)\b/i.test(suffix) ||
        /\b[A-Za-z .'-]+,\s*[A-Z]{2}\b/.test(suffix);
      if (stripSuffix) cleaned = dashMatch[1].trim();
    }
    if (cleaned && cleaned.split(/\s+/).length >= 2) return cleaned;
  }

  if (lines.length <= 1) {
    const normalized = raw.replace(/\s+/g, " ").trim();
    const parts = normalized.split(/\bdescription\b/i);
    const tail = parts[parts.length - 1] ?? "";
    let candidate = tail.replace(/^\s*\d+\s*words\b/i, "").trim();
    if (candidate) {
      const atMatch = candidate.match(/^(.+?)\s+@\s+.+$/);
      if (atMatch) candidate = atMatch[1].trim();
      const dashMatch = candidate.match(/^(.+?)\s+[-\u2013\u2014]\s+.+$/);
      if (dashMatch) candidate = dashMatch[1].trim();
      candidate = cleanScrapedText(candidate);
      if (
        candidate &&
        candidate.length <= 140 &&
        !looksLikeNoisyTitle(candidate) &&
        candidate.split(/\s+/).length >= 2
      ) {
        return candidate;
      }
    }
  }

  return null;
};

const parseUrlSafe = (value: string, base?: string): URL | null => {
  try {
    return new URL(value);
  } catch {
    if (!base) return null;
  }
  try {
    return new URL(value, base);
  } catch {
    return null;
  }
};

const isAshbyHost = (host: string) => host.endsWith("ashbyhq.com");
const isAvatureHost = (host: string) => host.endsWith("avature.net") || host.endsWith("avature.com");

const normalizeScrapedUrl = (rawUrl: string, sourceUrl?: string): string | null => {
  if (typeof rawUrl !== "string") return null;
  let cleaned = rawUrl.trim();
  if (!cleaned) return null;
  cleaned = cleaned.replace(/\\+/g, "/");
  const parsed = parseUrlSafe(cleaned, sourceUrl);
  if (!parsed || !parsed.hostname) return null;

  const host = parsed.hostname.toLowerCase();
  let path = parsed.pathname || "/";
  path = path.replace(/\/{2,}/g, "/");
  if (path.length > 1) path = path.replace(/\/+$/, "");
  if (isAshbyHost(host) && path.toLowerCase().endsWith("/application")) {
    path = path.slice(0, -"/application".length) || "/";
  }
  if (path.length > 1) path = path.replace(/\/+$/, "");

  if (isAvatureHost(host) && /\/(savejob|searchjobs|jobsearch)/i.test(path)) {
    return null;
  }

  if (sourceUrl) {
    const sourceParsed = parseUrlSafe(sourceUrl);
    const sourceHost = sourceParsed?.hostname?.toLowerCase() ?? "";
    const enforceMatch = isAshbyHost(sourceHost) || isAvatureHost(sourceHost);
    if (enforceMatch && sourceHost && host !== sourceHost) return null;
  }

  parsed.pathname = path;
  parsed.hash = "";
  let normalized = parsed.toString();
  if (path !== "/" && normalized.endsWith("/")) normalized = normalized.slice(0, -1);
  return normalized;
};

const titleFromSlug = (value: string) => {
  if (!value) return "";
  let decoded = value;
  try {
    decoded = decodeURIComponent(value);
  } catch {
    decoded = value;
  }
  return decoded.replace(/[-_]+/g, " ").replace(/\s+/g, " ").trim();
};

const deriveTitleFromUrl = (url: string): string | null => {
  const parsed = parseUrlSafe(url);
  if (!parsed) return null;
  const host = parsed.hostname.toLowerCase();
  const segments = parsed.pathname.split("/").filter(Boolean);
  if (!segments.length) return null;
  if (isAvatureHost(host)) {
    const jobDetailIndex = segments.findIndex((segment) => segment.toLowerCase() === "jobdetail");
    if (jobDetailIndex >= 0 && segments[jobDetailIndex + 1]) {
      const title = titleFromSlug(segments[jobDetailIndex + 1]);
      return title || null;
    }
  }
  const last = segments[segments.length - 1];
  if (!last || /^(application|savejob|searchjobs|jobsearch)$/i.test(last)) return null;
  const candidate = titleFromSlug(last);
  if (!candidate || looksLikeUuidish(candidate) || looksLikeNumericId(candidate)) return null;
  return candidate;
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
    sourceUrl?: string;
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
  city?: string;
  state?: string;
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
  if (options?.sourceUrl && looksLikeListingUrl(options.sourceUrl)) {
    const normalizedSource = normalizeUrlKey(options.sourceUrl);
    if (normalizedSource) seedUrlKeys.add(normalizedSource);
  }

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
          ? row.job_title ??
            row.title ??
            row.jobTitle ??
            row.position_title ??
            row.positionTitle ??
            row.posting_title ??
            row.heading ??
            row.position ??
            row.name ??
            row.role ??
            row.jobName
          : undefined) ?? row;

      const rawUrl = String(row?.url || row?.link || row?.href || row?.absolute_url || "").trim();
      const normalizedUrl = normalizeScrapedUrl(rawUrl, options?.sourceUrl);
      if (!normalizedUrl) return null;

      const hintedTitle = extractTitleFromListingBlob(rawTitle);
      let title = normalizeTitle(hintedTitle ?? rawTitle);
      if (looksLikeNoisyTitle(title)) {
        const fromUrl = deriveTitleFromUrl(normalizedUrl);
        if (fromUrl && !looksLikeNoisyTitle(fromUrl)) {
          title = fromUrl;
        } else {
          return null;
        }
      }

      const parsedUrl = parseUrlSafe(normalizedUrl);
      const ashbySlug = parsedUrl && isAshbyHost(parsedUrl.hostname.toLowerCase()) ? ashbySlugFromUrl(normalizedUrl) : null;
      const ashbyCompany = ashbySlug ? toTitleCaseSlug(ashbySlug) : null;

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
      let company = rawCompany || fallbackCompanyName(rawCompany, normalizedUrl);
      if (ashbyCompany) {
        const normalizedCompany = (company || "").toLowerCase().replace(/[^a-z0-9]/g, "");
        if (!company || normalizedCompany === "ashbyhq" || normalizedCompany === "ashby") {
          company = ashbyCompany;
        }
      }
      const locationLabel = formatLocationLabel(city, state, location);
      const remote = coerceBool(row.remote, locationLabel, title);
      const descriptionRaw =
        typeof row?.description === "string"
          ? cleanScrapedText(row.description)
          : typeof row?.content === "string"
            ? cleanScrapedText(row.content)
            : typeof row === "string"
              ? cleanScrapedText(row)
              : JSON.stringify(row, null, 2).slice(0, 4000);
      const description = stripEmbeddedJson(descriptionRaw);
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
        url: normalizedUrl,
        postedAt,
      };
    })
    .filter((j): j is NonNullable<typeof j> => Boolean(j)); // require a URL + title to keep signal

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

export const reparseRecentCompanyJobs = mutation({
  args: {
    company: v.string(),
    lookbackHours: v.optional(v.number()),
    jobLimit: v.optional(v.number()),
    scrapeLimit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const now = Date.now();
    const lookbackHours = Math.max(0.1, args.lookbackHours ?? 24);
    const jobLimit = Math.max(1, Math.min(args.jobLimit ?? 500, 5000));
    const scrapeLimit = Math.max(1, Math.min(args.scrapeLimit ?? 500, 5000));
    const cutoff = now - lookbackHours * 60 * 60 * 1000;

    const { resolvedName, names, normalized } = await resolveCompanyFilterSet(ctx, args.company);
    if (!normalized.size) {
      throw new Error("Unable to resolve company filter.");
    }

    const aliasRows = await ctx.db.query("domain_aliases").collect();
    const domainAliasLookup = new Map<string, string>();
    for (const row of aliasRows as any[]) {
      const domain = (row)?.domain?.trim?.() ?? "";
      const alias = normalizeCompanyKey((row)?.alias ?? "");
      if (domain && alias) {
        domainAliasLookup.set(domain, alias);
      }
    }

    const recentJobs = await ctx.db
      .query("jobs")
      .withIndex("by_scraped_at", (q: any) => q.gte("scrapedAt", cutoff))
      .order("desc")
      .take(jobLimit);

    const companyJobs = (recentJobs as any[]).filter((job) =>
      matchesCompanyFilters(job, normalized, domainAliasLookup)
    );

    const targetJobsByUrl = new Map<string, any>();
    for (const job of companyJobs) {
      const key = normalizeUrlKey((job).url);
      if (!key) continue;
      targetJobsByUrl.set(key, job);
    }

    if (targetJobsByUrl.size === 0) {
      return {
        companyInput: args.company,
        companyResolved: resolvedName,
        companyAliases: names,
        lookbackHours,
        jobLimit,
        scrapeLimit,
        jobsScanned: recentJobs.length,
        jobsMatched: 0,
        jobsWithScrape: 0,
        jobsUpdated: 0,
        jobDetailsUpdated: 0,
        jobsSkippedNoScrape: 0,
        scrapesScanned: 0,
      };
    }

    const scrapes = await ctx.db
      .query("scrapes")
      .withIndex("by_completedAt", (q: any) => q.gte("completedAt", cutoff))
      .order("desc")
      .take(scrapeLimit);

    const matchedByUrl = new Map<
      string,
      { job: any; parsed: ReturnType<typeof extractJobs>[number]; scrape: any; scrapeTime: number }
    >();
    const scrapeJobCounts = new Map<string, number>();

    for (const scrape of scrapes as any[]) {
      const parsedJobs = extractJobs(scrape.items, { sourceUrl: scrape.sourceUrl });
      const scrapeId = (scrape as any)._id;
      if (scrapeId) {
        scrapeJobCounts.set(String(scrapeId), parsedJobs.length);
      }
      if (parsedJobs.length === 0) continue;

      const scrapeTime = scrape.completedAt ?? scrape.startedAt ?? scrape._creationTime ?? now;
      for (const parsed of parsedJobs) {
        const key = normalizeUrlKey(parsed.url);
        if (!key || !targetJobsByUrl.has(key)) continue;
        const existing = matchedByUrl.get(key);
        if (!existing || scrapeTime > existing.scrapeTime) {
          matchedByUrl.set(key, { job: targetJobsByUrl.get(key), parsed, scrape, scrapeTime });
        }
      }

      if (matchedByUrl.size === targetJobsByUrl.size) {
        break;
      }
    }

    const aliasCache = new Map<string, string | null>();
    let jobsUpdated = 0;
    let jobDetailsUpdated = 0;
    let jobsSkippedNoScrape = 0;

    for (const [urlKey, job] of targetJobsByUrl.entries()) {
      const matched = matchedByUrl.get(urlKey);
      if (!matched) {
        jobsSkippedNoScrape += 1;
        continue;
      }

      const { parsed, scrape } = matched;
      const locationSeed = [parsed.location];
      const locationInfo = deriveLocationFields({ locations: locationSeed, location: parsed.location });
      const { city: derivedCity, state: derivedState } = splitLocation(
        parsed.city ?? parsed.state ? `${parsed.city ?? ""}, ${parsed.state ?? ""}` : locationInfo.primaryLocation
      );
      const city = parsed.city ?? derivedCity;
      const state = parsed.state ?? derivedState;
      const locationLabel = formatLocationLabel(city, state, locationInfo.primaryLocation);

      const scrapedWith = scrape.provider ?? scrape.items?.provider;
      const compensationUnknown = parsed.compensationUnknown === true;
      const compensationReason =
        typeof parsed.compensationReason === "string" && parsed.compensationReason.trim()
          ? parsed.compensationReason.trim()
          : compensationUnknown
            ? UNKNOWN_COMPENSATION_REASON
            : scrapedWith
              ? `${scrapedWith} extracted compensation`
              : "compensation provided in scrape payload";
      const resolvedCompany = await resolveCompanyForUrl(ctx, parsed.url, parsed.company, undefined, aliasCache);

      const patch: Record<string, any> = {};

      if (shouldReplaceText(parsed.title, job.title)) {
        patch.title = parsed.title;
      }
      if (shouldReplaceText(resolvedCompany, job.company)) {
        patch.company = resolvedCompany;
      }

      const shouldUpdateLocation = !isUnknownLabel(locationLabel) || isUnknownLabel(job.location);
      if (shouldUpdateLocation) {
        if (locationLabel && locationLabel !== job.location) patch.location = locationLabel;
        if (!arraysEqual(locationInfo.locations, job.locations)) patch.locations = locationInfo.locations;
        if (!arraysEqual(locationInfo.countries, job.countries)) patch.countries = locationInfo.countries;
        if (locationInfo.country !== job.country) patch.country = locationInfo.country;
        if (!arraysEqual(locationInfo.locationStates, job.locationStates)) {
          patch.locationStates = locationInfo.locationStates;
        }
        if (locationInfo.locationSearch !== job.locationSearch) {
          patch.locationSearch = locationInfo.locationSearch;
        }
        if (shouldReplaceText(city, job.city)) patch.city = city;
        if (shouldReplaceText(state, job.state)) patch.state = state;
      }

      if (typeof parsed.remote === "boolean" && parsed.remote !== job.remote) patch.remote = parsed.remote;
      if (parsed.level && parsed.level !== job.level) patch.level = parsed.level;
      if (typeof parsed.postedAt === "number" && parsed.postedAt !== job.postedAt) {
        patch.postedAt = parsed.postedAt;
      }
      if (job.scrapedAt === undefined && typeof scrape.completedAt === "number") {
        patch.scrapedAt = scrape.completedAt;
      }

      const prevKnownComp =
        job.compensationUnknown !== true &&
        typeof job.totalCompensation === "number" &&
        job.totalCompensation > 0;
      const shouldUpdateCompensation = !compensationUnknown || !prevKnownComp;
      if (shouldUpdateCompensation) {
        if (parsed.totalCompensation !== job.totalCompensation) {
          patch.totalCompensation = parsed.totalCompensation;
        }
        if (compensationUnknown !== job.compensationUnknown) {
          patch.compensationUnknown = compensationUnknown;
        }
        if (compensationReason !== job.compensationReason) {
          patch.compensationReason = compensationReason;
        }
      }

      if (Object.keys(patch).length > 0) {
        await ctx.db.patch(job._id, patch);
        jobsUpdated += 1;
      }

      const detailPatch: Record<string, any> = {};
      if (typeof parsed.description === "string" && parsed.description.trim()) {
        detailPatch.description = parsed.description;
      }
      if (scrapedWith) detailPatch.scrapedWith = scrapedWith;
      if (scrape.workflowName) detailPatch.workflowName = scrape.workflowName;
      const cost = typeof scrape.costMilliCents === "number" ? scrape.costMilliCents : undefined;
      const jobCount = scrapeJobCounts.get(String(scrape._id ?? "")) ?? 0;
      if (cost !== undefined && jobCount > 0) {
        detailPatch.scrapedCostMilliCents = Math.floor(cost / jobCount);
      }

      if (Object.keys(detailPatch).length > 0) {
        const existing = await ctx.db
          .query("job_details")
          .withIndex("by_job", (q: any) => q.eq("jobId", job._id))
          .first();
        if (existing) {
          const updates: Record<string, any> = {};
          for (const [key, value] of Object.entries(detailPatch)) {
            if ((existing as any)[key] !== value) {
              updates[key] = value;
            }
          }
          if (Object.keys(updates).length > 0) {
            await ctx.db.patch(existing._id, updates);
            jobDetailsUpdated += 1;
          }
        } else {
          await ctx.db.insert("job_details", { jobId: job._id, ...detailPatch });
          jobDetailsUpdated += 1;
        }
      }
    }

    return {
      companyInput: args.company,
      companyResolved: resolvedName,
      companyAliases: names,
      lookbackHours,
      jobLimit,
      scrapeLimit,
      jobsScanned: recentJobs.length,
      jobsMatched: companyJobs.length,
      jobsWithScrape: matchedByUrl.size,
      jobsUpdated,
      jobDetailsUpdated,
      jobsSkippedNoScrape,
      scrapesScanned: scrapes.length,
    };
  },
});

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
