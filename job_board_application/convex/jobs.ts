import { query, mutation } from "./_generated/server";
import { v } from "convex/values";
import { getAuthUserId } from "@convex-dev/auth/server";
import { paginationOptsValidator } from "convex/server";
import { ashbySlugFromUrl, greenhouseSlugFromUrl } from "./siteUtils";
import {
  splitLocation,
  formatLocationLabel,
  findCityInText,
  isUnknownLocationValue,
  normalizeLocations,
  deriveLocationFields,
} from "./location";
import type { Doc, Id } from "./_generated/dataModel";

const TITLE_RE = /^[ \t]*#{1,6}\s+(?<title>.+)$/im;
const LEVEL_RE =
  /\b(?<level>intern|junior|mid(?:-level)?|mid|sr|senior|staff|principal|lead|manager|director|vp|cto|chief technology officer)\b/i;
const LOCATION_RE =
  /\b(?:location|office|based\s+in)\s*[:\-–]\s*(?<location>[^\n,;]+(?:,\s*[^\n,;]+)?)/i;
const SIMPLE_LOCATION_LINE_RE = /^[ \t]*(?<location>[A-Z][\w .'-]+,\s*[A-Z][\w .'-]+)\s*$/m;
const SALARY_RE =
  /\$\s*(?<low>\d{2,3}(?:[.,]\d{3})*)(?:\s*[-–]\s*\$?\s*(?<high>\d{2,3}(?:[.,]\d{3})*))?\s*(?<period>per\s+year|per\s+annum|annual|yr|year|\/year|per\s+hour|hr|hour)?/i;
const SALARY_K_RE =
  /(?<currency>[$£€])?\s*(?<low>\d{2,3})\s*[kK]\s*(?:[-–]\s*(?<high>\d{2,3})\s*[kK])?\s*(?<code>USD|EUR|GBP)?/i;
const REMOTE_RE = /\b(remote(-first)?|hybrid|onsite|on-site)\b/i;

const isUnknownLabel = (value?: string | null) => {
  const normalized = (value || "").trim().toLowerCase();
  return (
    !normalized ||
    normalized === "unknown" ||
    normalized === "n/a" ||
    normalized === "na" ||
    normalized === "unspecified" ||
    normalized === "not available"
  );
};
const isVersionLabel = (value?: string | null) => /^v\d+$/i.test((value || "").trim());
const shouldOverrideCompany = (value?: string | null) => {
  const trimmed = (value || "").trim();
  return isUnknownLabel(value) || trimmed === "Greenhouse" || isVersionLabel(trimmed);
};

const toTitleCase = (value: string) => {
  const cleaned = value.replace(/[^a-z0-9]+/gi, " ").trim();
  if (!cleaned) return "";
  return cleaned
    .split(" ")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
};

export const deriveCompanyFromUrl = (url: string): string => {
  try {
    const greenhouseSlug = greenhouseSlugFromUrl(url);
    if (greenhouseSlug) {
      const greenhouseName = toTitleCase(greenhouseSlug);
      if (greenhouseName) return greenhouseName;
    }

    const parsed = new URL(url);
    const hostname = (parsed.hostname || "").toLowerCase();
    if (hostname.endsWith("greenhouse.io")) {
      const parts = parsed.pathname.split("/").filter(Boolean);
      if (parts.length > 0) {
        const slug = parts[0];
        const cleaned = toTitleCase(slug);
        if (cleaned) return cleaned;
      }
    }

    let baseHost = hostname;
    for (const prefix of ["careers.", "jobs.", "boards.", "boards-", "job-", "boards-"]) {
      if (baseHost.startsWith(prefix)) {
        baseHost = baseHost.slice(prefix.length);
        break;
      }
    }
    const parts = baseHost.split(".").filter(Boolean);
    const name = parts.length >= 2 ? parts[parts.length - 2] : parts[0] ?? "";
    return toTitleCase(name);
  } catch {
    return "";
  }
};

const toInt = (value: string | undefined | null) => {
  if (!value) return undefined;
  try {
    const digits = value.replace(/[,.]/g, "");
    return Number.isFinite(Number(digits)) ? parseInt(digits, 10) : undefined;
  } catch {
    return undefined;
  }
};

const arraysEqual = (a?: string[] | null, b?: string[] | null) =>
  JSON.stringify(a ?? []) === JSON.stringify(b ?? []);

const coerceLevelFromHint = (hint: string): "junior" | "mid" | "senior" | "staff" => {
  const h = hint.toLowerCase();
  if (h.includes("intern")) return "junior";
  if (h.includes("junior")) return "junior";
  if (h.includes("staff") || h.includes("principal") || h.includes("lead") || h.includes("director") || h.includes("vp") || h.includes("chief")) {
    return "staff";
  }
  if (h.includes("senior") || h === "sr") return "senior";
  if (h.includes("mid")) return "mid";
  return "mid";
};

export const parseMarkdownHints = (markdown: string) => {
  const hints: Record<string, any> = {};
  if (!markdown) return hints;

  const locationCandidates: string[] = [];

  const titleMatch = TITLE_RE.exec(markdown);
  if (titleMatch?.groups?.title) {
    hints.title = titleMatch.groups.title.trim();
  }

  // Location: prefer a short line beneath the header that looks like "City, State".
  const lines = markdown.split(/\r?\n/);
  for (const line of lines) {
    const t = line.trim();
    if (!t || t.startsWith("#")) continue;
    const lower = t.toLowerCase();
    if (lower.startsWith("job application for")) continue;
    if (t.includes("http")) continue;
    if (t.split(" ").length > 8) continue;
    if (t.includes(",")) {
      const candidate = t.split(";")[0].trim();
      if (/^[A-Za-z].*,/.test(candidate)) {
        locationCandidates.push(candidate);
      }
    }
  }
  if (!locationCandidates.length) {
    const cityHit = findCityInText(markdown);
    if (cityHit?.city && cityHit?.state) {
      locationCandidates.push(`${cityHit.city}, ${cityHit.state}`);
    }
  }
  if (!locationCandidates.length) {
    const locMatch = LOCATION_RE.exec(markdown) || SIMPLE_LOCATION_LINE_RE.exec(markdown);
    if (locMatch?.groups?.location) {
      locationCandidates.push(locMatch.groups.location.trim());
    }
  }

  const normalizedLocations = normalizeLocations(locationCandidates);
  if (normalizedLocations.length) {
    hints.locations = normalizedLocations;
    hints.location = normalizedLocations[0];
  } else if (locationCandidates.length) {
    hints.location = locationCandidates[0];
  }

  const levelMatch = LEVEL_RE.exec(markdown);
  if (levelMatch?.groups?.level) {
    hints.level = coerceLevelFromHint(levelMatch.groups.level);
  }

  const remoteMatch = REMOTE_RE.exec(markdown);
  if (remoteMatch) {
    const token = remoteMatch[1]?.toLowerCase() ?? "";
    if (token.includes("remote")) {
      hints.remote = true;
    } else if (token.includes("hybrid") || token.includes("on-site") || token.includes("onsite")) {
      hints.remote = false;
    } else {
      hints.remote = false;
    }
  }

  const collectSalaryValues = () => {
    const salaryValues: number[] = [];
    const salaryRanges: Array<{ low?: number; high?: number }> = [];
    const patterns = [
      { regex: SALARY_RE, multiplier: 1 },
      { regex: SALARY_K_RE, multiplier: 1000 },
    ];

    for (const { regex, multiplier } of patterns) {
      const flags = regex.flags.includes("g") ? regex.flags : `${regex.flags}g`;
      const globalRegex = new RegExp(regex.source, flags);
      for (const match of markdown.matchAll(globalRegex)) {
        const groups = match.groups ?? {};
        const period = typeof groups.period === "string" ? groups.period.toLowerCase() : "";
        if (period.includes("hour")) continue;
        const raw = (match[0] || "").toLowerCase();
        if (raw.includes("401k")) continue;

        const low = toInt(groups.low);
        const high = toInt(groups.high);
        const normalizedLow = typeof low === "number" ? low * multiplier : undefined;
        const normalizedHigh = typeof high === "number" ? high * multiplier : undefined;
        if (typeof normalizedLow === "number") salaryValues.push(normalizedLow);
        if (typeof normalizedHigh === "number") salaryValues.push(normalizedHigh);
        if (normalizedLow !== undefined || normalizedHigh !== undefined) {
          salaryRanges.push({ low: normalizedLow, high: normalizedHigh });
        }
      }
    }

    const filtered = salaryValues.filter((value) => value >= 10_000);
    const bestRange = salaryRanges
      .map((entry) => ({
        low: entry.low,
        high: entry.high,
        score: entry.high ?? entry.low ?? 0,
      }))
      .sort((a, b) => (b.score ?? 0) - (a.score ?? 0))
      .find((entry) => entry.score && entry.score >= 10_000);
    if (bestRange && (bestRange.low || bestRange.high)) {
      const rangePayload: Record<string, number> = {};
      if (typeof bestRange.low === "number") rangePayload.low = bestRange.low;
      if (typeof bestRange.high === "number") rangePayload.high = bestRange.high;
      if (Object.keys(rangePayload).length) {
        hints.compensationRange = rangePayload;
      }
    }

    return filtered;
  };

  const salaryValues = collectSalaryValues();
  if (salaryValues.length > 0) {
    const minSalary = Math.min(...salaryValues);
    const maxSalary = Math.max(...salaryValues);
    const averageSalary = Math.floor((minSalary + maxSalary) / 2);
    hints.compensation = averageSalary;
  }

  return hints;
};

export const buildUpdatesFromHints = (job: any, hints: Record<string, any>) => {
  const updates: Record<string, any> = {};

  if (hints.title && typeof job.title === "string" && job.title.toLowerCase().startsWith("job application for")) {
    updates.title = hints.title;
  }
  if (!updates.title && hints.title && typeof job.title === "string" && job.title !== hints.title) {
    updates.title = hints.title;
  }

  const normalizedLocations = normalizeLocations(hints.locations ?? hints.location);
  if (normalizedLocations.length) {
    const locationInfo = deriveLocationFields({ locations: normalizedLocations, location: normalizedLocations[0] });
    if (!job.location || isUnknownLocationValue(job.location) || job.location !== locationInfo.primaryLocation) {
      updates.location = locationInfo.primaryLocation;
    }
    if (!arraysEqual(job.locations, locationInfo.locations)) {
      updates.locations = locationInfo.locations;
    }
    if (!arraysEqual(job.locationStates, locationInfo.locationStates)) {
      updates.locationStates = locationInfo.locationStates;
    }
    if (!arraysEqual(job.countries, locationInfo.countries)) {
      updates.countries = locationInfo.countries;
    }
    if (job.country !== locationInfo.country) {
      updates.country = locationInfo.country;
    }
    if (locationInfo.locationSearch && job.locationSearch !== locationInfo.locationSearch) {
      updates.locationSearch = locationInfo.locationSearch;
    }
    if ((isUnknownLocationValue(job.city) || !job.city) && locationInfo.city) updates.city = locationInfo.city;
    if ((isUnknownLocationValue(job.state) || !job.state) && locationInfo.state) updates.state = locationInfo.state;
  }

  if (hints.level) {
    const nextLevel = coerceLevelFromHint(hints.level);
    if (job.level !== nextLevel) updates.level = nextLevel;
  }

  if (hints.remote === true && job.remote !== true) {
    updates.remote = true;
  } else if (hints.remote === false && job.remote === undefined) {
    updates.remote = false;
  }

  if (hints.compensation && (!job.totalCompensation || job.totalCompensation <= 0)) {
    updates.totalCompensation = hints.compensation;
    updates.compensationUnknown = false;
    updates.compensationReason = "parsed from description";
  } else if (hints.compensation && job.totalCompensation && job.totalCompensation > 0) {
    // Optionally tighten comp reason if we filled something previously from defaults.
    if (!job.compensationReason || job.compensationReason === "compensation provided in scrape payload") {
      updates.compensationReason = "parsed from description";
    }
  }

  return updates;
};

type DbJob = Omit<
  Doc<"jobs">,
  | "location"
  | "locations"
  | "countries"
  | "country"
  | "locationStates"
  | "locationSearch"
  | "city"
  | "state"
> & {
  location?: string | null;
  locations?: string[] | null;
  countries?: string[] | null;
  country?: string | null;
  locationStates?: string[] | null;
  locationSearch?: string | null;
  city?: string | null;
  state?: string | null;
  job_description?: string | null;
};

const ensureLocationFields = async (ctx: any, job: DbJob) => {
  const locationInfo = deriveLocationFields(job);
  const { city, state } = locationInfo;
  const normalizedCity = isUnknownLocationValue(job.city) ? locationInfo.city : job.city ?? locationInfo.city;
  const normalizedState = isUnknownLocationValue(job.state) ? locationInfo.state : job.state ?? locationInfo.state;
  const locationLabel = formatLocationLabel(normalizedCity, normalizedState, job.location ?? locationInfo.primaryLocation);

  const patched: Record<string, any> = {};
  if ((isUnknownLocationValue(job.city) || !job.city) && city) patched.city = city;
  if ((isUnknownLocationValue(job.state) || !job.state) && state) patched.state = state;
  if (!job.location || isUnknownLocationValue(job.location) || job.location !== locationLabel) {
    patched.location = locationLabel;
  }
  if (!Array.isArray(job.locations) || JSON.stringify(job.locations) !== JSON.stringify(locationInfo.locations)) {
    patched.locations = locationInfo.locations;
  }
  if (!Array.isArray(job.countries) || JSON.stringify(job.countries) !== JSON.stringify(locationInfo.countries)) {
    patched.countries = locationInfo.countries;
  }
  if (!job.country || job.country !== locationInfo.country) {
    patched.country = locationInfo.country;
  }
  if (!Array.isArray(job.locationStates) || JSON.stringify(job.locationStates) !== JSON.stringify(locationInfo.locationStates)) {
    patched.locationStates = locationInfo.locationStates;
  }
  if (!job.locationSearch || job.locationSearch !== locationInfo.locationSearch) {
    patched.locationSearch = locationInfo.locationSearch;
  }

  if (Object.keys(patched).length > 0 && typeof ctx.db?.patch === "function") {
    await ctx.db.patch(job._id, patched);
  }

  return {
    ...job,
    location: patched.location ?? locationLabel,
    locations: patched.locations ?? locationInfo.locations,
    locationStates: patched.locationStates ?? locationInfo.locationStates,
    locationSearch: patched.locationSearch ?? locationInfo.locationSearch,
    city: patched.city ?? normalizedCity,
    state: patched.state ?? normalizedState,
  } as DbJob;
};

const getJobDetailsByJobId = async (ctx: any, jobId: Id<"jobs">) => {
  return await ctx.db
    .query("job_details")
    .withIndex("by_job", (q: any) => q.eq("jobId", jobId))
    .first();
};

const mergeJobDetails = (job: Record<string, unknown>, details: Record<string, unknown> | null) => {
  if (!details) return job;
  const { jobId: _jobId, _id: _detailId, ...detailFields } = details;
  return { ...job, ...detailFields };
};

export const computeJobCountry = (job: DbJob) => {
  const locationInfo = deriveLocationFields(job);
  const locationCountries = locationInfo.countries ?? [];
  const locationStates = locationInfo.locationStates ?? [];
  const hasNonUnknownState = locationStates.some((state) => state && state !== "Unknown" && state !== "Remote");

  const explicitCountry = job.country?.trim();
  if (explicitCountry) {
    return explicitCountry;
  }

  const primaryCountry = locationCountries.find((c) => c && c !== "Unknown");
  if (primaryCountry && primaryCountry !== "Other") {
    return primaryCountry;
  }

  if (hasNonUnknownState) {
    return "United States";
  }

  if (primaryCountry === "Other") {
    return "Unknown";
  }

  return "Unknown";
};

const normalizeKeyPart = (value?: string | null) => (value ?? "").trim().toLowerCase();
const normalizeCompanyKey = (value?: string | null) => (value ?? "").trim().toLowerCase();

type FilterCursorPayload = {
  raw: string | null;
  carry: string[];
  done: boolean;
};

const parseFilterCursor = (cursor?: string | null) => {
  if (!cursor) {
    return { rawCursor: null, carryIds: [] as string[], rawIsDone: false };
  }

  try {
    const parsed = JSON.parse(cursor) as Partial<FilterCursorPayload> | null;
    if (parsed && typeof parsed === "object" && ("raw" in parsed || "carry" in parsed || "done" in parsed)) {
      return {
        rawCursor: typeof parsed.raw === "string" ? parsed.raw : null,
        carryIds: Array.isArray(parsed.carry) ? parsed.carry.filter((id): id is string => typeof id === "string") : [],
        rawIsDone: typeof parsed.done === "boolean" ? parsed.done : false,
      };
    }
  } catch {
    // Not our cursor format.
  }

  return { rawCursor: cursor, carryIds: [] as string[], rawIsDone: false };
};

const buildFilterCursor = (rawCursor: string | null, carryIds: string[], rawIsDone: boolean) =>
  JSON.stringify({ raw: rawCursor ?? null, carry: carryIds, done: rawIsDone } satisfies FilterCursorPayload);

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

const normalizeDomainInput = (value: string): string => {
  const trimmed = (value || "").trim();
  if (!trimmed) return "";

  try {
    const parsed = new URL(trimmed.includes("://") ? trimmed : `https://${trimmed}`);
    const host = parsed.hostname.toLowerCase();
    const greenhouseSlug = greenhouseSlugFromUrl(parsed.href);
    if (greenhouseSlug) return `${greenhouseSlug}.greenhouse.io`;
    const ashbySlug = ashbySlugFromUrl(parsed.href);
    if (ashbySlug) return `${ashbySlug}.ashbyhq.com`;
    return baseDomainFromHost(host);
  } catch {
    const hostOnly = trimmed.replace(/^https?:\/\//i, "").split("/")[0] || trimmed;
    const host = hostOnly.toLowerCase();
    const greenhouseSlug = greenhouseSlugFromUrl(host);
    if (greenhouseSlug) return `${greenhouseSlug}.greenhouse.io`;
    const ashbySlug = ashbySlugFromUrl(trimmed);
    if (ashbySlug) return `${ashbySlug}.ashbyhq.com`;
    return baseDomainFromHost(host);
  }
};

export const matchesCompanyFilters = (
  job: { company?: string | null; url?: string | null },
  normalizedCompanyFilters: Set<string>,
  domainAliasByDomain?: Map<string, string> | null
) => {
  if (!normalizedCompanyFilters.size) return true;
  const companyKey = normalizeCompanyKey(job.company);
  if (companyKey && normalizedCompanyFilters.has(companyKey)) return true;
  if (!domainAliasByDomain || domainAliasByDomain.size === 0) return false;
  const domain = normalizeDomainInput(job.url ?? "");
  if (!domain) return false;
  const aliasKey = normalizeCompanyKey(domainAliasByDomain.get(domain) ?? "");
  if (!aliasKey) return false;
  return normalizedCompanyFilters.has(aliasKey);
};

const buildJobGroupKey = (job: DbJob) => {
  // Group primarily by title + company, then level and remote flag to avoid over-merging unrelated roles
  const normalizedTitle = normalizeKeyPart(job.title).replace(/\s+/g, " ");
  const normalizedCompany = normalizeKeyPart(job.company).replace(/\s+/g, " ");
  const normalizedLevel = normalizeKeyPart(job.level as string | undefined);
  const remoteToken = job.remote ? "remote" : "onsite";
  return `${normalizedTitle}|${normalizedCompany}|${normalizedLevel}|${remoteToken}`;
};

const mergeStrings = (...candidates: Array<string | string[] | null | undefined>) => {
  const seen = new Set<string>();
  const merged: string[] = [];

  for (const entry of candidates.flat()) {
    if (Array.isArray(entry)) {
      for (const inner of entry) {
        const cleaned = (inner ?? "").trim();
        if (!cleaned || cleaned.toLowerCase() === "unknown") continue;
        const key = cleaned.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);
        merged.push(cleaned);
      }
      continue;
    }

    const cleaned = (entry ?? "").trim();
    if (!cleaned || cleaned.toLowerCase() === "unknown") continue;
    const key = cleaned.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    merged.push(cleaned);
  }

  return merged;
};

const pickBestCompJob = (jobs: DbJob[]) => {
  const withKnownComp = jobs.filter(
    (job) => job.compensationUnknown !== true && typeof job.totalCompensation === "number" && job.totalCompensation > 0
  );

  if (withKnownComp.length === 0) return null;

  return withKnownComp.sort((a, b) => (b.totalCompensation ?? 0) - (a.totalCompensation ?? 0))[0];
};

export const matchesCountryFilter = (jobCountry: string, countryFilter: string, isOtherCountry: boolean) => {
  if (!countryFilter) return true;
  if (!isOtherCountry) {
    return jobCountry === countryFilter || jobCountry === "Unknown";
  }
  return jobCountry !== "United States";
};

const runLocationMigration = async (ctx: any, limit = 500) => {
  const jobs = await ctx.db.query("jobs").take(limit);
  let patched = 0;

  for (const job of jobs) {
    const locationInfo = deriveLocationFields(job);
    const { city, state, primaryLocation, locations, locationStates, locationSearch, countries, country } = locationInfo;
    const locationLabel = formatLocationLabel(city, state, primaryLocation);
    const update: Record<string, any> = {};
    if (job.city !== city) update.city = city;
    if (job.state !== state) update.state = state;
    if (job.location !== locationLabel) update.location = locationLabel;
    if (!Array.isArray(job.locations) || JSON.stringify(job.locations) !== JSON.stringify(locations)) {
      update.locations = locations;
    }
    if (!Array.isArray(job.countries) || JSON.stringify(job.countries) !== JSON.stringify(countries)) {
      update.countries = countries;
    }
    if (job.country !== country) {
      update.country = country;
    }
    if (!Array.isArray(job.locationStates) || JSON.stringify(job.locationStates) !== JSON.stringify(locationStates)) {
      update.locationStates = locationStates;
    }
    if (job.locationSearch !== locationSearch) {
      update.locationSearch = locationSearch;
    }
    if (Object.keys(update).length) {
      await ctx.db.patch(job._id, update);
      patched += 1;
    }
  }

  return { patched };
};

export const listJobs = query({
  args: {
    paginationOpts: paginationOptsValidator,
    search: v.optional(v.string()),
    includeRemote: v.optional(v.boolean()),
    state: v.optional(v.string()),
    country: v.optional(v.string()),
    level: v.optional(v.union(v.literal("junior"), v.literal("mid"), v.literal("senior"), v.literal("staff"))),
    minCompensation: v.optional(v.number()),
    maxCompensation: v.optional(v.number()),
    hideUnknownCompensation: v.optional(v.boolean()),
    companies: v.optional(v.array(v.string())),
    useSearch: v.optional(v.boolean()),
  },
  handler: async (ctx, args) => {
    const userId = await getAuthUserId(ctx);
    if (!userId) {
      throw new Error("Not authenticated");
    }

    const rawSearch = (args.search ?? "").trim();
    const countryFilterRaw = (args.country ?? "").trim();
    const hasCountryFilter = countryFilterRaw.length > 0;
    const countryFilter = countryFilterRaw;
    const isOtherCountry = countryFilter.toLowerCase() === "other";
    const stateFilter = (args.state ?? "").trim();
    const shouldUseSearch = rawSearch.length > 0;

    const companyFilters = (args.companies ?? []).map((c) => c.trim()).filter(Boolean);
    const normalizedCompanyFilters = new Set(companyFilters.map((c) => normalizeCompanyKey(c)));
    const hasCompanyFilter = normalizedCompanyFilters.size > 0;
    const singleCompanyFilter = hasCompanyFilter && companyFilters.length === 1 ? companyFilters[0] : null;

    // Get user's applied/rejected jobs first
    const userApplications = await ctx.db
      .query("applications")
      .withIndex("by_user", (q) => q.eq("userId", userId))
      .collect();

    const appliedJobIds = new Set(userApplications.map(app => app.jobId));

    let domainAliasLookup: Map<string, string> | null = null;
    if (hasCompanyFilter) {
      const aliasRows = await ctx.db.query("domain_aliases").collect();
      domainAliasLookup = new Map();
      for (const row of aliasRows as any[]) {
        const domain = (row)?.domain?.trim?.() ?? "";
        const alias = normalizeCompanyKey((row)?.alias ?? "");
        if (domain && alias) {
          domainAliasLookup.set(domain, alias);
        }
      }
    }

    const jobPassesFilters = (job: any) => {
      // Remove jobs user has already applied to or rejected
      if (appliedJobIds.has(job._id)) {
        return false;
      }

      const locationInfo = deriveLocationFields(job);
      const jobCountry = computeJobCountry(job);

      if (hasCountryFilter && !matchesCountryFilter(jobCountry, countryFilter, isOtherCountry)) {
        return false;
      }
      if (stateFilter) {
        const statesForFilter = locationInfo.locationStates.length ? locationInfo.locationStates : [locationInfo.state];
        if (!statesForFilter.includes(stateFilter)) return false;
      }
      if (args.includeRemote === false && job.remote) {
        return false;
      }
      if (hasCompanyFilter) {
        if (!matchesCompanyFilters(job, normalizedCompanyFilters, domainAliasLookup)) {
          return false;
        }
      }

      // Apply compensation filters
      const compensationUnknown = job.compensationUnknown === true;
      const compValue = typeof job.totalCompensation === "number" ? job.totalCompensation : 0;
      if (args.hideUnknownCompensation && compensationUnknown) {
        return false;
      }
      if (args.minCompensation !== undefined && !compensationUnknown && compValue < args.minCompensation) {
        return false;
      }
      if (args.maxCompensation !== undefined && !compensationUnknown && compValue > args.maxCompensation) {
        return false;
      }
      return true;
    };

    // Apply search and filters
    let jobs;
    let jobsAlreadyFiltered = false;
    if (shouldUseSearch) {
      const SEARCH_LIMIT = 100;
      const matches = await ctx.db
        .query("jobs")
        .withSearchIndex("search_title", (q: any) => {
          let searchQuery = q.search("title", rawSearch);
          if (args.includeRemote === false) {
            searchQuery = searchQuery.eq("remote", false);
          }
          if (args.state) {
            searchQuery = searchQuery.eq("state", args.state);
          }
          if (args.level) {
            searchQuery = searchQuery.eq("level", args.level);
          }
          return searchQuery;
        })
        .take(SEARCH_LIMIT);

      jobs = {
        page: matches.sort((a: any, b: any) => (b.postedAt ?? 0) - (a.postedAt ?? 0)),
        isDone: true,
        continueCursor: null,
      };
    } else if (stateFilter) {
      const SEARCH_LIMIT = 200;
      const matches = await ctx.db
        .query("jobs")
        .withSearchIndex("search_locations", (q: any) => {
          let searchQuery = q.search("locationSearch", stateFilter);
          if (args.includeRemote === false) {
            searchQuery = searchQuery.eq("remote", false);
          }
          if (args.level) {
            searchQuery = searchQuery.eq("level", args.level);
          }
          return searchQuery;
        })
        .take(SEARCH_LIMIT);

      const fallbackCandidates = await ctx.db.query("jobs").withIndex("by_posted_at").order("desc").take(SEARCH_LIMIT);
      const combined = new Map<string, any>();
      for (const job of matches) {
        combined.set(String(job._id), job);
      }
      for (const job of fallbackCandidates) {
        const locationInfo = deriveLocationFields(job);
        const statesForFilter = locationInfo.locationStates.length ? locationInfo.locationStates : [locationInfo.state];
        if (args.includeRemote === false && job.remote) continue;
        if (args.level && job.level !== args.level) continue;
        if (statesForFilter.includes(stateFilter)) {
          combined.set(String(job._id), job);
        }
      }

      jobs = {
        page: Array.from(combined.values()).sort((a: any, b: any) => (b.postedAt ?? 0) - (a.postedAt ?? 0)),
        isDone: true,
        continueCursor: null,
      };
    } else {
      const buildBaseQuery = () => {
        let query: any = ctx.db.query("jobs");

        if (singleCompanyFilter) {
          query = query.withIndex("by_company_posted", (q: any) => q.eq("company", singleCompanyFilter));
        } else if (stateFilter) {
          query = query.withIndex("by_state_posted", (q: any) => q.eq("state", args.state));
        } else {
          query = query.withIndex("by_posted_at");
        }

        query = query.order("desc");

        if (args.includeRemote === false) {
          query = query.filter((q: any) => q.eq(q.field("remote"), false));
        }
        if (args.level) {
          query = query.filter((q: any) => q.eq(q.field("level"), args.level));
        }
        if (rawSearch && args.state) {
          query = query.filter((q: any) => q.eq(q.field("state"), args.state));
        }

        return query;
      };

      const needsFilteredPagination =
        appliedJobIds.size > 0 ||
        hasCompanyFilter ||
        hasCountryFilter ||
        args.hideUnknownCompensation === true ||
        args.minCompensation !== undefined ||
        args.maxCompensation !== undefined;

      if (!needsFilteredPagination) {
        jobs = await buildBaseQuery().paginate(args.paginationOpts);
      } else {
        const pageSize = args.paginationOpts.numItems ?? 50;
        const { rawCursor: initialRawCursor, carryIds, rawIsDone: initialRawIsDone } = parseFilterCursor(
          args.paginationOpts.cursor
        );
        let rawCursor = initialRawCursor;
        let rawIsDone = initialRawIsDone;
        const filteredBuffer: any[] = [];

        if (carryIds.length > 0) {
          const carryJobs = await Promise.all(carryIds.map((id) => ctx.db.get(id as Id<"jobs">)));
          for (const job of carryJobs) {
            if (job && jobPassesFilters(job)) {
              filteredBuffer.push(job);
            }
          }
        }

        if (!rawIsDone && filteredBuffer.length < pageSize) {
          const expandedSize = Math.min(pageSize * 4, 200);
          const page = await buildBaseQuery().paginate({
            ...args.paginationOpts,
            cursor: rawCursor,
            numItems: expandedSize,
          });
          rawCursor = page.continueCursor;
          rawIsDone = page.isDone;
          if (page.page.length) {
            const orderedPage = [...page.page].sort((a: any, b: any) => (b.postedAt ?? 0) - (a.postedAt ?? 0));
            for (const job of orderedPage) {
              if (jobPassesFilters(job)) {
                filteredBuffer.push(job);
              }
            }
          }
        }

        const pageJobs = filteredBuffer.slice(0, pageSize);
        const carryOverIds = filteredBuffer.slice(pageSize).map((job: any) => String(job._id));
        const isDone = rawIsDone && carryOverIds.length === 0;
        const continueCursor = isDone ? null : buildFilterCursor(rawCursor, carryOverIds, rawIsDone);

        jobs = {
          page: pageJobs,
          isDone,
          continueCursor,
        };
        jobsAlreadyFiltered = true;
      }
    }

    // Ensure descending order by postedAt for all paths
    const orderedPage = [...jobs.page].sort(
      (a: any, b: any) => (b.postedAt ?? 0) - (a.postedAt ?? 0)
    );

    // Filter out applied/rejected jobs and apply compensation filters
    const filteredJobs = jobsAlreadyFiltered ? orderedPage : orderedPage.filter(jobPassesFilters);

    // Group jobs with same title/company/level/remote into one row, merging locations and URLs
    const grouped = new Map<string, { base: any; members: any[] }>();

    for (const job of filteredJobs) {
      const key = buildJobGroupKey(job);
      const bucket = grouped.get(key);
      if (bucket) {
        bucket.members.push(job);
      } else {
        grouped.set(key, { base: job, members: [job] });
      }
    }

    const jobsWithData = await Promise.all(
      Array.from(grouped.values()).map(async ({ base, members }) => {
        // Pick a representative job for compensation display
        const compJob = pickBestCompJob(members as any) || base;
        const normalizedBase = await ensureLocationFields(ctx, base);

        const allLocations = mergeStrings(
          normalizedBase.locations,
          members.flatMap((m) => (Array.isArray((m).locations) ? (m).locations : [])),
          members.map((m) => (m).location),
        );

        const locationStatesMerged = Array.from(
          new Set(
            members.flatMap((m) => {
              const info = deriveLocationFields(m);
              return info.locationStates.length ? info.locationStates : [info.state];
            }).filter(Boolean)
          )
        );

        const urls = Array.from(new Set(members.map((m) => (m).url).filter(Boolean)));
        const applicationCount = await ctx.db
          .query("applications")
          .withIndex("by_job", (q) => q.eq("jobId", base._id))
          .filter((q) => q.eq(q.field("status"), "applied"))
          .collect();

        // Sum applications across grouped job ids (fallback to base when access limited)
        let totalApplications = applicationCount.length;
        if ((members).length > 1) {
          for (const member of members) {
            if ((member)._id === base._id) continue;
            const extra = await ctx.db
              .query("applications")
              .withIndex("by_job", (q) => q.eq("jobId", (member)._id))
              .filter((q) => q.eq(q.field("status"), "applied"))
              .collect();
            totalApplications += extra.length;
          }
        }

        return {
          ...normalizedBase,
          totalCompensation: compJob.totalCompensation,
          compensationUnknown: compJob.compensationUnknown,
          compensationReason: compJob.compensationReason,
          locations: allLocations,
          locationStates: locationStatesMerged,
          url: urls[0],
          alternateUrls: urls,
          groupedJobIds: members.map((m) => (m)._id),
          applicationCount: totalApplications,
          userStatus: null, // These jobs don't have user applications by definition
        } as any;
      })
    );

    return {
      page: jobsWithData,
      isDone: jobs.isDone,
      continueCursor: jobs.continueCursor,
    };
  },
});

export const searchCompanies = query({
  args: {
    search: v.optional(v.string()),
    limit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const userId = await getAuthUserId(ctx);
    if (!userId) {
      throw new Error("Not authenticated");
    }

    const searchTerm = (args.search ?? "").trim();
    const limit = Math.max(1, Math.min(args.limit ?? 12, 50));
    const baseQuery = searchTerm
      ? ctx.db
          .query("jobs")
          .withSearchIndex("search_company", (q) => q.search("company", searchTerm))
      : ctx.db.query("jobs").withIndex("by_posted_at").order("desc");

    const matches = await baseQuery.take(200);
    const counts = new Map<string, { name: string; count: number }>();

    for (const job of matches) {
      const companyName = typeof (job as any).company === "string" ? (job as any).company.trim() : "";
      if (!companyName) continue;
      const key = companyName.toLowerCase();
      const existing = counts.get(key);
      if (existing) {
        existing.count += 1;
      } else {
        counts.set(key, { name: companyName, count: 1 });
      }
    }

    const suggestions = Array.from(counts.values())
      .sort((a, b) => {
        if (b.count === a.count) return a.name.localeCompare(b.name);
        return b.count - a.count;
      })
      .slice(0, limit);

    return suggestions;
  },
});

export const applyToJob = mutation({
  args: {
    jobId: v.id("jobs"),
    type: v.union(v.literal("ai"), v.literal("manual")),
  },
  handler: async (ctx, args) => {
    const userId = await getAuthUserId(ctx);
    if (!userId) {
      throw new Error("Not authenticated");
    }

    // Check if user already applied or rejected this job
    const existingApplication = await ctx.db
      .query("applications")
      .withIndex("by_user_and_job", (q) => q.eq("userId", userId).eq("jobId", args.jobId))
      .unique();

    if (existingApplication) {
      throw new Error("Already applied to this job");
    }

    await ctx.db.insert("applications", {
      userId,
      jobId: args.jobId,
      status: "applied",
      appliedAt: Date.now(),
    });

    return { success: true };
  },
});

export const rejectJob = mutation({
  args: {
    jobId: v.id("jobs"),
  },
  handler: async (ctx, args) => {
    const userId = await getAuthUserId(ctx);
    if (!userId) {
      throw new Error("Not authenticated");
    }

    // Check if user already has an application for this job
    const existingApplication = await ctx.db
      .query("applications")
      .withIndex("by_user_and_job", (q) => q.eq("userId", userId).eq("jobId", args.jobId))
      .unique();

    if (existingApplication) {
      await ctx.db.patch(existingApplication._id, { status: "rejected" });
    } else {
      await ctx.db.insert("applications", {
        userId,
        jobId: args.jobId,
        status: "rejected",
        appliedAt: Date.now(),
      });
    }

    return { success: true };
  },
});

export const reparseJobFromDescription = mutation({
  args: { jobId: v.id("jobs") },
  handler: async (ctx, args) => {
    const job = await ctx.db.get(args.jobId);
    if (!job) throw new Error("Job not found");

    const details = await getJobDetailsByJobId(ctx, args.jobId);
    const description =
      typeof details?.description === "string"
        ? details.description
        : typeof (job as any).description === "string"
          ? (job as any).description
          : "";
    const hints = parseMarkdownHints(description);
    const updates = buildUpdatesFromHints(job, hints);
    const derivedCompany = deriveCompanyFromUrl(job.url || "");
    if (derivedCompany && shouldOverrideCompany(job.company)) {
      updates.company = derivedCompany;
    }

    if (Object.keys(updates).length === 0) {
      return { updated: 0, hints };
    }

    await ctx.db.patch(args.jobId, updates);
    return { updated: Object.keys(updates).length, hints };
  },
});

export const reparseAllJobs = mutation({
  args: {
    limit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const limit = args.limit ?? 200;
    const jobs = await ctx.db.query("jobs").take(limit);
    let updated = 0;

    for (const job of jobs) {
      const details = await getJobDetailsByJobId(ctx, job._id);
      const description =
        typeof details?.description === "string"
          ? details.description
          : typeof (job as any).description === "string"
            ? (job as any).description
            : "";
      const hints = parseMarkdownHints(description);
      const updates = buildUpdatesFromHints(job as any, hints);
      const derivedCompany = deriveCompanyFromUrl((job as any).url || "");
      if (derivedCompany && shouldOverrideCompany((job as any).company)) {
        updates.company = derivedCompany;
      }
      if (Object.keys(updates).length > 0) {
        await ctx.db.patch(job._id, updates);
        updated += 1;
      }
    }

    return { scanned: jobs.length, updated };
  },
});

export const retagVersionCompany = mutation({
  args: {},
  handler: async (ctx) => {
    const labels = ["V1", "v1"];
    let scanned = 0;
    let updated = 0;

    for (const label of labels) {
      let cursor: any = null;
      while (true) {
        const { page, isDone, continueCursor } = await ctx.db
          .query("jobs")
          .withIndex("by_company", (q: any) => q.eq("company", label))
          .paginate({ cursor, numItems: 200 });

        scanned += page.length;
        for (const job of page) {
          const derived = deriveCompanyFromUrl((job as any).url || "");
          if (!derived || derived === (job as any).company) continue;
          await ctx.db.patch(job._id, { company: derived });
          updated += 1;
        }

        if (isDone || !continueCursor) break;
        cursor = continueCursor;
      }
    }

    return { scanned, updated };
  },
});

export const getRecentJobs = query({
  args: {},
  handler: async (ctx) => {
    // This query will automatically update when new jobs are inserted
    // because Convex queries are reactive by default
    const jobs = await ctx.db
      .query("jobs")
      .withIndex("by_posted_at")
      .order("desc")
      .take(20); // Increased from 10 to show more recent jobs

    const normalized = await Promise.all(jobs.map((job: any) => ensureLocationFields(ctx, job)));
    return normalized;
  },
});

export const getAppliedJobs = query({
  args: {},
  handler: async (ctx) => {
    const userId = await getAuthUserId(ctx);
    if (!userId) {
      throw new Error("Not authenticated");
    }

    const applications = await ctx.db
      .query("applications")
      .withIndex("by_user", (q) => q.eq("userId", userId))
      .filter((q) => q.eq(q.field("status"), "applied"))
      .collect();

    const appliedJobs = await Promise.all(
      applications.map(async (application) => {
        const job = await ctx.db.get(application.jobId);
        if (!job) return null;
        const normalized = await ensureLocationFields(ctx, job as any);

        // Fetch worker status from form_fill_queue
        const workerStatus = await ctx.db
          .query("form_fill_queue")
          .withIndex("by_user", (q) => q.eq("userId", userId))
          .filter((q) => q.eq(q.field("jobUrl"), job.url))
          .first();

        return {
          ...normalized,
          appliedAt: application.appliedAt,
          userStatus: application.status,
          workerStatus: workerStatus?.status ?? null,
          workerUpdatedAt: workerStatus?.updatedAt ?? workerStatus?.queuedAt ?? null,
        };
      })
    );

    return appliedJobs
      .filter((job) => job !== null)
      .sort((a, b) => b.appliedAt - a.appliedAt);
  },
});

export const getRejectedJobs = query({
  args: {},
  handler: async (ctx) => {
    const userId = await getAuthUserId(ctx);
    if (!userId) {
      throw new Error("Not authenticated");
    }

    const applications = await ctx.db
      .query("applications")
      .withIndex("by_user", (q) => q.eq("userId", userId))
      .filter((q) => q.eq(q.field("status"), "rejected"))
      .collect();

    const rejectedJobs = await Promise.all(
      applications.map(async (application) => {
        const job = await ctx.db.get(application.jobId);
        if (!job) return null;
        const normalized = await ensureLocationFields(ctx, job as any);
        return {
          ...normalized,
          rejectedAt: application.appliedAt,
          userStatus: application.status,
        };
      })
    );

    return rejectedJobs
      .filter((job) => job !== null)
      .sort((a, b) => (b?.rejectedAt ?? 0) - (a?.rejectedAt ?? 0));
  },
});

export const getJobById = query({
  args: {
    id: v.id("jobs"),
  },
  handler: async (ctx, args) => {
    const job = await ctx.db.get(args.id);
    if (!job) return null;

    const normalized = await ensureLocationFields(ctx, job as any);
    const details = await getJobDetailsByJobId(ctx, args.id);
    return mergeJobDetails(normalized, details);
  },
});

export const getJobDetails = query({
  args: {
    jobId: v.optional(v.id("jobs")),
  },
  handler: async (ctx, args) => {
    if (!args.jobId) return null;
    const details = await getJobDetailsByJobId(ctx, args.jobId);
    if (!details) return null;
    const { jobId: _jobId, _id: _detailId, ...detailFields } = details;
    return detailFields;
  },
});

export const checkIfJobsExist = query({
  args: {},
  handler: async (ctx) => {
    const jobs = await ctx.db.query("jobs").take(1);
    return jobs.length > 0;
  },
});

export const withdrawApplication = mutation({
  args: {
    jobId: v.id("jobs"),
  },
  handler: async (ctx, args) => {
    const userId = await getAuthUserId(ctx);
    if (!userId) {
      throw new Error("Not authenticated");
    }

    const existingApplication = await ctx.db
      .query("applications")
      .withIndex("by_user_and_job", (q) => q.eq("userId", userId).eq("jobId", args.jobId))
      .unique();

    if (!existingApplication) {
      throw new Error("Application not found");
    }
    if (existingApplication.status !== "applied") {
      throw new Error("No active application to withdraw");
    }

    await ctx.db.delete(existingApplication._id);
    return { success: true };
  },
});

export const normalizeDevTestJobs = mutation({
  args: {},
  handler: async (ctx) => {
    const jobs = await ctx.db.query("jobs").collect();
    const detailRows = await ctx.db.query("job_details").collect();
    const detailByJobId = new Map(detailRows.map((row: any) => [String(row.jobId), row]));
    const needsFix = jobs.filter((j: any) => {
      const tooShort = (s: any) => typeof s === "string" && s.trim().length <= 2;
      const details = detailByJobId.get(String(j._id));
      const description =
        typeof details?.description === "string"
          ? details.description
          : typeof j.description === "string"
            ? j.description
            : "";
      return (
        (j.title && (j.title.startsWith("HC-") || tooShort(j.title))) ||
        tooShort(j.company) ||
        tooShort(j.location) ||
        tooShort(description) ||
        (typeof j.totalCompensation === "number" && j.totalCompensation <= 10) ||
        j.company === "Health Co"
      );
    });

    const titles = [
      "Software Engineer",
      "Frontend Developer",
      "Backend Engineer",
      "Full Stack Developer",
      "Data Engineer",
    ];
    const companies = ["Acme Corp", "SampleSoft", "Initech", "Globex", "Umbrella Labs"];
    const locations = ["Remote - US", "San Francisco, CA", "New York, NY", "Austin, TX", "Seattle, WA"];

    let updates = 0;
    for (const j of needsFix) {
      const pick = (arr: string[]) => arr[Math.floor(Math.random() * arr.length)];
      const comp = 100000 + Math.floor(Math.random() * 90000);
      const loc = pick(locations);
      const { city, state } = splitLocation(loc);
      const detailPatch = {
        description:
          "This is a realistic sample listing used for development. Replace with real scraped data in production.",
      };
      await ctx.db.patch(j._id, {
        title: pick(titles),
        company: pick(companies),
        location: formatLocationLabel(city, state, loc),
        city,
        state,
        totalCompensation: comp,
        remote: loc.toLowerCase().includes("remote") ?? true,
      });
      const existingDetails = detailByJobId.get(String(j._id));
      if (existingDetails) {
        await ctx.db.patch(existingDetails._id, detailPatch);
      } else {
        await ctx.db.insert("job_details", { jobId: j._id, ...detailPatch });
      }
      updates++;
    }
    return { success: true, updated: updates };
  },
});

export const migrateJobLocations = mutation({
  args: {
    limit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const limit = args.limit ?? 500;
    return runLocationMigration(ctx, limit);
  },
});

export const deleteJob = mutation({
  args: {
    jobId: v.id("jobs"),
  },
  handler: async (ctx, args) => {
    await ctx.db.delete(args.jobId);
    return { success: true };
  },
});
