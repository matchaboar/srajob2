import { query, mutation, internalMutation } from "./_generated/server";
import { v } from "convex/values";
import { getAuthUserId } from "@convex-dev/auth/server";
import { paginationOptsValidator } from "convex/server";
import { ashbySlugFromUrl, greenhouseSlugFromUrl } from "./siteUtils";
import {
  splitLocation,
  formatLocationLabel,
  findCityInText,
  resolveLocationFromDictionary,
  isUnknownLocationValue,
  normalizeLocations,
  deriveLocationFields,
  inferCountryFromLocation,
} from "./location";
import type { Doc, Id } from "./_generated/dataModel";
import {
  buildDescriptionPreview,
  deleteDescriptionFromStorage,
  loadDescriptionFromStorage,
  storeDescriptionInStorage,
} from "./jobDescriptionStorage";

const TITLE_RE = /^[ \t]*#{1,6}\s+(?<title>.+)$/im;
const LEVEL_RE =
  /\b(?<level>intern|junior|mid(?:-level)?|mid|sr|senior|staff|principal|lead|manager|director|vp|cto|chief technology officer)\b/i;
const LOCATION_RE =
  /\b(?:location|office|based\s+in)\s*[:\-–]\s*(?<location>[^\n,;]+(?:,\s*[^\n,;]+)?)/i;
const SIMPLE_LOCATION_LINE_RE =
  /^[ \t]*(?<location>[A-Z][\w .'-]+,\s*[A-Z][\w .'-]+)\s*$/m;
const SALARY_RE =
  /\$\s*(?<low>\d{2,3}(?:[.,]\d{3})*)(?:\s*[-–]\s*(?:USD|US\$)?\s*\$?\s*(?<high>\d{2,3}(?:[.,]\d{3})*))?\s*(?<period>per\s+year|per\s+annum|annual|yr|year|\/year|per\s+hour|hr|hour)?/i;
const SALARY_K_RE =
  /(?<currency>[$£€])?\s*(?<low>\d{2,3})\s*[kK]\s*(?:[-–]\s*(?<high>\d{2,3})\s*[kK])?\s*(?<code>USD|EUR|GBP)?/i;
const REMOTE_RE = /\b(remote(-first)?|hybrid|onsite|on-site)\b/i;
const CITY_STATE_ABBR_RE =
  /\b(?<city>[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+)*)\s*,\s*(?<state>[A-Z]{2})\b/g;

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
const UNKNOWN_TITLE_LABELS = new Set([
  "page_title",
  "title",
  "job_title",
  "untitled",
  "application",
]);

const isUnknownJobTitle = (value?: string | null) => {
  const normalized = (value || "").trim().toLowerCase();
  if (!normalized) return true;
  if (isUnknownLabel(normalized)) return true;
  return UNKNOWN_TITLE_LABELS.has(normalized);
};

export const deriveEngineerFlag = (title?: string | null) => {
  if (isUnknownJobTitle(title)) return true;
  return (title || "").toLowerCase().includes("engineer");
};
const isVersionLabel = (value?: string | null) =>
  /^v\d+$/i.test((value || "").trim());
const shouldOverrideCompany = (value?: string | null) => {
  const trimmed = (value || "").trim();
  const normalized = trimmed.toLowerCase().replace(/[^a-z0-9]/g, "");
  return (
    isUnknownLabel(value) ||
    trimmed === "Greenhouse" ||
    isVersionLabel(trimmed) ||
    normalized === "ashbyhq"
  );
};

const normalizeSortTimestamp = (value: unknown) =>
  typeof value === "number" && Number.isFinite(value) ? value : 0;

const compareNewestJobs = (a: any, b: any) => {
  const aScraped = normalizeSortTimestamp(a.scrapedAt);
  const bScraped = normalizeSortTimestamp(b.scrapedAt);
  if (aScraped === bScraped) {
    const aUnknown = a.postedAtUnknown === true;
    const bUnknown = b.postedAtUnknown === true;
    if (aUnknown !== bUnknown) {
      return aUnknown ? 1 : -1;
    }
  }

  const aPosted = normalizeSortTimestamp(a.postedAt);
  const bPosted = normalizeSortTimestamp(b.postedAt);
  if (aPosted !== bPosted) {
    return bPosted - aPosted;
  }
  return bScraped - aScraped;
};

const compareScrapedJobs = (a: any, b: any) => {
  const aScraped = normalizeSortTimestamp(a.scrapedAt);
  const bScraped = normalizeSortTimestamp(b.scrapedAt);
  if (aScraped !== bScraped) {
    return bScraped - aScraped;
  }
  const aPosted = normalizeSortTimestamp(a.postedAt);
  const bPosted = normalizeSortTimestamp(b.postedAt);
  return bPosted - aPosted;
};

const toTitleCase = (value: string) => {
  const cleaned = value.replace(/[^a-z0-9]+/gi, " ").trim();
  if (!cleaned) return "";
  return cleaned
    .split(" ")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
};

const WORKDAY_HOST_SUFFIX = "myworkdayjobs.com";
const WORKDAY_SKIP_SUBDOMAINS = new Set([
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

const deriveWorkdayCompany = (hostname: string): string => {
  if (!hostname.endsWith(WORKDAY_HOST_SUFFIX)) return "";
  const parts = hostname.split(".").filter(Boolean);
  if (parts.length < 3) return "";
  const subdomains = parts.slice(0, -2);
  for (const candidate of subdomains) {
    if (!candidate) continue;
    if (WORKDAY_SKIP_SUBDOMAINS.has(candidate)) continue;
    if (/^wd\d+$/i.test(candidate)) continue;
    const cleaned = toTitleCase(candidate);
    if (cleaned) return cleaned;
  }
  for (let i = subdomains.length - 1; i >= 0; i -= 1) {
    const candidate = subdomains[i];
    if (!candidate) continue;
    if (/^wd\d+$/i.test(candidate)) continue;
    const cleaned = toTitleCase(candidate);
    if (cleaned) return cleaned;
  }
  return "";
};

export const deriveCompanyFromUrl = (url: string): string => {
  try {
    const greenhouseSlug = greenhouseSlugFromUrl(url);
    if (greenhouseSlug) {
      const greenhouseName = toTitleCase(greenhouseSlug);
      if (greenhouseName) return greenhouseName;
    }

    const ashbySlug = ashbySlugFromUrl(url);
    if (ashbySlug) {
      const ashbyName = toTitleCase(ashbySlug);
      if (ashbyName) return ashbyName;
    }

    const parsed = new URL(url);
    const hostname = (parsed.hostname || "").toLowerCase();
    const workdayCompany = deriveWorkdayCompany(hostname);
    if (workdayCompany) return workdayCompany;
    if (hostname.endsWith("greenhouse.io")) {
      const parts = parsed.pathname.split("/").filter(Boolean);
      if (parts.length > 0) {
        const slug = parts[0];
        const cleaned = toTitleCase(slug);
        if (cleaned) return cleaned;
      }
    }

    let baseHost = hostname;
    for (const prefix of [
      "careers.",
      "jobs.",
      "boards.",
      "boards-",
      "job-",
      "boards-",
    ]) {
      if (baseHost.startsWith(prefix)) {
        baseHost = baseHost.slice(prefix.length);
        break;
      }
    }
    const parts = baseHost.split(".").filter(Boolean);
    const name = parts.length >= 2 ? parts[parts.length - 2] : (parts[0] ?? "");
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

const coerceLevelFromHint = (
  hint: string,
): "junior" | "mid" | "senior" | "staff" => {
  const h = hint.toLowerCase();
  if (h.includes("intern")) return "junior";
  if (h.includes("junior")) return "junior";
  if (
    h.includes("staff") ||
    h.includes("principal") ||
    h.includes("lead") ||
    h.includes("director") ||
    h.includes("vp") ||
    h.includes("chief")
  ) {
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
  const addResolvedCandidates = (candidates: string[]) => {
    for (const candidate of candidates) {
      if (!candidate) continue;
      if (!resolveLocationFromDictionary(candidate)) continue;
      if (locationCandidates.includes(candidate)) continue;
      locationCandidates.push(candidate);
    }
  };
  const extractExplicitCityStates = (text: string): string[] => {
    const matches: string[] = [];
    const regex = new RegExp(CITY_STATE_ABBR_RE.source, "g");
    for (const match of text.matchAll(regex)) {
      const city = match.groups?.city?.trim();
      const state = match.groups?.state?.trim();
      if (!city || !state) continue;
      matches.push(`${city}, ${state}`);
    }
    return matches;
  };

  const titleMatch = TITLE_RE.exec(markdown);
  if (titleMatch?.groups?.title) {
    hints.title = titleMatch.groups.title.trim();
  }

  // Location: prefer a short line beneath the header that looks like "City, State".
  const lines = markdown.split(/\r?\n/);
  const remoteCountry = (() => {
    for (const line of lines) {
      const match = line.match(/remote\s*[,–-]\s*([A-Za-z][A-Za-z .'-]+)/i);
      if (!match?.[1]) continue;
      const cleaned = match[1]
        .split(/\s+(?:or|and)\s+|\/|\||;/i)[0]
        .replace(/[).,;]+$/g, "")
        .trim();
      if (!cleaned) continue;
      const inferred = inferCountryFromLocation(cleaned);
      if (inferred) return inferred;
    }
    return null;
  })();

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
  if (remoteCountry) {
    hints.remoteCountry = remoteCountry;
    hints.remote = true;
    if (locationCandidates.length === 0) {
      locationCandidates.push("Remote");
    }
  }
  if (!locationCandidates.length) {
    const locationLine = lines.find((line) =>
      /(?:^|\b)(location|office|based\s+in)\s*[:\-–]/i.test(line),
    );
    if (locationLine) {
      addResolvedCandidates(extractExplicitCityStates(locationLine));
    }
  }
  if (!locationCandidates.length) {
    addResolvedCandidates(extractExplicitCityStates(markdown));
  }
  if (!locationCandidates.length) {
    const cityHit = findCityInText(markdown);
    if (cityHit?.city && cityHit?.state) {
      locationCandidates.push(`${cityHit.city}, ${cityHit.state}`);
    }
  }
  if (!locationCandidates.length) {
    const locMatch =
      LOCATION_RE.exec(markdown) || SIMPLE_LOCATION_LINE_RE.exec(markdown);
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
    } else if (
      token.includes("hybrid") ||
      token.includes("on-site") ||
      token.includes("onsite")
    ) {
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
        const period =
          typeof groups.period === "string" ? groups.period.toLowerCase() : "";
        if (period.includes("hour")) continue;
        const raw = (match[0] || "").toLowerCase();
        if (raw.includes("401k")) continue;

        const low = toInt(groups.low);
        const high = toInt(groups.high);
        const normalizedLow =
          typeof low === "number" ? low * multiplier : undefined;
        const normalizedHigh =
          typeof high === "number" ? high * multiplier : undefined;
        if (typeof normalizedLow === "number") salaryValues.push(normalizedLow);
        if (typeof normalizedHigh === "number")
          salaryValues.push(normalizedHigh);
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
      if (typeof bestRange.high === "number")
        rangePayload.high = bestRange.high;
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

  if (
    hints.title &&
    typeof job.title === "string" &&
    job.title.toLowerCase().startsWith("job application for")
  ) {
    updates.title = hints.title;
  }
  if (
    !updates.title &&
    hints.title &&
    typeof job.title === "string" &&
    job.title !== hints.title
  ) {
    updates.title = hints.title;
  }

  const normalizedLocations = normalizeLocations(
    hints.locations ?? hints.location,
  );
  if (normalizedLocations.length) {
    const locationInfo = deriveLocationFields({
      locations: normalizedLocations,
      location: normalizedLocations[0],
    });
    if (
      !job.location ||
      isUnknownLocationValue(job.location) ||
      job.location !== locationInfo.primaryLocation
    ) {
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
    if (
      locationInfo.locationSearch &&
      job.locationSearch !== locationInfo.locationSearch
    ) {
      updates.locationSearch = locationInfo.locationSearch;
    }
    if ((isUnknownLocationValue(job.city) || !job.city) && locationInfo.city)
      updates.city = locationInfo.city;
    if ((isUnknownLocationValue(job.state) || !job.state) && locationInfo.state)
      updates.state = locationInfo.state;
  }

  const remoteCountry =
    typeof hints.remoteCountry === "string" ? hints.remoteCountry.trim() : "";
  if (remoteCountry) {
    const hasOnlyRemoteLocation =
      normalizedLocations.length === 0 ||
      (normalizedLocations.length === 1 &&
        normalizedLocations[0].toLowerCase() === "remote");
    if (hasOnlyRemoteLocation) {
      const nextLocations = normalizedLocations.length
        ? normalizedLocations
        : ["Remote"];
      if (!arraysEqual(job.locations, nextLocations))
        updates.locations = nextLocations;
      const nextLocationStates = ["Remote"];
      if (!arraysEqual(job.locationStates, nextLocationStates))
        updates.locationStates = nextLocationStates;
      if (job.location !== "Remote") updates.location = "Remote";
      const nextCountries = [remoteCountry];
      if (!arraysEqual(job.countries, nextCountries))
        updates.countries = nextCountries;
      if (job.country !== remoteCountry) updates.country = remoteCountry;
      const nextLocationSearch = `Remote ${remoteCountry}`.trim();
      if (job.locationSearch !== nextLocationSearch)
        updates.locationSearch = nextLocationSearch;
    }
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

  if (
    hints.compensation &&
    (!job.totalCompensation || job.totalCompensation <= 0)
  ) {
    updates.totalCompensation = hints.compensation;
    updates.compensationUnknown = false;
    updates.compensationReason = "parsed from description";
  } else if (
    hints.compensation &&
    job.totalCompensation &&
    job.totalCompensation > 0
  ) {
    // Optionally tighten comp reason if we filled something previously from defaults.
    if (
      !job.compensationReason ||
      job.compensationReason === "compensation provided in scrape payload"
    ) {
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

type JobDetailDoc = Doc<"job_details">;
type JobDetailFields = Omit<JobDetailDoc, "jobId" | "_id">;
type ScrapeQueueInfo = {
  scrapeQueueCreatedAt?: number;
  scrapeQueueCompletedAt?: number;
  scrapeQueueStatus?: string;
};

type JobWithDetails = DbJob &
  Partial<JobDetailFields> &
  ScrapeQueueInfo & {
    descriptionStorageAvailable?: boolean;
  };

const ensureLocationFields = async (
  ctx: any,
  job: DbJob,
  options: { allowPatch?: boolean } = {},
) => {
  const hasLocation =
    typeof job.location === "string" &&
    job.location.trim() &&
    !isUnknownLocationValue(job.location);
  const hasLocations = Array.isArray(job.locations) && job.locations.length > 0;
  const hasLocationStates =
    Array.isArray(job.locationStates) && job.locationStates.length > 0;
  const hasLocationSearch =
    typeof job.locationSearch === "string" &&
    job.locationSearch.trim() &&
    !isUnknownLocationValue(job.locationSearch);
  const hasCity =
    typeof job.city === "string" &&
    job.city.trim() &&
    !isUnknownLocationValue(job.city);
  const hasState =
    typeof job.state === "string" &&
    job.state.trim() &&
    !isUnknownLocationValue(job.state);

  if (
    hasLocation &&
    hasLocations &&
    hasLocationStates &&
    hasLocationSearch &&
    hasCity &&
    hasState
  ) {
    return job;
  }

  const allowPatch = Boolean(
    options.allowPatch && typeof ctx.db?.patch === "function",
  );
  const locationInfo = deriveLocationFields(job);
  const { city, state } = locationInfo;
  const normalizedCity = isUnknownLocationValue(job.city)
    ? locationInfo.city
    : (job.city ?? locationInfo.city);
  const normalizedState = isUnknownLocationValue(job.state)
    ? locationInfo.state
    : (job.state ?? locationInfo.state);
  const locationLabel = formatLocationLabel(
    normalizedCity,
    normalizedState,
    job.location ?? locationInfo.primaryLocation,
  );

  if (!allowPatch) {
    return {
      ...job,
      location: locationLabel,
      locations: locationInfo.locations,
      locationStates: locationInfo.locationStates,
      locationSearch: locationInfo.locationSearch,
      city: normalizedCity,
      state: normalizedState,
    } as DbJob;
  }

  const patched: Record<string, any> = {};
  if ((isUnknownLocationValue(job.city) || !job.city) && city)
    patched.city = city;
  if ((isUnknownLocationValue(job.state) || !job.state) && state)
    patched.state = state;
  if (
    !job.location ||
    isUnknownLocationValue(job.location) ||
    job.location !== locationLabel
  ) {
    patched.location = locationLabel;
  }
  if (
    !Array.isArray(job.locations) ||
    JSON.stringify(job.locations) !== JSON.stringify(locationInfo.locations)
  ) {
    patched.locations = locationInfo.locations;
  }
  if (
    !Array.isArray(job.countries) ||
    JSON.stringify(job.countries) !== JSON.stringify(locationInfo.countries)
  ) {
    patched.countries = locationInfo.countries;
  }
  if (!job.country || job.country !== locationInfo.country) {
    patched.country = locationInfo.country;
  }
  if (
    !Array.isArray(job.locationStates) ||
    JSON.stringify(job.locationStates) !==
    JSON.stringify(locationInfo.locationStates)
  ) {
    patched.locationStates = locationInfo.locationStates;
  }
  if (
    !job.locationSearch ||
    job.locationSearch !== locationInfo.locationSearch
  ) {
    patched.locationSearch = locationInfo.locationSearch;
  }

  if (Object.keys(patched).length > 0) {
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

const getJobDetailsByJobId = async (
  ctx: any,
  jobId: Id<"jobs">,
): Promise<JobDetailDoc | null> => {
  return (await ctx.db
    .query("job_details")
    .withIndex("by_job", (q: any) => q.eq("jobId", jobId))
    .first()) as JobDetailDoc | null;
};

const resolveDescriptionText = async (
  ctx: any,
  job: any,
  details: JobDetailDoc | null,
) => {
  const stored = await loadDescriptionFromStorage(
    ctx,
    (details as any)?.descriptionStorageId,
  );
  if (typeof stored === "string" && stored.trim()) return stored;
  if (typeof details?.description === "string") return details.description;
  if (typeof (job as any)?.description === "string")
    return (job as any).description;
  return "";
};

const countAppliedApplications = async (
  ctx: any,
  jobIds: Array<Id<"jobs"> | string>,
) => {
  const unique = Array.from(
    new Set(jobIds.map((id) => String(id)).filter(Boolean)),
  );
  if (unique.length === 0) return 0;

  const counts = await Promise.all(
    unique.map(async (jobId) => {
      const applications = await ctx.db
        .query("applications")
        .withIndex("by_job", (q: any) => q.eq("jobId", jobId))
        .filter((q: any) => q.eq(q.field("status"), "applied"))
        .collect();
      return applications.length;
    }),
  );

  return counts.reduce((sum, count) => sum + count, 0);
};

const mergeJobDetails = (
  job: DbJob,
  details: JobDetailDoc | null,
): JobWithDetails => {
  if (!details) return job;
  const {
    jobId: _jobId,
    _id: _detailId,
    descriptionStorageId: _storageId,
    ...detailFields
  } = details;
  return {
    ...job,
    ...detailFields,
    descriptionStorageAvailable: Boolean(
      (details as any)?.descriptionStorageId,
    ),
  };
};

export const computeJobCountry = (
  job: DbJob,
  locationInfo?: ReturnType<typeof deriveLocationFields>,
) => {
  const explicitCountry = job.country?.trim();
  if (explicitCountry) {
    return explicitCountry;
  }

  const resolvedLocation =
    locationInfo ??
    (Array.isArray(job.countries) ||
      Array.isArray(job.locationStates) ||
      job.state
      ? null
      : deriveLocationFields(job));
  const locationCountries = Array.isArray(job.countries)
    ? job.countries
    : (resolvedLocation?.countries ?? []);

  const primaryCountry = locationCountries.find((c) => c && c !== "Unknown");
  if (primaryCountry && primaryCountry !== "Other") {
    return primaryCountry;
  }

  const locationCandidates: string[] = [];
  if (typeof job.location === "string" && job.location.trim()) {
    locationCandidates.push(job.location);
  }
  if (Array.isArray(job.locations) && job.locations.length) {
    locationCandidates.push(...job.locations);
  }
  if (resolvedLocation?.primaryLocation) {
    locationCandidates.push(resolvedLocation.primaryLocation);
  }

  for (const loc of locationCandidates) {
    const inferred = inferCountryFromLocation(loc);
    if (inferred) {
      return inferred === "Other" ? "Unknown" : inferred;
    }
  }

  const locationStates = Array.isArray(job.locationStates)
    ? job.locationStates
    : job.state
      ? [job.state]
      : (resolvedLocation?.locationStates ?? []);

  for (const state of locationStates) {
    const inferred = inferCountryFromLocation(state);
    if (inferred && inferred !== "Other") {
      return inferred;
    }
  }

  const hasNonUnknownState = locationStates.some(
    (state) => state && state !== "Unknown" && state !== "Remote",
  );
  if (hasNonUnknownState) {
    return "United States";
  }

  if (primaryCountry === "Other") {
    return "Unknown";
  }

  return "Unknown";
};

const normalizeKeyPart = (value?: string | null) =>
  (value ?? "").trim().toLowerCase();
const normalizeCompanyKey = (value?: string | null) =>
  (value ?? "").trim().toLowerCase();

const COMPANY_SUFFIXES = new Set([
  "inc",
  "incorporated",
  "corp",
  "corporation",
  "co",
  "company",
  "llc",
  "llp",
  "ltd",
  "limited",
  "plc",
]);

export const normalizeCompanyFilterKey = (value?: string | null) => {
  const cleaned = (value ?? "")
    .toLowerCase()
    .replace(/['’]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
  if (!cleaned) return "";

  const tokens = cleaned.split(/\s+/);
  const maybeStripJoinedSuffix = () => {
    for (const size of [3, 2]) {
      if (tokens.length <= size) continue;
      const tail = tokens.slice(-size);
      if (!tail.every((token) => token.length === 1)) continue;
      const joined = tail.join("");
      if (!COMPANY_SUFFIXES.has(joined)) continue;
      tokens.splice(-size);
      return true;
    }
    return false;
  };

  while (tokens.length > 1) {
    const last = tokens[tokens.length - 1];
    if (COMPANY_SUFFIXES.has(last)) {
      tokens.pop();
      continue;
    }
    if (maybeStripJoinedSuffix()) {
      continue;
    }
    break;
  }

  return tokens.join("");
};

export const deriveCompanyKey = (value?: string | null) => {
  const normalized = normalizeCompanyFilterKey(value);
  if (normalized) return normalized;
  return (value ?? "").trim().toLowerCase();
};

type CompanySummary = {
  name: string;
  count: number;
  avgCompensationJunior: number | null;
  avgCompensationMid: number | null;
  avgCompensationSenior: number | null;
  currencyCode: string | null;
  sampleUrl: string | null;
  lastPostedAt: number;
  lastScrapedAt: number;
};

type CompanyLevelStats = {
  sum: number;
  count: number;
};

const emptyCompanyLevelStats = (): CompanyLevelStats => ({ sum: 0, count: 0 });
const averageFromStats = (stats: CompanyLevelStats) =>
  stats.count > 0 ? Math.round(stats.sum / stats.count) : null;
const normalizeUsdCurrency = (value: unknown): string | null => {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const upper = trimmed.toUpperCase();
  if (upper === "USD" || upper === "US$" || upper === "$" || upper === "USD$") {
    return "USD";
  }
  return null;
};

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
    if (
      parsed &&
      typeof parsed === "object" &&
      ("raw" in parsed || "carry" in parsed || "done" in parsed)
    ) {
      return {
        rawCursor: typeof parsed.raw === "string" ? parsed.raw : null,
        carryIds: Array.isArray(parsed.carry)
          ? parsed.carry.filter((id): id is string => typeof id === "string")
          : [],
        rawIsDone: typeof parsed.done === "boolean" ? parsed.done : false,
      };
    }
  } catch {
    // Not our cursor format.
  }

  return { rawCursor: cursor, carryIds: [] as string[], rawIsDone: false };
};

const buildFilterCursor = (
  rawCursor: string | null,
  carryIds: string[],
  rawIsDone: boolean,
) =>
  JSON.stringify({
    raw: rawCursor ?? null,
    carry: carryIds,
    done: rawIsDone,
  } satisfies FilterCursorPayload);

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
    const parsed = new URL(
      trimmed.includes("://") ? trimmed : `https://${trimmed}`,
    );
    const host = parsed.hostname.toLowerCase();
    if (host.endsWith(WORKDAY_HOST_SUFFIX)) return host;
    const greenhouseSlug = greenhouseSlugFromUrl(parsed.href);
    if (greenhouseSlug) return `${greenhouseSlug}.greenhouse.io`;
    const ashbySlug = ashbySlugFromUrl(parsed.href);
    if (ashbySlug) return `${ashbySlug}.ashbyhq.com`;
    return baseDomainFromHost(host);
  } catch {
    const hostOnly =
      trimmed.replace(/^https?:\/\//i, "").split("/")[0] || trimmed;
    const host = hostOnly.toLowerCase();
    if (host.endsWith(WORKDAY_HOST_SUFFIX)) return host;
    const greenhouseSlug = greenhouseSlugFromUrl(host);
    if (greenhouseSlug) return `${greenhouseSlug}.greenhouse.io`;
    const ashbySlug = ashbySlugFromUrl(trimmed);
    if (ashbySlug) return `${ashbySlug}.ashbyhq.com`;
    return baseDomainFromHost(host);
  }
};

export const matchesCompanyFilters = (
  job: {
    company?: string | null;
    companyKey?: string | null;
    url?: string | null;
  },
  normalizedCompanyFilters: Set<string>,
  domainAliasByDomain?: Map<string, string> | null,
) => {
  if (!normalizedCompanyFilters.size) return true;
  const jobCompanyKey =
    typeof job.companyKey === "string" && job.companyKey.trim()
      ? normalizeCompanyFilterKey(job.companyKey)
      : normalizeCompanyFilterKey(job.company);
  if (jobCompanyKey && normalizedCompanyFilters.has(jobCompanyKey)) return true;
  if (!domainAliasByDomain || domainAliasByDomain.size === 0) return false;
  const domain = normalizeDomainInput(job.url ?? "");
  if (!domain) return false;
  const aliasKey = normalizeCompanyFilterKey(
    domainAliasByDomain.get(domain) ?? "",
  );
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

const mergeStrings = (
  ...candidates: Array<string | string[] | null | undefined>
) => {
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
    (job) =>
      job.compensationUnknown !== true &&
      typeof job.totalCompensation === "number" &&
      job.totalCompensation > 0,
  );

  if (withKnownComp.length === 0) return null;

  return withKnownComp.sort(
    (a, b) => (b.totalCompensation ?? 0) - (a.totalCompensation ?? 0),
  )[0];
};

export const matchesCountryFilter = (
  jobCountry: string,
  countryFilter: string,
  isOtherCountry: boolean,
) => {
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
    const {
      city,
      state,
      primaryLocation,
      locations,
      locationStates,
      locationSearch,
      countries,
      country,
    } = locationInfo;
    const locationLabel = formatLocationLabel(city, state, primaryLocation);
    const update: Record<string, any> = {};
    if (job.city !== city) update.city = city;
    if (job.state !== state) update.state = state;
    if (job.location !== locationLabel) update.location = locationLabel;
    if (
      !Array.isArray(job.locations) ||
      JSON.stringify(job.locations) !== JSON.stringify(locations)
    ) {
      update.locations = locations;
    }
    if (
      !Array.isArray(job.countries) ||
      JSON.stringify(job.countries) !== JSON.stringify(countries)
    ) {
      update.countries = countries;
    }
    if (job.country !== country) {
      update.country = country;
    }
    if (
      !Array.isArray(job.locationStates) ||
      JSON.stringify(job.locationStates) !== JSON.stringify(locationStates)
    ) {
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
    level: v.optional(
      v.union(
        v.literal("junior"),
        v.literal("mid"),
        v.literal("senior"),
        v.literal("staff"),
      ),
    ),
    minCompensation: v.optional(v.number()),
    maxCompensation: v.optional(v.number()),
    hideUnknownCompensation: v.optional(v.boolean()),
    companies: v.optional(v.array(v.string())),
    useSearch: v.optional(v.boolean()),
    engineer: v.optional(v.boolean()),
    excludeApplied: v.optional(v.boolean()),
    sortBy: v.optional(v.union(v.literal("posted"), v.literal("scraped"))),
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
    const wantsEngineer = args.engineer === true;
    const sortByScraped = args.sortBy === "scraped";

    const companyFilters = (args.companies ?? [])
      .map((c) => c.trim())
      .filter(Boolean);
    const normalizedCompanyFilters = new Set(
      companyFilters.map((c) => normalizeCompanyFilterKey(c)).filter(Boolean),
    );
    const hasCompanyFilter = normalizedCompanyFilters.size > 0;
    const singleCompanyFilter =
      companyFilters.length === 1 ? companyFilters[0] : null;
    const singleCompanyKey = singleCompanyFilter
      ? normalizeCompanyFilterKey(singleCompanyFilter)
      : "";
    const requestedPageSize = args.paginationOpts.numItems ?? 50;
    const shouldExcludeApplied = args.excludeApplied !== false;
    const hasAppliedApplications = shouldExcludeApplied
      ? Boolean(
        await ctx.db
          .query("applications")
          .withIndex("by_user_status_applied_at", (q) =>
            q.eq("userId", userId).eq("status", "applied"),
          )
          .first(),
      )
      : false;
    const hasRejectedApplications =
      shouldExcludeApplied && !hasAppliedApplications
        ? Boolean(
          await ctx.db
            .query("applications")
            .withIndex("by_user_status_applied_at", (q) =>
              q.eq("userId", userId).eq("status", "rejected"),
            )
            .first(),
        )
        : false;
    const hasUserApplications = shouldExcludeApplied
      ? hasAppliedApplications || hasRejectedApplications
      : false;
    const maxPageSize = hasCompanyFilter || hasUserApplications ? 25 : 50;
    const pageSize = Math.max(1, Math.min(requestedPageSize, maxPageSize));
    const paginationOpts = { ...args.paginationOpts, numItems: pageSize };

    let domainAliasLookup: Map<string, string> | null = null;
    if (hasCompanyFilter) {
      const aliasRows = await ctx.db.query("domain_aliases").collect();
      domainAliasLookup = new Map();
      for (const row of aliasRows as any[]) {
        const domain = row?.domain?.trim?.() ?? "";
        const alias = normalizeCompanyFilterKey(row?.alias ?? "");
        if (domain && alias) {
          domainAliasLookup.set(domain, alias);
        }
      }
    }

    const jobPassesFilters = (job: any) => {
      if (args.includeRemote === false && job.remote) {
        return false;
      }
      if (wantsEngineer) {
        const isEngineer =
          typeof job.engineer === "boolean"
            ? job.engineer
            : deriveEngineerFlag(job.title);
        if (!isEngineer) return false;
      }
      if (hasCompanyFilter) {
        if (
          !matchesCompanyFilters(
            job,
            normalizedCompanyFilters,
            domainAliasLookup,
          )
        ) {
          return false;
        }
      }

      // Apply compensation filters
      const compensationUnknown = job.compensationUnknown === true;
      const compValue =
        typeof job.totalCompensation === "number" ? job.totalCompensation : 0;
      if (args.hideUnknownCompensation && compensationUnknown) {
        return false;
      }
      if (
        args.minCompensation !== undefined &&
        !compensationUnknown &&
        compValue < args.minCompensation
      ) {
        return false;
      }
      if (
        args.maxCompensation !== undefined &&
        !compensationUnknown &&
        compValue > args.maxCompensation
      ) {
        return false;
      }

      if (!stateFilter && !hasCountryFilter) {
        return true;
      }

      let locationInfo: ReturnType<typeof deriveLocationFields> | null = null;
      const getLocationInfo = () => {
        if (!locationInfo) {
          locationInfo = deriveLocationFields(job);
        }
        return locationInfo;
      };

      if (stateFilter) {
        const statesForFilter =
          Array.isArray(job.locationStates) && job.locationStates.length
            ? job.locationStates
            : job.state
              ? [job.state]
              : (() => {
                const info = getLocationInfo();
                return info.locationStates.length
                  ? info.locationStates
                  : [info.state];
              })();
        if (!statesForFilter.includes(stateFilter)) return false;
      }
      if (hasCountryFilter) {
        const jobCountry = computeJobCountry(job, locationInfo ?? undefined);
        if (!matchesCountryFilter(jobCountry, countryFilter, isOtherCountry)) {
          return false;
        }
      }
      return true;
    };

    let appliedJobIds: Set<string> | null = null;
    const loadAppliedJobIds = async () => {
      if (appliedJobIds) return appliedJobIds;

      // Use the denormalized index for O(1) lookup instead of O(N) application queries
      const indexDoc = await ctx.db
        .query("user_application_index")
        .withIndex("by_user", (q: any) => q.eq("userId", userId))
        .unique();

      if (indexDoc) {
        // Fast path: read from single denormalized document
        appliedJobIds = new Set([
          ...(indexDoc.appliedJobIds || []),
          ...(indexDoc.rejectedJobIds || []),
        ]);
      } else {
        // Fallback for users without index (pre-migration or new users with no applications)
        const [appliedRows, rejectedRows] = await Promise.all([
          ctx.db
            .query("applications")
            .withIndex("by_user_status_applied_at", (q: any) =>
              q.eq("userId", userId).eq("status", "applied"),
            )
            .collect(),
          ctx.db
            .query("applications")
            .withIndex("by_user_status_applied_at", (q: any) =>
              q.eq("userId", userId).eq("status", "rejected"),
            )
            .collect(),
        ]);
        appliedJobIds = new Set(
          [...appliedRows, ...rejectedRows].map((row: any) => String(row.jobId)),
        );
      }
      return appliedJobIds;
    };
    const filterOutAppliedJobs = async (jobsToFilter: any[]) => {
      if (
        !shouldExcludeApplied ||
        !hasUserApplications ||
        jobsToFilter.length === 0
      )
        return jobsToFilter;
      const appliedIds = await loadAppliedJobIds();
      return jobsToFilter.filter((job) => !appliedIds.has(String(job._id)));
    };

    // Apply search and filters
    let jobs;
    let jobsAlreadyFiltered = false;
    let applicationsFiltered = false;
    if (shouldUseSearch) {
      const SEARCH_LIMIT = 100;
      const matches = await ctx.db
        .query("jobs")
        .withSearchIndex("search_title", (q: any) => {
          let searchQuery = q.search("title", rawSearch);
          if (wantsEngineer) {
            searchQuery = searchQuery.eq("engineer", true);
          }
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

      const compareFn = sortByScraped ? compareScrapedJobs : compareNewestJobs;

      jobs = {
        page: matches.sort(compareFn),
        isDone: true,
        continueCursor: null,
      };
    } else if (stateFilter && !sortByScraped) {
      const SEARCH_LIMIT = 200;
      const matches = await ctx.db
        .query("jobs")
        .withSearchIndex("search_locations", (q: any) => {
          let searchQuery = q.search("locationSearch", stateFilter);
          if (wantsEngineer) {
            searchQuery = searchQuery.eq("engineer", true);
          }
          if (args.includeRemote === false) {
            searchQuery = searchQuery.eq("remote", false);
          }
          if (args.level) {
            searchQuery = searchQuery.eq("level", args.level);
          }
          return searchQuery;
        })
        .take(SEARCH_LIMIT);

      const fallbackCandidates = await ctx.db
        .query("jobs")
        .withIndex("by_posted_scraped")
        .order("desc")
        .take(SEARCH_LIMIT);
      const combined = new Map<string, any>();
      for (const job of matches) {
        combined.set(String(job._id), job);
      }
      for (const job of fallbackCandidates) {
        const locationInfo = deriveLocationFields(job);
        const statesForFilter = locationInfo.locationStates.length
          ? locationInfo.locationStates
          : [locationInfo.state];
        if (wantsEngineer) {
          const isEngineer =
            typeof job.engineer === "boolean"
              ? job.engineer
              : deriveEngineerFlag(job.title);
          if (!isEngineer) continue;
        }
        if (args.includeRemote === false && job.remote) continue;
        if (args.level && job.level !== args.level) continue;
        if (statesForFilter.includes(stateFilter)) {
          combined.set(String(job._id), job);
        }
      }

      jobs = {
        page: Array.from(combined.values()).sort(compareNewestJobs),
        isDone: true,
        continueCursor: null,
      };
    } else {
      const buildBaseQuery = () => {
        let query: any = ctx.db.query("jobs");

        if (sortByScraped) {
          if (wantsEngineer) {
            query = query.withIndex("by_engineer_scraped_posted", (q: any) =>
              q.eq("engineer", true),
            );
          } else {
            query = query.withIndex("by_scraped_posted");
          }
        } else {
          if (stateFilter) {
            query = query.withIndex("by_state_posted", (q: any) =>
              q.eq("state", args.state),
            );
          } else if (singleCompanyKey) {
            query = query.withIndex("by_company_key_posted", (q: any) =>
              q.eq("companyKey", singleCompanyKey),
            );
          } else if (wantsEngineer) {
            query = query.withIndex("by_engineer_posted_scraped", (q: any) =>
              q.eq("engineer", true),
            );
          } else {
            query = query.withIndex("by_posted_scraped");
          }
        }

        query = query.order("desc");

        if (wantsEngineer && stateFilter && !sortByScraped) {
          query = query.filter((q: any) => q.eq(q.field("engineer"), true));
        }
        if (args.includeRemote === false) {
          query = query.filter((q: any) => q.eq(q.field("remote"), false));
        }
        if (args.level) {
          query = query.filter((q: any) => q.eq(q.field("level"), args.level));
        }
        if (stateFilter && sortByScraped) {
          query = query.filter((q: any) => q.eq(q.field("state"), args.state));
        }
        if (rawSearch && args.state) {
          query = query.filter((q: any) => q.eq(q.field("state"), args.state));
        }

        return query;
      };

      const needsFilteredPagination =
        hasUserApplications ||
        hasCompanyFilter ||
        hasCountryFilter ||
        args.hideUnknownCompensation === true ||
        args.minCompensation !== undefined ||
        args.maxCompensation !== undefined;

      if (!needsFilteredPagination) {
        jobs = await buildBaseQuery().paginate(paginationOpts);
      } else {
        const {
          rawCursor: initialRawCursor,
          carryIds,
          rawIsDone: initialRawIsDone,
        } = parseFilterCursor(paginationOpts.cursor);
        let rawCursor = initialRawCursor;
        let rawIsDone = initialRawIsDone;
        const filteredBuffer: any[] = [];

        if (carryIds.length > 0) {
          const carryJobs = await Promise.all(
            carryIds.map((id) => ctx.db.get(id as Id<"jobs">)),
          );
          const carryMatches = carryJobs.filter(
            (job) => job && jobPassesFilters(job),
          );
          const carryWithoutApplied = await filterOutAppliedJobs(carryMatches);
          filteredBuffer.push(...carryWithoutApplied);
        }

        if (!rawIsDone && filteredBuffer.length < pageSize) {
          const expandedSize = Math.min(pageSize * 2, 100);
          const page = await buildBaseQuery().paginate({
            ...paginationOpts,
            cursor: rawCursor,
            numItems: expandedSize,
          });
          rawCursor = page.continueCursor;
          rawIsDone = page.isDone;
          if (page.page.length) {
            const orderedPage = [...page.page].sort(sortByScraped ? compareScrapedJobs : compareNewestJobs);
            const pageMatches = orderedPage.filter(jobPassesFilters);
            const pageWithoutApplied = await filterOutAppliedJobs(pageMatches);
            filteredBuffer.push(...pageWithoutApplied);
          }
        }

        const pageJobs = filteredBuffer.slice(0, pageSize);
        const carryOverIds = filteredBuffer
          .slice(pageSize)
          .map((job: any) => String(job._id));
        const isDone = rawIsDone && carryOverIds.length === 0;
        const continueCursor = isDone
          ? null
          : buildFilterCursor(rawCursor, carryOverIds, rawIsDone);

        jobs = {
          page: pageJobs,
          isDone,
          continueCursor,
        };
        jobsAlreadyFiltered = true;
        applicationsFiltered = true;
      }
    }

    // Ensure descending order by postedAt then scrapedAt for all paths,
    // with unknown postedAt entries pushed after known ones when scrapedAt matches.
    const orderedPage = [...jobs.page].sort(sortByScraped ? compareScrapedJobs : compareNewestJobs);

    // Apply remaining filters and then exclude any applied/rejected jobs as needed.
    const filteredJobs = jobsAlreadyFiltered
      ? orderedPage
      : orderedPage.filter(jobPassesFilters);
    const appliedFilteredJobs = applicationsFiltered
      ? filteredJobs
      : await filterOutAppliedJobs(filteredJobs);

    // Group jobs with same title/company/level/remote into one row, merging locations and URLs
    const grouped = new Map<string, { base: any; members: any[] }>();

    for (const job of appliedFilteredJobs) {
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
        const {
          description: _description,
          locationSearch: _locationSearch,
          ...listBase
        } = normalizedBase;

        const allLocations = mergeStrings(
          normalizedBase.locations,
          members.flatMap((m) =>
            Array.isArray(m.locations) ? m.locations : [],
          ),
          members.map((m) => m.location),
        );

        const locationStatesMerged = Array.from(
          new Set(
            members
              .flatMap((m) => {
                if (
                  Array.isArray(m.locationStates) &&
                  m.locationStates.length
                ) {
                  return m.locationStates;
                }
                if (m.state) {
                  return [m.state];
                }
                const info = deriveLocationFields(m);
                return info.locationStates.length
                  ? info.locationStates
                  : [info.state];
              })
              .filter(Boolean),
          ),
        );

        const urls = Array.from(
          new Set(members.map((m) => m.url).filter(Boolean)),
        );

        return {
          ...listBase,
          totalCompensation: compJob.totalCompensation,
          compensationUnknown: compJob.compensationUnknown,
          compensationReason: compJob.compensationReason,
          locations: allLocations,
          locationStates: locationStatesMerged,
          url: urls[0],
          alternateUrls: urls,
          groupedJobIds: members.map((m) => m._id),
          applicationCount: 0,
          userStatus: null, // These jobs don't have user applications by definition
        } as any;
      }),
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
        .withSearchIndex("search_company", (q) =>
          q.search("company", searchTerm),
        )
      : ctx.db.query("jobs").withIndex("by_posted_scraped").order("desc");

    const matches = await baseQuery.take(200);
    const counts = new Map<string, { name: string; count: number }>();

    for (const job of matches) {
      const companyName =
        typeof (job as any).company === "string"
          ? (job as any).company.trim()
          : "";
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

export const refreshCompanySummaries = internalMutation({
  args: {},
  handler: async (ctx) => {
    const jobs = await ctx.db.query("jobs").collect();
    const summaries = new Map<
      string,
      {
        name: string;
        count: number;
        currencyCode: string | null;
        sampleUrl: string | null;
        lastPostedAt: number;
        lastScrapedAt: number;
        levels: Record<"junior" | "mid" | "senior", CompanyLevelStats>;
      }
    >();

    for (const job of jobs as Doc<"jobs">[]) {
      const rawCompany =
        typeof job.company === "string" ? job.company.trim() : "";
      const companyName = rawCompany.replace(/\s+/g, " ").trim();
      if (!companyName || isUnknownLabel(companyName)) continue;

      const key =
        typeof job.companyKey === "string" && job.companyKey.trim()
          ? job.companyKey.trim()
          : deriveCompanyKey(companyName);
      if (!key) continue;

      let entry = summaries.get(key);
      if (!entry) {
        entry = {
          name: companyName,
          count: 0,
          currencyCode: null,
          sampleUrl: null,
          lastPostedAt: 0,
          lastScrapedAt: 0,
          levels: {
            junior: emptyCompanyLevelStats(),
            mid: emptyCompanyLevelStats(),
            senior: emptyCompanyLevelStats(),
          },
        };
        summaries.set(key, entry);
      } else if (companyName && companyName.length < entry.name.length) {
        entry.name = companyName;
      }

      entry.count += 1;
      if (!entry.sampleUrl && typeof job.url === "string" && job.url.trim()) {
        entry.sampleUrl = job.url.trim();
      }
      if (
        typeof job.postedAt === "number" &&
        job.postedAt > entry.lastPostedAt
      ) {
        entry.lastPostedAt = job.postedAt;
      }
      const scrapedAt =
        typeof job.scrapedAt === "number"
          ? job.scrapedAt
          : typeof job._creationTime === "number"
            ? job._creationTime
            : 0;
      if (scrapedAt > entry.lastScrapedAt) {
        entry.lastScrapedAt = scrapedAt;
      }

      const compensationUnknown = job.compensationUnknown === true;
      const compValue =
        typeof job.totalCompensation === "number"
          ? job.totalCompensation
          : null;
      const usdCurrency = normalizeUsdCurrency(job.currencyCode);
      if (
        !compensationUnknown &&
        compValue &&
        Number.isFinite(compValue) &&
        compValue > 0 &&
        usdCurrency
      ) {
        if (!entry.currencyCode) {
          entry.currencyCode = usdCurrency;
        }
        if (
          job.level === "junior" ||
          job.level === "mid" ||
          job.level === "senior"
        ) {
          const stats = entry.levels[job.level];
          stats.sum += compValue;
          stats.count += 1;
        }
      }
    }

    const now = Date.now();
    const existing = (await ctx.db
      .query("company_summaries")
      .collect()) as Doc<"company_summaries">[];
    const existingByKey = new Map(existing.map((row) => [row.key, row]));
    const seen = new Set<string>();
    let inserted = 0;
    let updated = 0;
    let deleted = 0;

    for (const [key, entry] of summaries) {
      seen.add(key);
      const avgJunior = averageFromStats(entry.levels.junior);
      const avgMid = averageFromStats(entry.levels.mid);
      const avgSenior = averageFromStats(entry.levels.senior);
      const payload = {
        key,
        name: entry.name,
        count: entry.count,
        sampleUrl: entry.sampleUrl ?? undefined,
        currencyCode: entry.currencyCode ?? undefined,
        avgCompensationJunior: avgJunior ?? undefined,
        avgCompensationMid: avgMid ?? undefined,
        avgCompensationSenior: avgSenior ?? undefined,
        lastPostedAt: entry.lastPostedAt || undefined,
        lastScrapedAt: entry.lastScrapedAt || undefined,
        updatedAt: now,
      };
      const existingRow = existingByKey.get(key);
      if (existingRow) {
        await ctx.db.patch(existingRow._id, payload);
        updated += 1;
      } else {
        await ctx.db.insert("company_summaries", payload);
        inserted += 1;
      }
    }

    for (const row of existing) {
      if (!seen.has(row.key)) {
        await ctx.db.delete(row._id);
        deleted += 1;
      }
    }

    return { inserted, updated, deleted, total: summaries.size };
  },
});

export const listCompanySummaries = query({
  args: {
    limit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const userId = await getAuthUserId(ctx);
    if (!userId) {
      throw new Error("Not authenticated");
    }

    const limit = Math.max(1, Math.min(args.limit ?? 200, 1000));
    const summaries = (await ctx.db
      .query("company_summaries")
      .collect()) as Doc<"company_summaries">[];

    return summaries
      .map((row) => ({
        name: row.name,
        count: row.count,
        avgCompensationJunior:
          typeof row.avgCompensationJunior === "number"
            ? row.avgCompensationJunior
            : null,
        avgCompensationMid:
          typeof row.avgCompensationMid === "number"
            ? row.avgCompensationMid
            : null,
        avgCompensationSenior:
          typeof row.avgCompensationSenior === "number"
            ? row.avgCompensationSenior
            : null,
        currencyCode:
          typeof row.currencyCode === "string" ? row.currencyCode : null,
        sampleUrl: typeof row.sampleUrl === "string" ? row.sampleUrl : null,
        lastPostedAt:
          typeof row.lastPostedAt === "number" ? row.lastPostedAt : 0,
        lastScrapedAt:
          typeof row.lastScrapedAt === "number" ? row.lastScrapedAt : 0,
      }))
      .sort((a, b) => {
        if (b.lastPostedAt !== a.lastPostedAt)
          return b.lastPostedAt - a.lastPostedAt;
        if (b.lastScrapedAt !== a.lastScrapedAt)
          return b.lastScrapedAt - a.lastScrapedAt;
        return a.name.localeCompare(b.name);
      })
      .slice(0, limit) as CompanySummary[];
  },
});

// Helper to update the denormalized user_application_index
// This keeps a single document per user with all applied/rejected job IDs
// for O(1) lookup in listJobs instead of O(N) application queries
async function updateUserApplicationIndex(
  ctx: any,
  userId: Id<"users">,
  action: "add_applied" | "add_rejected" | "remove" | "move_to_rejected",
  jobId: Id<"jobs">,
) {
  const jobIdStr = String(jobId);

  // Get or create the index document
  const existing = await ctx.db
    .query("user_application_index")
    .withIndex("by_user", (q: any) => q.eq("userId", userId))
    .unique();

  if (!existing) {
    // Create new index document
    const appliedJobIds = action === "add_applied" ? [jobIdStr] : [];
    const rejectedJobIds = action === "add_rejected" || action === "move_to_rejected" ? [jobIdStr] : [];
    await ctx.db.insert("user_application_index", {
      userId,
      appliedJobIds,
      rejectedJobIds,
      updatedAt: Date.now(),
    });
    return;
  }

  // Update existing index document
  const appliedSet = new Set(existing.appliedJobIds || []);
  const rejectedSet = new Set(existing.rejectedJobIds || []);

  switch (action) {
    case "add_applied":
      appliedSet.add(jobIdStr);
      rejectedSet.delete(jobIdStr); // In case it was rejected before
      break;
    case "add_rejected":
      rejectedSet.add(jobIdStr);
      appliedSet.delete(jobIdStr); // In case it was applied before
      break;
    case "move_to_rejected":
      appliedSet.delete(jobIdStr);
      rejectedSet.add(jobIdStr);
      break;
    case "remove":
      appliedSet.delete(jobIdStr);
      rejectedSet.delete(jobIdStr);
      break;
  }

  await ctx.db.patch(existing._id, {
    appliedJobIds: Array.from(appliedSet),
    rejectedJobIds: Array.from(rejectedSet),
    updatedAt: Date.now(),
  });
}

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
      .withIndex("by_user_and_job", (q) =>
        q.eq("userId", userId).eq("jobId", args.jobId),
      )
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

    // Update the denormalized index for O(1) filtering in listJobs
    await updateUserApplicationIndex(ctx, userId, "add_applied", args.jobId);

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
      .withIndex("by_user_and_job", (q) =>
        q.eq("userId", userId).eq("jobId", args.jobId),
      )
      .unique();

    if (existingApplication) {
      await ctx.db.patch(existingApplication._id, { status: "rejected" });
      // If was previously applied, move to rejected; otherwise just ensure it's in rejected
      const action = existingApplication.status === "applied" ? "move_to_rejected" : "add_rejected";
      await updateUserApplicationIndex(ctx, userId, action, args.jobId);
    } else {
      await ctx.db.insert("applications", {
        userId,
        jobId: args.jobId,
        status: "rejected",
        appliedAt: Date.now(),
      });
      // Update the denormalized index for O(1) filtering in listJobs
      await updateUserApplicationIndex(ctx, userId, "add_rejected", args.jobId);
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
    const description = await resolveDescriptionText(ctx, job, details);
    const hints = parseMarkdownHints(description);
    const updates = buildUpdatesFromHints(job, hints);
    const derivedCompany = deriveCompanyFromUrl(job.url || "");
    if (derivedCompany && shouldOverrideCompany(job.company)) {
      updates.company = derivedCompany;
      updates.companyKey = deriveCompanyKey(derivedCompany);
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
      const description = await resolveDescriptionText(ctx, job, details);
      const hints = parseMarkdownHints(description);
      const updates = buildUpdatesFromHints(job as any, hints);
      const derivedCompany = deriveCompanyFromUrl((job as any).url || "");
      if (derivedCompany && shouldOverrideCompany((job as any).company)) {
        updates.company = derivedCompany;
        updates.companyKey = deriveCompanyKey(derivedCompany);
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
      const rows = await ctx.db
        .query("jobs")
        .withIndex("by_company", (q: any) => q.eq("company", label))
        .take(1000);

      scanned += rows.length;
      for (const job of rows) {
        const derived = deriveCompanyFromUrl((job as any).url || "");
        if (!derived || derived === (job as any).company) continue;
        await ctx.db.patch(job._id, {
          company: derived,
          companyKey: deriveCompanyKey(derived),
        });
        updated += 1;
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
      .withIndex("by_posted_scraped")
      .order("desc")
      .take(20); // Increased from 10 to show more recent jobs

    const normalized = await Promise.all(
      jobs.map((job: any) => ensureLocationFields(ctx, job)),
    );
    return normalized;
  },
});

export const listJobsByScrapedAt = query({
  args: {
    scrapedAfter: v.number(),
    limit: v.optional(v.number()),
  },
  returns: v.array(
    v.object({
      _id: v.id("jobs"),
      url: v.string(),
      scrapedAt: v.number(),
    }),
  ),
  handler: async (ctx, args) => {
    const limit = Math.max(1, Math.min(args.limit ?? 200, 2000));
    const jobs = await ctx.db
      .query("jobs")
      .withIndex("by_scraped_at", (q) => q.gte("scrapedAt", args.scrapedAfter))
      .order("desc")
      .take(limit);

    return jobs
      .filter((job) => typeof job.scrapedAt === "number")
      .map((job) => ({
        _id: job._id,
        url: job.url,
        scrapedAt: job.scrapedAt as number,
      }));
  },
});

export const listQueuedJobs = query({
  args: {
    paginationOpts: paginationOptsValidator,
    status: v.optional(
      v.union(
        v.literal("pending"),
        v.literal("processing"),
        v.literal("completed"),
        v.literal("failed"),
        v.literal("invalid"),
      ),
    ),
    scheduledBefore: v.optional(v.number()),
  },
  returns: v.object({
    page: v.array(
      v.object({
        _id: v.string(),
        url: v.string(),
        sourceUrl: v.string(),
        provider: v.optional(v.string()),
        siteId: v.optional(v.id("sites")),
        pattern: v.optional(v.string()),
        urlType: v.optional(v.union(v.literal("listing"), v.literal("detail"))),
        bucket: v.optional(v.number()),
        status: v.union(
          v.literal("pending"),
          v.literal("processing"),
          v.literal("completed"),
          v.literal("failed"),
          v.literal("invalid"),
        ),
        attempts: v.optional(v.number()),
        lastError: v.optional(v.string()),
        createdAt: v.optional(v.number()),
        updatedAt: v.optional(v.number()),
        completedAt: v.optional(v.number()),
        scheduledAt: v.optional(v.number()),
      }),
    ),
    isDone: v.boolean(),
    continueCursor: v.union(v.string(), v.null()),
  }),
  handler: async (_ctx, args) => {
    const requested = args.paginationOpts.numItems ?? 20;
    const limit = Math.min(Math.max(requested, 1), 20);
    return {
      page: [] as any[],
      isDone: true,
      continueCursor: null,
    };
  },
});

export const resetQueuedUrlRetries = mutation({
  args: {
    id: v.string(),
  },
  returns: v.object({ success: v.boolean(), skipped: v.boolean() }),
  handler: async (_ctx, _args) => {
    return { success: true, skipped: true };
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
      .withIndex("by_user_status_applied_at", (q) =>
        q.eq("userId", userId).eq("status", "applied"),
      )
      .order("desc")
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
          workerUpdatedAt:
            workerStatus?.updatedAt ?? workerStatus?.queuedAt ?? null,
        };
      }),
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
      .withIndex("by_user_status_applied_at", (q) =>
        q.eq("userId", userId).eq("status", "rejected"),
      )
      .order("desc")
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
      }),
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
    const merged = mergeJobDetails(normalized, details);
    return {
      ...merged,
    };
  },
});

export const getJobIdByUrl = query({
  args: {
    url: v.string(),
  },
  returns: v.union(v.id("jobs"), v.null()),
  handler: async (ctx, args) => {
    const candidate = args.url.trim();
    if (!candidate) return null;
    const stripped = candidate.replace(/\/+$/, "");
    const queryValues =
      stripped && stripped !== candidate ? [candidate, stripped] : [candidate];
    for (const value of queryValues) {
      const match = await ctx.db
        .query("jobs")
        .withIndex("by_url", (q: any) => q.eq("url", value))
        .first();
      if (match) return match._id;
    }
    return null;
  },
});

export const setJobDescriptionStorage = internalMutation({
  args: {
    jobId: v.id("jobs"),
    description: v.string(),
    storageId: v.id("_storage"),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const job = await ctx.db.get(args.jobId);
    if (!job) {
      throw new Error("Job not found");
    }
    const preview = buildDescriptionPreview(args.description);
    const existing = await ctx.db
      .query("job_details")
      .withIndex("by_job", (q: any) => q.eq("jobId", args.jobId))
      .first();
    const patch = {
      description: preview,
      descriptionStorageId: args.storageId,
    };
    await ctx.db.patch(job._id, { description: preview });
    if (existing) {
      await ctx.db.patch(existing._id, patch);
      return null;
    }
    await ctx.db.insert("job_details", {
      jobId: args.jobId,
      ...patch,
    });
    return null;
  },
});

export const getJobDetails = query({
  args: {
    jobId: v.optional(v.id("jobs")),
    groupedJobIds: v.optional(v.array(v.id("jobs"))),
  },
  handler: async (ctx, args) => {
    if (!args.jobId) return null;
    const job = await ctx.db.get(args.jobId);
    const jobIds =
      args.groupedJobIds && args.groupedJobIds.length > 0
        ? Array.from(new Set([args.jobId, ...args.groupedJobIds]))
        : [args.jobId];
    const detailEntries = await Promise.all(
      jobIds.map(async (jobId) => ({
        jobId,
        details: await getJobDetailsByJobId(ctx, jobId),
      })),
    );
    const primaryDetailEntry =
      detailEntries.find(
        (entry) => entry.jobId === args.jobId && entry.details,
      ) ?? detailEntries.find((entry) => entry.details);
    const storageDetailEntry = detailEntries.find(
      (entry) => (entry.details as any)?.descriptionStorageId,
    );
    const descriptionStorageAvailable = Boolean(
      (storageDetailEntry?.details as any)?.descriptionStorageId,
    );
    const descriptionStorageJobId = descriptionStorageAvailable
      ? storageDetailEntry?.jobId
      : undefined;
    const applicationCount = await countAppliedApplications(ctx, jobIds);

    if (!primaryDetailEntry?.details) {
      const fallbackDescription =
        typeof job?.description === "string" ? job.description : undefined;
      return {
        description: fallbackDescription,
        applicationCount,
        descriptionStorageAvailable,
        descriptionStorageJobId,
      };
    }

    const {
      jobId: _jobId,
      _id: _detailId,
      descriptionStorageId: _storageId,
      ...detailFields
    } = primaryDetailEntry.details;
    if (!detailFields.description) {
      if (typeof job?.description === "string") {
        detailFields.description = job.description;
      }
    }
    return {
      ...detailFields,
      applicationCount,
      descriptionStorageAvailable,
      descriptionStorageJobId,
    };
  },
});

export const getJobDescriptionUrl = query({
  args: {
    jobId: v.id("jobs"),
  },
  returns: v.union(v.string(), v.null()),
  handler: async (ctx, args) => {
    const details = await getJobDetailsByJobId(ctx, args.jobId);
    const storageId = (details as any)?.descriptionStorageId;
    if (!storageId) return null;
    const url = await ctx.storage.getUrl(storageId);
    return url ?? null;
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
      .withIndex("by_user_and_job", (q) =>
        q.eq("userId", userId).eq("jobId", args.jobId),
      )
      .unique();

    if (!existingApplication) {
      throw new Error("Application not found");
    }
    if (existingApplication.status !== "applied") {
      throw new Error("No active application to withdraw");
    }

    await ctx.db.delete(existingApplication._id);

    // Update the denormalized index for O(1) filtering in listJobs
    await updateUserApplicationIndex(ctx, userId, "remove", args.jobId);

    return { success: true };
  },
});

export const normalizeDevTestJobs = mutation({
  args: {},
  handler: async (ctx) => {
    const jobs = await ctx.db.query("jobs").collect();
    const detailRows = await ctx.db.query("job_details").collect();
    const detailByJobId = new Map(
      detailRows.map((row: any) => [String(row.jobId), row]),
    );
    const needsFix = jobs.filter((j: any) => {
      const tooShort = (s: any) =>
        typeof s === "string" && s.trim().length <= 2;
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
        (typeof j.totalCompensation === "number" &&
          j.totalCompensation <= 10) ||
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
    const companies = [
      "Acme Corp",
      "SampleSoft",
      "Initech",
      "Globex",
      "Umbrella Labs",
    ];
    const locations = [
      "Remote - US",
      "San Francisco, CA",
      "New York, NY",
      "Austin, TX",
      "Seattle, WA",
    ];

    let updates = 0;
    for (const j of needsFix) {
      const pick = (arr: string[]) =>
        arr[Math.floor(Math.random() * arr.length)];
      const comp = 100000 + Math.floor(Math.random() * 90000);
      const loc = pick(locations);
      const { city, state } = splitLocation(loc);
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
      const descriptionFields = await storeDescriptionInStorage(
        ctx,
        "This is a realistic sample listing used for development. Replace with real scraped data in production.",
        (existingDetails as any)?.descriptionStorageId,
      );
      const detailPatch = { ...descriptionFields };
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
    // Delete job_details
    const details = await ctx.db
      .query("job_details")
      .withIndex("by_job", (q: any) => q.eq("jobId", args.jobId))
      .collect();
    for (const detail of details as any[]) {
      await deleteDescriptionFromStorage(
        ctx,
        (detail as any).descriptionStorageId,
      );
      await ctx.db.delete(detail._id);
    }

    // Delete job_url_keys entries
    const urlKeys = await ctx.db
      .query("job_url_keys")
      .withIndex("by_job", (q: any) => q.eq("jobId", args.jobId))
      .collect();
    for (const urlKey of urlKeys as any[]) {
      await ctx.db.delete(urlKey._id);
    }

    // Delete applications
    const applications = await ctx.db
      .query("applications")
      .withIndex("by_job", (q: any) => q.eq("jobId", args.jobId))
      .collect();
    for (const application of applications as any[]) {
      await ctx.db.delete(application._id);
    }

    // Finally delete the job itself
    await ctx.db.delete(args.jobId);
    return { success: true };
  },
});
