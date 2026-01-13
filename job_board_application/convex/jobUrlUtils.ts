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
const isHubspotHost = (host: string) => host.endsWith("hubspot.com");
const isConvexShareHost = (host: string) =>
  host.endsWith(".convex.site") || host.endsWith(".convex.cloud");

/**
 * Canonicalize Greenhouse URLs to a single format.
 * Greenhouse has multiple URL formats that all point to the same job:
 * - Web format: https://boards.greenhouse.io/{slug}/jobs/{job_id}
 * - API format: https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{job_id}
 * - Job-boards format: https://job-boards.greenhouse.io/{slug}/jobs/{job_id}
 *
 * This function normalizes all formats to the canonical web format.
 * Returns the original URL if it's not a recognized Greenhouse URL.
 */
export const canonicalizeGreenhouseUrl = (url: string): string => {
  // Pattern for web format: boards.greenhouse.io/{slug}/jobs/{job_id}
  const webMatch = url.match(
    /^https?:\/\/boards\.greenhouse\.io\/([^/]+)\/jobs\/(\d+)\/?$/i
  );
  if (webMatch) {
    const [, slug, jobId] = webMatch;
    return `https://boards.greenhouse.io/${slug}/jobs/${jobId}`;
  }

  // Pattern for API format: boards-api.greenhouse.io/v1/boards/{slug}/jobs/{job_id}
  const apiMatch = url.match(
    /^https?:\/\/boards-api\.greenhouse\.io\/v1\/boards\/([^/]+)\/jobs\/(\d+)\/?$/i
  );
  if (apiMatch) {
    const [, slug, jobId] = apiMatch;
    return `https://boards.greenhouse.io/${slug}/jobs/${jobId}`;
  }

  // Pattern for job-boards format: job-boards.greenhouse.io/{slug}/jobs/{job_id}
  const jobBoardsMatch = url.match(
    /^https?:\/\/job-boards\.greenhouse\.io\/([^/]+)\/jobs\/(\d+)\/?$/i
  );
  if (jobBoardsMatch) {
    const [, slug, jobId] = jobBoardsMatch;
    return `https://boards.greenhouse.io/${slug}/jobs/${jobId}`;
  }

  // Not a recognized Greenhouse URL, return as-is
  return url;
};

export function normalizeScrapedUrl(rawUrl: string, sourceUrl?: string): string | null {
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

  if (isAvatureHost(host) && /(\/savejob|\/searchjobs|\/jobsearch)/i.test(path)) {
    return null;
  }
  if (isHubspotHost(host) && path.toLowerCase().startsWith("/careers/jobs/")) {
    if (parsed.searchParams.has("hubs_signup-cta")) return null;
  }
  if (isConvexShareHost(host) && path.toLowerCase().startsWith("/share/job")) {
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
}

export const normalizeJobUrlKey = (rawUrl: string, sourceUrl?: string): string | null => {
  const normalized = normalizeScrapedUrl(rawUrl, sourceUrl);
  if (normalized) {
    // Apply provider-specific canonicalization for consistent URL keys
    return canonicalizeGreenhouseUrl(normalized);
  }
  if (typeof rawUrl !== "string") return null;
  const trimmed = rawUrl.trim();
  if (!trimmed) return null;
  const withoutSlash = trimmed.replace(/\/+$/, "");
  // Apply canonicalization to fallback path too
  return canonicalizeGreenhouseUrl(withoutSlash || trimmed);
};

export { isAshbyHost, isAvatureHost, isHubspotHost, parseUrlSafe };
