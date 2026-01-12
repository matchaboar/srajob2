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
  if (normalized) return normalized;
  if (typeof rawUrl !== "string") return null;
  const trimmed = rawUrl.trim();
  if (!trimmed) return null;
  const withoutSlash = trimmed.replace(/\/+$/, "");
  return withoutSlash || trimmed;
};

export { isAshbyHost, isAvatureHost, isHubspotHost, parseUrlSafe };
