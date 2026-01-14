/**
 * Centralized domain and URL parsing utilities.
 * Extracted from AdminPage, CompanyIcon, QueuedUrlRow.
 */

// Common subdomain prefixes to skip when extracting company names
export const COMMON_SUBDOMAIN_PREFIXES = new Set([
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

// Reserved path segments that don't represent company names
export const RESERVED_PATH_SEGMENTS = new Set([
  "boards",
  "jobs",
  "careers",
  "jobdetail",
  "job-details",
  "jobdetails",
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

// Known hosted job board domains
export const HOSTED_JOB_DOMAINS = [
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

// Company slugs that are job board platforms, not actual companies
export const HOSTED_COMPANY_SLUGS = new Set([
  "avature",
  "greenhouse",
  "ashby",
  "lever",
  "workable",
  "smartrecruiters",
  "workday",
  "icims",
  "jobvite",
  "bamboohr",
]);

/**
 * Extract base domain from hostname (handles ccTLDs like .co.uk).
 */
export const baseDomainFromHost = (host: string): string => {
  const parts = host.split(".").filter(Boolean);
  if (parts.length <= 1) return host;
  const last = parts[parts.length - 1];
  const secondLast = parts[parts.length - 2];
  const shouldUseThree = last.length === 2 || secondLast.length === 2;
  if (shouldUseThree && parts.length >= 3) {
    return parts.slice(-3).join(".");
  }
  return parts.slice(-2).join(".");
};

/**
 * Check if a host matches a hosted job board domain.
 */
export const resolveHostedJobsDomain = (host: string): string | null =>
  HOSTED_JOB_DOMAINS.find(
    (domain) => host === domain || host.endsWith(`.${domain}`)
  ) ?? null;

/**
 * Extract company slug from URL pathname.
 */
export const extractCompanyFromPath = (pathname: string): string | null => {
  const parts = pathname.split("/").filter(Boolean);
  for (const part of parts) {
    const cleaned = part.toLowerCase();
    if (RESERVED_PATH_SEGMENTS.has(cleaned)) continue;
    if (/^\d+$/.test(cleaned)) continue;
    if (!/^[a-z0-9-]+$/.test(cleaned)) continue;
    return cleaned;
  }
  return null;
};

/**
 * Extract company slug from subdomain of a hosted job domain.
 */
export const extractCompanyFromSubdomain = (
  host: string,
  hostedDomain: string
): string | null => {
  const hostParts = host.split(".").filter(Boolean);
  const baseParts = hostedDomain.split(".").filter(Boolean);
  if (hostParts.length <= baseParts.length) return null;
  const subdomains = hostParts.slice(0, hostParts.length - baseParts.length);
  for (let i = subdomains.length - 1; i >= 0; i -= 1) {
    const candidate = subdomains[i]?.toLowerCase() ?? "";
    if (!candidate || COMMON_SUBDOMAIN_PREFIXES.has(candidate)) continue;
    // Skip Workday instance identifiers like "wd1", "wd2"
    if (/^wd\d+$/i.test(candidate)) continue;
    if (!/^[a-z0-9-]+$/.test(candidate)) continue;
    return candidate;
  }
  return null;
};

/**
 * Extract company label from a URL string.
 */
export const extractCompanyLabel = (urlValue?: string): string | null => {
  if (!urlValue) return null;
  try {
    const parsed = new URL(urlValue);
    const host = parsed.hostname.toLowerCase().replace(/^www\./, "");
    const baseDomain = baseDomainFromHost(host);
    const hostedDomain = resolveHostedJobsDomain(host);

    if (hostedDomain) {
      // Try subdomain extraction first
      const subdomainSlug = extractCompanyFromSubdomain(host, hostedDomain);
      if (subdomainSlug) return subdomainSlug;
      // Try path extraction
      const pathCandidate = extractCompanyFromPath(parsed.pathname);
      if (pathCandidate) return pathCandidate;
      // Fall back to first part of base domain
      return baseDomain.split(".")[0] ?? baseDomain;
    }

    // Non-hosted domain: try subdomain extraction
    const hostParts = host.split(".").filter(Boolean);
    const baseParts = baseDomain.split(".").filter(Boolean);
    if (hostParts.length > baseParts.length) {
      const subdomains = hostParts.slice(0, hostParts.length - baseParts.length);
      for (let i = subdomains.length - 1; i >= 0; i -= 1) {
        const candidate = subdomains[i];
        if (!candidate || COMMON_SUBDOMAIN_PREFIXES.has(candidate)) continue;
        if (/^wd\d+$/i.test(candidate)) continue;
        return candidate;
      }
    }
    return baseDomain.split(".")[0] ?? baseDomain;
  } catch {
    return null;
  }
};

/**
 * Convert slug to display name: "my-company" -> "My Company"
 */
export const toDisplayName = (value: string): string =>
  value
    .replace(/[-_]+/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());

/**
 * Convert slug to title case: "my-company" -> "My Company"
 */
export const toTitleCaseSlug = (slug: string): string => {
  return slug
    .replace(/[_-]+/g, " ")
    .split(/[\s.]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
};

/**
 * Safely parse a URL string, adding protocol if missing.
 */
export const safeParseUrl = (rawUrl: string): URL | null => {
  if (!rawUrl) return null;
  try {
    return new URL(rawUrl.includes("://") ? rawUrl : `https://${rawUrl}`);
  } catch {
    return null;
  }
};

/**
 * Check if URL appears to be a Greenhouse URL.
 */
export const isGreenhouseUrlString = (rawUrl: string): boolean => {
  if (!rawUrl) return false;
  if (/greenhouse/i.test(rawUrl)) return true;
  try {
    const parsed = new URL(rawUrl);
    return /greenhouse/i.test(parsed.hostname);
  } catch {
    return /greenhouse/i.test(rawUrl);
  }
};

/**
 * Extract Greenhouse board slug from URL.
 */
export const greenhouseSlugFromUrl = (rawUrl: string): string | null => {
  const parsed = safeParseUrl(rawUrl);
  if (!parsed) return null;
  const query = new URLSearchParams(parsed.search);
  const boardParam = query.get("board");
  if (boardParam) return boardParam.toLowerCase();
  const parts = parsed.pathname.split("/").filter(Boolean);
  const boardsIdx = parts.findIndex((p) => p.toLowerCase() === "boards");
  if (boardsIdx >= 0 && boardsIdx + 1 < parts.length) {
    return parts[boardsIdx + 1].toLowerCase();
  }
  if (
    parts.length >= 3 &&
    parts[0].toLowerCase() === "v1" &&
    parts[1].toLowerCase() === "boards"
  ) {
    return parts[2].toLowerCase();
  }
  const hostParts = (parsed.hostname || "").toLowerCase().split(".").filter(Boolean);
  if (
    hostParts.length >= 3 &&
    hostParts[hostParts.length - 2] !== "greenhouse"
  ) {
    return hostParts[hostParts.length - 2];
  }
  return null;
};
