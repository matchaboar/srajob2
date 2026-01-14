/**
 * Brandfetch utilities for company logo fetching.
 */
import { toSlug } from "./slugUtils";
import {
  baseDomainFromHost,
  resolveHostedJobsDomain,
  COMMON_SUBDOMAIN_PREFIXES,
  RESERVED_PATH_SEGMENTS,
} from "../domainUtils";

export const BRAND_FETCH_CLIENT = "1idXaGHc5cKcElppzC7";

export const BRANDFETCH_LOGO_OVERRIDES: Record<string, string> = {
  mithril:
    "https://cdn.brandfetch.io/idZPhPbkaC/w/432/h/432/theme/dark/logo.png?c=1bxid64Mup7aczewSAYMX&t=1759798646882",
  together:
    "https://cdn.brandfetch.io/idgEzjThpb/w/400/h/400/theme/dark/icon.jpeg?c=1bxid64Mup7aczewSAYMX&t=1764613007905",
  togetherai:
    "https://cdn.brandfetch.io/idgEzjThpb/w/400/h/400/theme/dark/icon.jpeg?c=1bxid64Mup7aczewSAYMX&t=1764613007905",
  togetherdotai:
    "https://cdn.brandfetch.io/idgEzjThpb/w/400/h/400/theme/dark/icon.jpeg?c=1bxid64Mup7aczewSAYMX&t=1764613007905",
};

export const BRANDFETCH_DOMAIN_OVERRIDES: Record<string, string> = {
  oscar: "hioscar.com",
  serval: "serval.com",
};

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

const UUID_LABEL_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const LONG_HEX_LABEL_REGEX = /^[0-9a-f]{24,}$/i;

const isOpaqueDomainLabel = (label: string): boolean =>
  UUID_LABEL_REGEX.test(label) || LONG_HEX_LABEL_REGEX.test(label);

const DOMAIN_SEGMENT_REGEX = /^[a-z0-9.-]+$/;

const isDomainSegment = (segment: string): boolean => {
  if (!segment.includes(".")) return false;
  if (!DOMAIN_SEGMENT_REGEX.test(segment)) return false;
  if (segment.startsWith(".") || segment.endsWith(".")) return false;
  if (segment.includes("..")) return false;
  return true;
};

const formatSlugAsDomain = (slug: string): string => {
  const normalized = slug.toLowerCase();
  return normalized.includes(".") ? normalized : `${normalized}.com`;
};

const extractCompanySlug = (pathname: string): string | null => {
  const parts = pathname.split("/").filter(Boolean);
  for (const part of parts) {
    const cleaned = part.toLowerCase();
    if (
      cleaned === "jobdetail" ||
      cleaned === "job-details" ||
      cleaned === "jobdetails"
    ) {
      break;
    }
    if (RESERVED_PATH_SEGMENTS.has(cleaned)) continue;
    if (/^\d+$/.test(cleaned)) continue;
    if (isOpaqueDomainLabel(cleaned)) continue;
    if (isDomainSegment(cleaned)) return cleaned;
    if (!/^[a-z0-9-]+$/.test(cleaned)) continue;
    return cleaned;
  }
  return null;
};

const extractCompanySlugFromHost = (
  host: string,
  hostedDomain: string
): string | null => {
  const hostParts = host.split(".").filter(Boolean);
  const domainParts = hostedDomain.split(".").filter(Boolean);
  if (hostParts.length <= domainParts.length) return null;
  const subdomains = hostParts.slice(0, hostParts.length - domainParts.length);
  for (let i = subdomains.length - 1; i >= 0; i -= 1) {
    const candidate = subdomains[i]?.toLowerCase() ?? "";
    if (!candidate) continue;
    if (COMMON_SUBDOMAIN_PREFIXES.has(candidate)) continue;
    if (/^wd\d+$/i.test(candidate)) continue;
    if (!/^[a-z0-9-]+$/.test(candidate)) continue;
    return candidate;
  }
  return null;
};

export const domainMatchesSlug = (domain: string, slug: string): boolean => {
  const normalizedDomain = domain.toLowerCase();
  const normalizedSlug = slug.toLowerCase();
  return (
    normalizedDomain === normalizedSlug ||
    normalizedDomain.startsWith(`${normalizedSlug}.`)
  );
};

/**
 * Derive the Brandfetch domain for a company.
 */
export const deriveBrandfetchDomain = (
  company: string,
  url?: string
): string | null => {
  const trimmedCompany = (company || "").trim();
  const companySlug = toSlug(trimmedCompany);
  const domainOverride = companySlug
    ? BRANDFETCH_DOMAIN_OVERRIDES[companySlug] ?? null
    : null;
  if (domainOverride) {
    return domainOverride;
  }
  const fallbackCompanyDomain = (): string | null => {
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
        const slug = extractCompanySlug(parsed.pathname);
        if (slug) {
          return formatSlugAsDomain(slug);
        }
        const hostSlug = extractCompanySlugFromHost(host, hostedDomain);
        if (hostSlug) {
          return formatSlugAsDomain(hostSlug);
        }
        const companyFallback = fallbackCompanyDomain();
        if (companyFallback) {
          return companyFallback;
        }
      }
      const baseDomain = baseDomainFromHost(host);
      const baseLabel = baseDomain.split(".")[0] ?? "";
      const companyFallback = fallbackCompanyDomain();
      if (companyFallback && isOpaqueDomainLabel(baseLabel)) {
        return companyFallback;
      }
      return baseDomain;
    } catch {
      // fall through to company fallback
    }
  }
  return fallbackCompanyDomain();
};
