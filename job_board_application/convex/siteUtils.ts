const COMMON_SUBDOMAIN_PREFIXES = ["www", "jobs", "careers", "boards", "app", "apply"];

const cleanPathname = (pathname: string) => {
  if (!pathname) return "/";
  const cleaned = pathname.replace(/\/+$/, "");
  return cleaned || "/";
};

export const greenhouseSlugFromUrl = (rawUrl: string): string | null => {
  try {
    const parsed = new URL(rawUrl.includes("://") ? rawUrl : `https://${rawUrl}`);
    const host = parsed.hostname.toLowerCase();
    if (!/greenhouse/.test(host)) return null;
    const parts = parsed.pathname.split("/").filter(Boolean);
    const boardsIdx = parts.findIndex((p) => p.toLowerCase() === "boards");
    const slug =
      boardsIdx >= 0 && boardsIdx + 1 < parts.length
        ? parts[boardsIdx + 1]
        : parts.length > 0
          ? parts[0]
          : null;
    if (!slug || /^v\d+$/i.test(slug)) return null;
    return slug.toLowerCase();
  } catch {
    return null;
  }
};

export const normalizeSiteUrl = (rawUrl: string, type?: string): string => {
  const trimmed = (rawUrl || "").trim();
  if (!trimmed) return "";

  const maybeSlug = greenhouseSlugFromUrl(trimmed);
  if (type === "greenhouse" || maybeSlug) {
    if (maybeSlug) {
      return `https://api.greenhouse.io/v1/boards/${maybeSlug}/jobs`;
    }
  }

  try {
    const parsed = new URL(trimmed.includes("://") ? trimmed : `https://${trimmed}`);
    parsed.hash = "";
    const pathname = cleanPathname(parsed.pathname);
    const search = parsed.search || "";
    return `${parsed.protocol}//${parsed.host.toLowerCase()}${pathname}${search}`;
  } catch {
    return trimmed.toLowerCase();
  }
};

export const siteCanonicalKey = (rawUrl: string, type?: string): string => {
  const normalized = normalizeSiteUrl(rawUrl, type);
  const keyType = (type || "general").toLowerCase();
  return `${keyType}:${normalized}`;
};

export const fallbackCompanyNameFromUrl = (url: string): string => {
  if (!url) return "Site";
  const slug = greenhouseSlugFromUrl(url);
  if (slug) return slug;
  try {
    const parsed = new URL(url.includes("://") ? url : `https://${url}`);
    const hostParts = parsed.hostname
      .toLowerCase()
      .split(".")
      .filter(Boolean)
      .filter((part) => !COMMON_SUBDOMAIN_PREFIXES.includes(part));
    if (hostParts.length >= 2) return hostParts[hostParts.length - 2];
    if (hostParts.length === 1) return hostParts[0];
  } catch {
    // ignore parse errors; fall through
  }
  return "Site";
};
