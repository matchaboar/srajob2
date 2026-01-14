/**
 * Slug utilities for company name processing.
 */

const slugCharMap: Record<string, string> = {
  "+": "plus",
  ".": "dot",
  "&": "and",
  đ: "d",
  ħ: "h",
  ı: "i",
  ĸ: "k",
  ŀ: "l",
  ł: "l",
  ß: "ss",
  ŧ: "t",
  ø: "o",
};

/**
 * Convert company name to a slug for icon lookup.
 */
export const toSlug = (company: string): string | null => {
  const lowered = company.toLowerCase();
  const replaced = lowered.replace(
    /[+.&đħıĸŀłßŧø]/g,
    (char) => slugCharMap[char] ?? ""
  );
  const normalized = replaced.normalize("NFD").replace(/[^a-z0-9]/g, "");
  return normalized || null;
};

/**
 * Convert slug to simple-icons export name.
 */
export const slugToExportName = (slug: string): string =>
  `si${slug[0].toUpperCase()}${slug.slice(1)}`;

/**
 * Build fallback initial letter for company.
 */
export const buildFallbackInitial = (company: string): string => {
  const trimmed = company.trim();
  const first = trimmed.match(/[A-Za-z0-9]/)?.[0];
  return first ? first.toUpperCase() : "?";
};
