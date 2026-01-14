/**
 * Color utilities for company icons.
 */

/**
 * Normalize hex color to 6-digit format.
 */
export const normalizeHex = (hex: string): string | null => {
  let cleaned = hex.replace("#", "").toUpperCase();
  if (cleaned.length === 3) {
    cleaned = cleaned
      .split("")
      .map((c) => c + c)
      .join("");
  }
  if (cleaned.length !== 6) {
    return null;
  }
  return `#${cleaned}`;
};

/**
 * Ensure color has sufficient contrast for dark backgrounds.
 */
export const ensureReadableColor = (hex: string): string => {
  const normalized = normalizeHex(hex) ?? "#E2E8F0";
  const r = parseInt(normalized.slice(1, 3), 16);
  const g = parseInt(normalized.slice(3, 5), 16);
  const b = parseInt(normalized.slice(5, 7), 16);
  if (Number.isNaN(r) || Number.isNaN(g) || Number.isNaN(b)) {
    return "#E2E8F0";
  }
  const luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255;
  if (luminance < 0.38) {
    const mix = (value: number) => Math.round(value + (255 - value) * 0.45);
    const toHex = (value: number) => value.toString(16).padStart(2, "0");
    return `#${toHex(mix(r))}${toHex(mix(g))}${toHex(mix(b))}`;
  }
  return normalized;
};
