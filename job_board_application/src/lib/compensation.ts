import { parse } from "yaml";
import salaryCapsYaml from "../config/company_salary_caps.yml?raw";

export const formatCompensationDisplay = (value: number | null) => {
  if (value === null) return "";
  return `$${Math.round(value / 1000)}k`;
};

export const formatCurrencyCompensation = (value?: number, currencyCode: string = "USD") => {
  if (typeof value !== "number" || Number.isNaN(value)) return "Unknown";
  try {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: currencyCode || "USD",
      maximumFractionDigits: 0,
    }).format(value);
  } catch {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      maximumFractionDigits: 0,
    }).format(value);
  }
};

export const parseCompensationInput = (value: string, opts?: { max?: number }) => {
  const normalized = value.trim().toLowerCase();
  if (!normalized) return null;

  const cleaned = normalized.replace(/[$,]/g, "");
  const hasK = cleaned.endsWith("k");
  const numericPart = hasK ? cleaned.slice(0, -1) : cleaned;
  const parsed = parseFloat(numericPart);

  if (Number.isNaN(parsed)) return null;
  if (parsed <= 0) return null;

  let dollars = Math.round(parsed * (hasK ? 1000 : 1));
  if (opts?.max !== undefined) {
    dollars = Math.min(dollars, opts.max);
  }
  return dollars;
};

export const UNKNOWN_COMPENSATION_REASON = "pending markdown structured extraction";

type SalaryLevel = "junior" | "mid" | "senior" | "staff";

type SalaryCaps = Record<SalaryLevel, number>;

type SalaryCapsConfig = {
  company_salary_caps?: Record<string, Partial<SalaryCaps>>;
};

const SALARY_LEVELS: SalaryLevel[] = ["junior", "mid", "senior", "staff"];

const normalizeCompanyKey = (value: string) => value.toLowerCase().replace(/[^a-z0-9]/g, "");

const COMPANY_SALARY_CAPS = (() => {
  try {
    const parsed = parse(salaryCapsYaml) as SalaryCapsConfig;
    const rawCaps = parsed?.company_salary_caps ?? {};
    const entries = new Map<string, SalaryCaps>();
    for (const [company, levels] of Object.entries(rawCaps)) {
      if (!company || typeof levels !== "object" || levels === null) continue;
      const key = normalizeCompanyKey(company);
      if (!key) continue;
      const normalized: Partial<SalaryCaps> = {};
      for (const level of SALARY_LEVELS) {
        const value = (levels as Partial<SalaryCaps>)[level];
        if (typeof value === "number" && Number.isFinite(value) && value > 0) {
          normalized[level] = value;
        }
      }
      if (normalized.mid) {
        entries.set(key, normalized as SalaryCaps);
      }
    }
    return entries;
  } catch {
    return new Map<string, SalaryCaps>();
  }
})();

const resolveConfigCompensation = (job: any) => {
  const company = typeof job?.company === "string" ? job.company.trim() : "";
  if (!company) return null;
  const caps = COMPANY_SALARY_CAPS.get(normalizeCompanyKey(company));
  if (!caps) return null;
  const rawLevel = typeof job?.level === "string" ? job.level.trim().toLowerCase() : "";
  const level = SALARY_LEVELS.includes(rawLevel as SalaryLevel) ? (rawLevel as SalaryLevel) : "mid";
  const value = caps[level] ?? caps.mid;
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  return { value, level, usedMidFallback: level !== rawLevel };
};

export type CompensationMeta = {
  display: string;
  isUnknown: boolean;
  isEstimated: boolean;
  reason: string;
  currencyCode: string;
};

export const buildCompensationMeta = (job: any): CompensationMeta => {
  const isUnknown = job?.compensationUnknown === true || typeof job?.totalCompensation !== "number";
  const currencyCode = job?.currencyCode || "USD";

  const fallback = isUnknown ? resolveConfigCompensation(job) : null;
  const hasFallback = Boolean(fallback);
  const display = isUnknown
    ? hasFallback
      ? formatCurrencyCompensation(fallback?.value as number | undefined, currencyCode)
      : "Unknown"
    : formatCurrencyCompensation(job?.totalCompensation as number | undefined, currencyCode);

  const fallbackReason = hasFallback
    ? `Company salary cap from config${fallback?.usedMidFallback ? " (mid-level fallback)" : ""}`
    : null;
  const reason =
    fallbackReason ??
    (typeof job?.compensationReason === "string" && job.compensationReason.trim()
      ? job.compensationReason.trim()
      : isUnknown
        ? UNKNOWN_COMPENSATION_REASON
        : typeof job?.scrapedWith === "string" && job.scrapedWith.trim()
          ? `${job.scrapedWith} extracted compensation`
          : "Compensation provided in listing");

  return {
    display,
    isUnknown: isUnknown && !hasFallback,
    isEstimated: hasFallback,
    reason,
    currencyCode,
  };
};
