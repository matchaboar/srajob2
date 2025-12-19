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

export type CompensationMeta = {
  display: string;
  isUnknown: boolean;
  reason: string;
  currencyCode: string;
};

export const buildCompensationMeta = (job: any): CompensationMeta => {
  const isUnknown = job?.compensationUnknown === true || typeof job?.totalCompensation !== "number";
  const currencyCode = job?.currencyCode || "USD";

  const display = isUnknown
    ? "Unknown"
    : formatCurrencyCompensation(job?.totalCompensation as number | undefined, currencyCode);

  const reason =
    typeof job?.compensationReason === "string" && job.compensationReason.trim()
      ? job.compensationReason.trim()
      : isUnknown
        ? UNKNOWN_COMPENSATION_REASON
        : typeof job?.scrapedWith === "string" && job.scrapedWith.trim()
          ? `${job.scrapedWith} extracted compensation`
          : "Compensation provided in listing";

  return {
    display,
    isUnknown,
    reason,
    currencyCode,
  };
};
