export const formatCompensationDisplay = (value: number | null) => {
  if (value === null) return "";
  return `$${Math.round(value / 1000)}k`;
};

export const parseCompensationInput = (value: string, opts?: { max?: number }) => {
  const normalized = value.trim().toLowerCase();
  if (!normalized) return null;

  const cleaned = normalized.replace(/[\$,]/g, "");
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
