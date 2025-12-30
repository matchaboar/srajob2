export function countJobs(items: any): number {
  if (!items) return 0;

  // Common shapes: array, { items: [...] }, { results: { items: [...] } }, { results: [...] }
  if (Array.isArray(items)) return items.length;
  if (typeof items === "object") {
    if (Array.isArray((items as any).normalized)) return (items as any).normalized.length;
    if (Array.isArray((items as any).items)) return (items as any).items.length;
    if (Array.isArray((items as any).results)) return (items as any).results.length;
    if ((items as any).results && Array.isArray((items as any).results.items)) {
      return (items as any).results.items.length;
    }
  }
  return 0;
}
