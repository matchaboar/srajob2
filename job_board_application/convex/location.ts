const STATE_NAME_BY_ABBR: Record<string, string> = {
  WA: "Washington",
  NY: "New York",
  CA: "California",
  AZ: "Arizona",
};

const STATE_ALIASES: Record<string, string> = {
  washington: "Washington",
  "washington state": "Washington",
  seattle: "Washington",
  "new york": "New York",
  nyc: "New York",
  california: "California",
  cali: "California",
  bay: "California",
  sf: "California",
  "san francisco": "California",
  arizona: "Arizona",
  phoenix: "Arizona",
};

export type CityState = { city: string; state: string };

export const TARGET_STATES = ["Washington", "New York", "California", "Arizona"] as const;

export const normalizeState = (input: string): string | null => {
  const trimmed = (input || "").trim();
  if (!trimmed) return null;

  const upper = trimmed.toUpperCase();
  if (STATE_NAME_BY_ABBR[upper]) return STATE_NAME_BY_ABBR[upper];

  const lower = trimmed.toLowerCase();
  if (STATE_ALIASES[lower]) return STATE_ALIASES[lower];

  const match = TARGET_STATES.find((s) => s.toLowerCase() === lower);
  return match ?? null;
};

export const splitLocation = (location: string): CityState => {
  const raw = (location || "").trim();
  if (!raw) return { city: "Unknown", state: "Unknown" };

  const lower = raw.toLowerCase();
  if (lower.includes("remote")) {
    return { city: "Remote", state: "Remote" };
  }

  const parts = raw.split(",").map((p) => p.trim()).filter(Boolean);
  const city = parts[0] ?? raw;
  const maybeState = parts[1] ?? "";

  const stateName = normalizeState(maybeState) ?? normalizeState(raw.split(" ").pop() ?? "") ?? "Unknown";

  return {
    city: city || "Unknown",
    state: stateName,
  };
};

export const formatLocationLabel = (city?: string | null, state?: string | null, fallback?: string | null) => {
  const cleanCity = (city || "").trim();
  const cleanState = (state || "").trim();

  if (cleanCity.toLowerCase() === "remote" || cleanState.toLowerCase() === "remote") {
    return "Remote";
  }

  if (cleanCity && cleanState) return `${cleanCity}, ${cleanState}`;
  if (cleanCity) return cleanCity;
  if (cleanState) return cleanState;
  return (fallback || "Unknown").trim() || "Unknown";
};

export const TARGET_STATE_OPTIONS = ["Washington", "New York", "California", "Arizona"] as const;
