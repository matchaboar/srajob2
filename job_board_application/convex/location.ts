const STATE_NAME_BY_ABBR: Record<string, string> = {
  WA: "Washington",
  NY: "New York",
  CA: "California",
  AZ: "Arizona",
  TX: "Texas",
  MA: "Massachusetts",
  IL: "Illinois",
  GA: "Georgia",
  CO: "Colorado",
  FL: "Florida",
  OR: "Oregon",
  NJ: "New Jersey",
  DC: "District of Columbia",
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
  texas: "Texas",
  austin: "Texas",
  houston: "Texas",
  dallas: "Texas",
  massachusetts: "Massachusetts",
  boston: "Massachusetts",
  illinois: "Illinois",
  chicago: "Illinois",
  georgia: "Georgia",
  atlanta: "Georgia",
  colorado: "Colorado",
  denver: "Colorado",
  florida: "Florida",
  miami: "Florida",
  oregon: "Oregon",
  portland: "Oregon",
  "new jersey": "New Jersey",
  "district of columbia": "District of Columbia",
  "washington dc": "District of Columbia",
  "washington, dc": "District of Columbia",
  "los angeles": "California",
  "san diego": "California",
  "san jose": "California",
  "palo alto": "California",
  "mountain view": "California",
  "redwood city": "California",
  sunnyvale: "California",
  cupertino: "California",
};

export type CityState = { city: string; state: string };

export const TARGET_STATES = [
  "Washington",
  "New York",
  "California",
  "Arizona",
  "Texas",
  "Massachusetts",
  "Illinois",
  "Georgia",
  "Colorado",
  "Florida",
  "Oregon",
  "New Jersey",
  "District of Columbia",
] as const;

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

const COMMON_TECH_CITIES: Record<string, string> = {
  "san francisco": "California",
  "new york": "New York",
  seattle: "Washington",
  austin: "Texas",
  boston: "Massachusetts",
  chicago: "Illinois",
  atlanta: "Georgia",
  denver: "Colorado",
  "los angeles": "California",
  "san diego": "California",
  "palo alto": "California",
  "mountain view": "California",
  "menlo park": "California",
  "redwood city": "California",
  sunnyvale: "California",
  cupertino: "California",
  "san jose": "California",
  portland: "Oregon",
  miami: "Florida",
  houston: "Texas",
  dallas: "Texas",
  "washington dc": "District of Columbia",
};

const COMMON_CITY_KEYS = Object.keys(COMMON_TECH_CITIES);

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

export const TARGET_STATE_OPTIONS = TARGET_STATES;

export const findCityInText = (text: string): CityState | null => {
  const lowerText = (text || "").toLowerCase();
  let best: { city: string; state: string; index: number } | null = null;

  for (const city of COMMON_CITY_KEYS) {
    const idx = lowerText.indexOf(city);
    if (idx === -1) continue;
    if (best === null || idx < best.index) {
      best = { city, state: COMMON_TECH_CITIES[city], index: idx };
    }
  }

  if (!best) return null;
  const cityName = best.city
    .split(" ")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
  return { city: cityName, state: best.state };
};

export const isUnknownLocationValue = (value?: string | null) => {
  const normalized = (value || "").trim().toLowerCase();
  return (
    !normalized ||
    normalized === "unknown" ||
    normalized === "n/a" ||
    normalized === "na" ||
    normalized === "unspecified" ||
    normalized === "not available"
  );
};
