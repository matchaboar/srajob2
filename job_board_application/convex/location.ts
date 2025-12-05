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

  const normalizedState = normalizeState(maybeState);
  const stateName =
    normalizedState ??
    (maybeState || normalizeState(raw.split(" ").pop() ?? "") || "Unknown");

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

const splitByMultiSeparator = (value: string): string[] => {
  return value
    .split(/[;|/]/)
    .map((v) => v.trim())
    .filter(Boolean);
};

const COUNTRY_ALIASES: Record<string, string> = {
  usa: "United States",
  "u.s.a": "United States",
  "united states": "United States",
  "united states of america": "United States",
  us: "United States",
  uk: "United Kingdom",
  "united kingdom": "United Kingdom",
  britain: "United Kingdom",
  england: "United Kingdom",
  scotland: "United Kingdom",
  wales: "United Kingdom",
  eu: "Europe",
  europe: "Europe",
  espana: "Spain",
  spain: "Spain",
  france: "France",
  india: "India",
  canada: "Canada",
  mexico: "Mexico",
  germany: "Germany",
  netherlands: "Netherlands",
  ireland: "Ireland",
  australia: "Australia",
  singapore: "Singapore",
};

export const normalizeLocations = (value?: string | string[] | null): string[] => {
  const seeds: string[] = [];
  if (Array.isArray(value)) {
    seeds.push(...value);
  } else if (typeof value === "string" && value.trim()) {
    seeds.push(value);
  }

  const seen = new Set<string>();
  const normalized: string[] = [];
  for (const seed of seeds) {
    for (const part of splitByMultiSeparator(seed)) {
      const cleaned = part.replace(/\s+/g, " ").trim().replace(/^[,;]+|[,;]+$/g, "");
      const lowered = cleaned.toLowerCase();
      if (!cleaned || cleaned.length < 3 || cleaned.length > 100) continue;
      if (["unknown", "n/a", "na"].includes(lowered)) continue;
      if (seen.has(cleaned)) continue;
      seen.add(cleaned);
      normalized.push(cleaned);
    }
  }

  return normalized;
};

export const deriveLocationStates = (locations: string[]): string[] => {
  const states = new Set<string>();
  for (const loc of locations) {
    const { state } = splitLocation(loc);
    if (state && !isUnknownLocationValue(state)) {
      states.add(state);
    }
  }
  return Array.from(states);
};

export const buildLocationSearch = (locations: string[]): string => {
  const tokens = new Set<string>();
  for (const loc of locations) {
    const { city, state } = splitLocation(loc);
    const inferredCountry = inferCountryFromLocation(loc);
    const parts = [loc, city, state, inferredCountry ?? ""];
    for (const part of parts) {
      const cleaned = (part || "").trim();
      if (!cleaned || isUnknownLocationValue(cleaned)) continue;
      cleaned
        .split(/[\s,]+/)
        .map((t) => t.trim())
        .filter(Boolean)
        .forEach((t) => tokens.add(t));
    }
  }
  return Array.from(tokens).join(" ");
};

export const inferCountryFromLocation = (location: string | null | undefined): string | null => {
  const raw = (location || "").trim();
  if (!raw) return null;
  const lower = raw.toLowerCase();
  if (lower.includes("remote")) return "Remote";

  const parts = raw.split(",").map((p) => p.trim()).filter(Boolean);
  const tail = parts.length > 1 ? parts[parts.length - 1] : raw;
  const tailLower = tail.toLowerCase();

  if (COUNTRY_ALIASES[tailLower]) return COUNTRY_ALIASES[tailLower];
  if (COUNTRY_ALIASES[lower]) return COUNTRY_ALIASES[lower];
  if (normalizeState(tail)) return "United States";
  return null;
};

export const deriveCountries = (locations: string[]): string[] => {
  const countries = new Set<string>();
  for (const loc of locations) {
    const inferred = inferCountryFromLocation(loc);
    if (inferred) {
      countries.add(inferred);
    }
  }
  return Array.from(countries);
};

export const deriveLocationFields = (input: { locations?: string[] | null; location?: string | null }) => {
  const normalizedLocations = normalizeLocations(input.locations ?? input.location);
  const primaryLocationRaw = normalizedLocations[0] ?? input.location ?? "Unknown";
  const { city, state } = splitLocation(primaryLocationRaw);
  const locationLabel = formatLocationLabel(city, state, primaryLocationRaw);
  const locations = normalizedLocations.length ? normalizedLocations : [locationLabel];
  const locationStates = deriveLocationStates(locations);
  const countries = deriveCountries(locations);
  const country =
    countries.find((c) => c === "United States") ??
    countries[0] ??
    (locationStates.length > 0 && locationStates[0] !== "Unknown" ? "United States" : "Other");
  const locationSearch = buildLocationSearch(locations);

  return {
    locations,
    countries,
    country,
    locationStates,
    locationSearch,
    primaryLocation: locationLabel,
    city,
    state,
  };
};
