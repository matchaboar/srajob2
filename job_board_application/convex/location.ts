import locationDictionary from "./locationDictionary.json" assert { type: "json" };

type LocationDictionaryEntry = {
  city: string;
  state: string;
  country: string;
  aliases?: string[];
  remoteOnly?: boolean;
};

export type CityState = { city: string; state: string; country?: string; remoteOnly?: boolean };

const STATE_NAME_BY_ABBR: Record<string, string> = {
  AL: "Alabama",
  AK: "Alaska",
  AZ: "Arizona",
  AR: "Arkansas",
  CA: "California",
  CO: "Colorado",
  CT: "Connecticut",
  DC: "District of Columbia",
  DE: "Delaware",
  FL: "Florida",
  GA: "Georgia",
  HI: "Hawaii",
  IA: "Iowa",
  ID: "Idaho",
  IL: "Illinois",
  IN: "Indiana",
  KS: "Kansas",
  KY: "Kentucky",
  LA: "Louisiana",
  MA: "Massachusetts",
  MD: "Maryland",
  ME: "Maine",
  MI: "Michigan",
  MN: "Minnesota",
  MO: "Missouri",
  MS: "Mississippi",
  MT: "Montana",
  NC: "North Carolina",
  ND: "North Dakota",
  NE: "Nebraska",
  NH: "New Hampshire",
  NJ: "New Jersey",
  NM: "New Mexico",
  NV: "Nevada",
  NY: "New York",
  OH: "Ohio",
  OK: "Oklahoma",
  OR: "Oregon",
  PA: "Pennsylvania",
  RI: "Rhode Island",
  SC: "South Carolina",
  SD: "South Dakota",
  TN: "Tennessee",
  TX: "Texas",
  UT: "Utah",
  VA: "Virginia",
  VT: "Vermont",
  WA: "Washington",
  WI: "Wisconsin",
  WV: "West Virginia",
  WY: "Wyoming",
};

const normalizeLocationKey = (value: string): string =>
  value
    .toLowerCase()
    .replace(/\(.*?\)/g, " ")
    .replace(/[^a-z0-9 ]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const titleCase = (value: string): string =>
  value
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");

const escapeRegExp = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const containsWholePhrase = (haystack: string, needle: string) => {
  const pattern = new RegExp(`(^|\\s)${escapeRegExp(needle)}(\\s|$)`);
  return pattern.test(haystack);
};

const normalizedEntries: LocationDictionaryEntry[] = (locationDictionary as LocationDictionaryEntry[]).map((entry) => ({
  city: entry.city.trim(),
  state: entry.state.trim(),
  country: entry.country.trim(),
  aliases: (entry.aliases ?? []).map((alias) => alias.trim()).filter(Boolean),
  remoteOnly: Boolean(entry.remoteOnly),
}));

const STATE_ABBR_BY_NAME: Record<string, string> = Object.fromEntries(
  Object.entries(STATE_NAME_BY_ABBR).map(([abbr, name]) => [name, abbr])
);

export const TARGET_STATES = Array.from(
  new Set(
    normalizedEntries
      .filter((entry) => entry.country === "United States" && entry.state && entry.state !== "Remote")
      .map((entry) => entry.state)
  )
).sort();

export const normalizeState = (input: string): string | null => {
  const normalizedKey = normalizeLocationKey(input);
  if (!normalizedKey) return null;

  const upper = normalizedKey.toUpperCase().replace(/ /g, "");
  if (STATE_NAME_BY_ABBR[upper]) return STATE_NAME_BY_ABBR[upper];

  const title = titleCase(normalizedKey);
  if (STATE_ABBR_BY_NAME[title]) return title;

  const match = TARGET_STATES.find((s) => normalizeLocationKey(s) === normalizedKey);
  return match ?? null;
};

const LOCATION_DICTIONARY = new Map<string, CityState>();
const CITY_KEYWORDS: Map<string, CityState> = new Map();

const reorderByUsPreference = (locations: string[]): string[] => {
  const prioritized = [...locations];
  const findUsIndex = (allowRemote: boolean) =>
    prioritized.findIndex((loc) => {
      const resolved = resolveLocationFromDictionary(loc);
      const country = resolved?.country ?? inferCountryFromLocation(loc);
      const lowerLoc = loc.toLowerCase();
      const isRemote =
        resolved?.remoteOnly ||
        (resolved?.city || "").toLowerCase() === "remote" ||
        (resolved?.state || "").toLowerCase() === "remote" ||
        lowerLoc.includes("remote");
      if (!allowRemote && isRemote) return false;
      return country === "United States";
    });

  const nonRemoteUsIdx = findUsIndex(false);
  if (nonRemoteUsIdx > 0) {
    const [hit] = prioritized.splice(nonRemoteUsIdx, 1);
    prioritized.unshift(hit);
    return prioritized;
  }

  const remoteUsIdx = findUsIndex(true);
  if (remoteUsIdx > 0) {
    const [hit] = prioritized.splice(remoteUsIdx, 1);
    prioritized.unshift(hit);
  }

  return prioritized;
};

const registerLocationKey = (value: string, entry: CityState, trackCityKeyword = false, allowOverride = false) => {
  const key = normalizeLocationKey(value);
  if (!key) return;
  if (!allowOverride && LOCATION_DICTIONARY.has(key)) return;
  LOCATION_DICTIONARY.set(key, entry);
  if (trackCityKeyword && !entry.remoteOnly) {
    CITY_KEYWORDS.set(key, entry);
  }
};

const registerEntry = (entry: LocationDictionaryEntry) => {
  const state = entry.state || "Unknown";
  const country = entry.country || undefined;
  const stateAbbr = STATE_ABBR_BY_NAME[state] ?? null;
  const record: CityState = {
    city: entry.city,
    state,
    country,
    remoteOnly: Boolean(entry.remoteOnly),
  };

  const aliasSeeds = new Set<string>([entry.city, ...(entry.aliases ?? [])]);
  for (const alias of aliasSeeds) {
    registerLocationKey(alias, record, true);
    registerLocationKey(`${alias}, ${state}`, record);
    registerLocationKey(`${alias}, ${country ?? state}`, record);
    if (stateAbbr) {
      registerLocationKey(`${alias}, ${stateAbbr}`, record);
    }
  }
};

for (const entry of normalizedEntries) {
  registerEntry(entry);
}

const LOCATION_DICTIONARY_KEYS: Array<[string, CityState]> = Array.from(LOCATION_DICTIONARY.entries()).sort(
  (a, b) => b[0].length - a[0].length
);
const CITY_KEYWORD_KEYS = Array.from(CITY_KEYWORDS.keys()).sort((a, b) => b.length - a.length);

export const resolveLocationFromDictionary = (location: string, options?: { allowRemote?: boolean }): CityState | null => {
  const allowRemote = options?.allowRemote ?? true;
  const normalized = normalizeLocationKey(location);
  if (!normalized) return null;

  const direct = LOCATION_DICTIONARY.get(normalized);
  if (direct && (allowRemote || !direct.remoteOnly)) return direct;

  for (const [key, entry] of LOCATION_DICTIONARY_KEYS) {
    if (!allowRemote && entry.remoteOnly) continue;
    if (key.length < 3) continue;
    if (entry.remoteOnly) {
      if (normalized === key) return entry;
      continue;
    }
    if (containsWholePhrase(normalized, key)) {
      return entry;
    }
  }

  return null;
};

export const splitLocation = (location: string): CityState => {
  const raw = (location || "").trim();
  if (!raw) return { city: "Unknown", state: "Unknown" };

  const resolved = resolveLocationFromDictionary(raw);
  if (resolved) return resolved;

  return { city: "Unknown", state: "Unknown" };
};

export const formatLocationLabel = (
  city?: string | null,
  state?: string | null,
  _fallback?: string | null,
  country?: string | null
) => {
  const cleanCity = (city || "").trim();
  const cleanState = (state || "").trim();
  const cleanCountry = (country || "").trim();

  if (cleanCity.toLowerCase() === "remote" || cleanState.toLowerCase() === "remote") {
    return "Remote";
  }

  if (cleanCity && cleanState && cleanCity !== "Unknown" && cleanState !== "Unknown") return `${cleanCity}, ${cleanState}`;
  if (cleanCity && cleanCountry && cleanCountry !== "Unknown") return `${cleanCity}, ${cleanCountry}`;
  if (cleanCity && cleanCity !== "Unknown") return cleanCity;
  if (cleanState && cleanState !== "Unknown") return cleanState;
  if (cleanCountry && cleanCountry !== "Unknown") return cleanCountry;
  return "Unknown";
};

export const TARGET_STATE_OPTIONS = TARGET_STATES;

export const findCityInText = (text: string): CityState | null => {
  const normalizedText = normalizeLocationKey(text);
  let best: { entry: CityState; index: number; key: string } | null = null;

  for (const key of CITY_KEYWORD_KEYS) {
    const idx = normalizedText.indexOf(key);
    if (idx === -1) continue;
    const beforeOk = idx === 0 || normalizedText[idx - 1] === " ";
    const afterOk = idx + key.length === normalizedText.length || normalizedText[idx + key.length] === " ";
    if (!beforeOk || !afterOk) continue;
    const entry = CITY_KEYWORDS.get(key);
    if (!entry) continue;
    if (
      best === null ||
      key.length > best.key.length ||
      (key.length === best.key.length && idx < best.index)
    ) {
      best = { entry, index: idx, key };
    }
  }

  return best ? best.entry : null;
};

export const isUnknownLocationValue = (value?: string | null) => {
  const normalized = (value || "").trim().toLowerCase();
  return (
    !normalized ||
    normalized === "unknown" ||
    normalized === "n/a" ||
    normalized === "n a" ||
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
  "u s a": "United States",
  "u s": "United States",
  "united states": "United States",
  "united states of america": "United States",
  us: "United States",
  america: "United States",
  uk: "United Kingdom",
  "united kingdom": "United Kingdom",
  britain: "United Kingdom",
  england: "United Kingdom",
  scotland: "United Kingdom",
  wales: "United Kingdom",
  eu: "Europe",
  europe: "Europe",
  spain: "Spain",
  espana: "Spain",
  france: "France",
  germany: "Germany",
  netherlands: "Netherlands",
  belgium: "Belgium",
  portugal: "Portugal",
  ireland: "Ireland",
  denmark: "Denmark",
  sweden: "Sweden",
  norway: "Norway",
  finland: "Finland",
  italy: "Italy",
  poland: "Poland",
  "czech republic": "Czech Republic",
  austria: "Austria",
  hungary: "Hungary",
  switzerland: "Switzerland",
  brazil: "Brazil",
  argentina: "Argentina",
  chile: "Chile",
  colombia: "Colombia",
  peru: "Peru",
  uruguay: "Uruguay",
  mexico: "Mexico",
  canada: "Canada",
  india: "India",
  singapore: "Singapore",
  "hong kong": "Hong Kong",
  japan: "Japan",
  "south korea": "South Korea",
  korea: "South Korea",
  china: "China",
  israel: "Israel",
  "united arab emirates": "United Arab Emirates",
  uae: "United Arab Emirates",
  australia: "Australia",
  "new zealand": "New Zealand",
  newzealand: "New Zealand",
  qatar: "Qatar",
};
const CANADIAN_PROVINCE_CODES = new Set([
  "AB",
  "BC",
  "MB",
  "NB",
  "NL",
  "NS",
  "NT",
  "NU",
  "ON",
  "PE",
  "QC",
  "SK",
  "YT",
]);
const CANADIAN_PROVINCE_NAMES = new Set([
  "alberta",
  "british columbia",
  "manitoba",
  "new brunswick",
  "newfoundland and labrador",
  "nova scotia",
  "northwest territories",
  "nunavut",
  "ontario",
  "prince edward island",
  "quebec",
  "saskatchewan",
  "yukon",
]);
const UNKNOWN_LOCATION_TOKENS = new Set(["unknown", "n/a", "n a", "na", "unspecified", "not available"]);

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
      if (!cleaned || cleaned.length < 2 || cleaned.length > 100) continue;
      const resolved = resolveLocationFromDictionary(cleaned);
      if (!resolved) continue;
      const label = formatLocationLabel(resolved.city, resolved.state, undefined, resolved.country);
      if (!label || seen.has(label)) continue;
      seen.add(label);
      normalized.push(label);
    }
  }

  return reorderByUsPreference(normalized);
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
    const { city, state, country } = splitLocation(loc);
    const inferredCountry = inferCountryFromLocation(loc);
    const parts = [loc, city, state, country ?? "", inferredCountry ?? ""];
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
  const resolved = resolveLocationFromDictionary(raw);
  if (resolved) {
    if (resolved.city.toLowerCase() === "remote" || resolved.state.toLowerCase() === "remote") {
      return "United States";
    }
    if (resolved.country && !isUnknownLocationValue(resolved.country)) return resolved.country;
    if (!isUnknownLocationValue(resolved.state)) return resolved.country ?? "United States";
  }

  const normalized = normalizeLocationKey(raw);
  if (!normalized) return null;
  if (normalized.includes("remote")) return "United States";
  if (UNKNOWN_LOCATION_TOKENS.has(normalized) || normalized.includes("unknown")) return "United States";

  const parts = raw.split(",").map((p) => p.trim()).filter(Boolean);
  for (const part of parts) {
    const normalizedPart = normalizeLocationKey(part);
    const upperPart = part.toUpperCase();
    if (COUNTRY_ALIASES[normalizedPart]) return COUNTRY_ALIASES[normalizedPart];
    if (CANADIAN_PROVINCE_CODES.has(upperPart) || CANADIAN_PROVINCE_NAMES.has(normalizedPart)) return "Canada";
    if (normalizeState(part)) return "United States";
  }

  if (COUNTRY_ALIASES[normalized]) return COUNTRY_ALIASES[normalized];
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
  const prioritizedLocations = reorderByUsPreference(normalizedLocations);
  const primaryLocationRaw = prioritizedLocations[0] ?? "Unknown";
  const { city, state, country: resolvedCountry } = splitLocation(primaryLocationRaw);
  const locationLabel = formatLocationLabel(city, state, primaryLocationRaw, resolvedCountry);
  const locations = prioritizedLocations.length ? prioritizedLocations : [locationLabel];
  const locationStates = deriveLocationStates(locations);
  const countries = deriveCountries(locations);
  if (resolvedCountry && !countries.includes(resolvedCountry)) {
    countries.unshift(resolvedCountry);
  }
  const isUnknownLocation = isUnknownLocationValue(primaryLocationRaw) || isUnknownLocationValue(locationLabel);
  const defaultCountry =
    locationLabel.toLowerCase().includes("remote") || isUnknownLocation ? "United States" : resolvedCountry ?? "Other";
  const country =
    countries.find((c) => c === "United States") ??
    countries[0] ??
    (locationStates.length > 0 && locationStates[0] !== "Unknown" ? "United States" : defaultCountry);
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
