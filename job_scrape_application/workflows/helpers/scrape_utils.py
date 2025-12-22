from __future__ import annotations

import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from ...constants import is_remote_company, title_matches_required_keywords
from pydantic import BaseModel, ConfigDict, Field

from .link_extractors import dedupe_str_list, extract_links_from_payload

WHITESPACE_PATTERN = r"\s+"
ERROR_404_PATTERN = r"\b404\b"
TITLE_PATTERN = r"^[ \t]*#{1,6}\s+(?P<title>.+)$"
LEVEL_PATTERN = r"\b(?P<level>intern|junior|mid(?:-level)?|mid|sr|senior|staff|principal|lead|manager|director|vp|cto|chief technology officer)\b"
LOCATION_PATTERN = r"\b(?:location|office|based\s+in)\s*[:\-–]\s*(?P<location>[^\n,;]+(?:,\s*[^\n,;]+)?)"
SIMPLE_LOCATION_LINE_PATTERN = r"^[ \t]*(?P<location>[A-Z][\w .'-]+,\s*[A-Z][\w .'-]+)\s*$"
WORK_FROM_PATTERN = r"\bwork(?:ing)?\s+from\s+(?P<location>.+)$"
SALARY_PATTERN = (
    r"\$\s*(?P<low>(?:\d{2,3}(?:[.,]\d{3})+|\d{4,6}))"
    r"(?:\s*[-–]\s*\$?\s*(?P<high>(?:\d{2,3}(?:[.,]\d{3})+|\d{4,6})))?"
    r"\s*(?P<period>per\s+year|per\s+annum|annual|yr|year|/year|per\s+hour|hr|hour)?"
)
SALARY_RANGE_LABEL_PATTERN = (
    r"(?:salary|compensation|pay)\s+range\s*[:=\-–]\s*"
    r"(?P<low>(?:\d{2,3}(?:[.,]\d{3})+|\d{4,6}))"
    r"(?:\s*[-–]\s*(?P<high>(?:\d{2,3}(?:[.,]\d{3})+|\d{4,6})))?"
    r"(?:\s*(?P<code>USD|EUR|GBP))?"
)
SALARY_K_PATTERN = (
    r"(?P<currency>[$£€])?\s*(?P<low>\d{2,3})\s*[kK]\s*(?:[-–]\s*(?P<high>\d{2,3})\s*[kK])?"
    r"\s*(?P<code>USD|EUR|GBP)?"
)
REMOTE_PATTERN = r"\b(remote(-first)?|hybrid|onsite|on-site)\b"
PARENTHETICAL_PATTERN = r"\(.*?\)"
NON_ALNUM_SPACE_PATTERN = r"[^a-z0-9 ]+"
LOCATION_KEY_BOUNDARY_PATTERN_TEMPLATE = r"(?:^|\s){key}(?:\s|$)"
LOCATION_SPLIT_PATTERN = r"[;|/]"
LOCATION_PREFIX_PATTERN = r"^(?:location|office|offices|based in)\s*[:\-–]\s*"
NON_ALNUM_PATTERN = r"[^a-z0-9]+"
NUMBER_TOKEN_PATTERN = r"[0-9][0-9,\.]+"
RETIREMENT_PLAN_PATTERN = r"\b401\s*\(?k\)?\b"

DEFAULT_TOTAL_COMPENSATION = 0
# Limit used when persisting entire scrape payloads to Convex (keep scrape docs <1MB).
MAX_SCRAPE_DESCRIPTION_CHARS = 8000
# Higher ceiling for the actual job documents so the UI can render full descriptions.
MAX_JOB_DESCRIPTION_CHARS = 200_000
# Titles should be short; cap aggressively to prevent oversized payloads.
MAX_TITLE_CHARS = 500
# Backward compat alias (used only inside this module previously).
MAX_DESCRIPTION_CHARS = MAX_SCRAPE_DESCRIPTION_CHARS
UNKNOWN_COMPENSATION_REASON = "pending markdown structured extraction"
_NAV_MENU_SEQUENCE = [
    "Welcome",
    "Culture",
    "Workplace Benefits",
    "Candidate Experience",
    "Diversity, Equity & Inclusion",
    "Learning & Development",
    "Pup Culture Blog",
    "Teams",
    "Engineering",
    "General & Administrative",
    "Marketing",
    "Product Design",
    "Product Management",
    "Sales",
    "Technical Solutions",
    "Early Career & Internships",
    "Locations",
    "Americas",
    "Asia Pacific",
    "EMEA",
    "Remote",
    "All Jobs",
]
NAV_BLOCK_PATTERN = (
    r"(?:"
    + r"\s+".join(re.escape(term) for term in _NAV_MENU_SEQUENCE)
    + r")(?:\s+###\s*Careers)?(?:\s+"
    + r"\s+".join(re.escape(term) for term in _NAV_MENU_SEQUENCE)
    + r")?"
)
_COOKIE_SIGNAL_PATTERN = (
    r"(cookie\s+preferences|cookie\s+policy|cookie\s+consent|cookie\s+settings|"
    r"your\s+choice\s+regarding\s+cookies|this\s+website\s+uses\s+cookies|"
    r"accept\s+all|reject\s+all|save\s+and\s+close|manage\s+cookies|"
    r"essential\s+cookies|performance\s+cookies|functional\s+cookies|advertising\s+cookies|"
    r"cookiebot|onetrust|trustarc|optimizely|google\s+analytics|microsoft\s+clarity|"
    r"tag\s+manager|gtm)"
)
_COOKIE_SIGNAL_RE = re.compile(_COOKIE_SIGNAL_PATTERN, flags=re.IGNORECASE)
_COOKIE_WORD_RE = re.compile(r"\bcookies?\b", flags=re.IGNORECASE)
_COOKIE_UI_CONTROL_RE = re.compile(
    r"^(accept\s+all|reject\s+all|save\s+and\s+close|cookie\s+preferences|preferences)$",
    flags=re.IGNORECASE,
)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_AVATURE_TAIL_MARKERS = (
    "back to job search",
    "similar jobs",
)


def _score_apply_url(url: str) -> int:
    """Prefer company-hosted URLs over Greenhouse API endpoints.

    Higher scores are better. We want to avoid sending applicants to
    boards-api/api.greenhouse.io when a marketing/careers link exists.
    """

    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        host = ""

    if "boards-api.greenhouse.io" in host or host.startswith("api.greenhouse.io"):
        return 0  # least preferred: raw API endpoints
    if host.endswith("greenhouse.io"):
        return 1  # fallback: hosted Greenhouse job page
    if host:
        return 2  # best: company-owned domain
    return -1


def _strip_ashby_application_url(url: str) -> str:
    """Return the Ashby job overview URL when given an /application URL."""

    try:
        parsed = urlparse(url)
    except Exception:
        return url
    host = (parsed.hostname or "").lower()
    if not host.endswith("ashbyhq.com"):
        return url
    path = parsed.path or ""
    if not path.endswith("/application"):
        return url
    trimmed = path[: -len("/application")] or "/"
    return parsed._replace(path=trimmed).geturl()


def _apply_url_candidates(row: Dict[str, Any]) -> List[str]:
    """Collect plausible apply URLs from a normalized/raw row."""

    fields = (
        "apply_url",
        "applyUrl",
        "company_url",
        "companyUrl",
        "absolute_apply_url",
        "absoluteApplyUrl",
        "absolute_applyUrl",
        "absolute_apply_url",
        "absolute_url",
        "absoluteUrl",
        "job_url",
        "jobUrl",
        "url",
        "link",
        "href",
        "_url",
    )

    candidates: List[str] = []
    for key in fields:
        val = row.get(key)
        if isinstance(val, str) and val.strip():
            candidates.append(val.strip())
    return candidates


def prefer_apply_url(row: Dict[str, Any]) -> Optional[str]:
    """Return the preferred apply URL with a bias toward company domains."""

    candidates = _apply_url_candidates(row)
    if not candidates:
        return None

    best = None
    best_score = -2
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        normalized = _strip_ashby_application_url(candidate)
        if normalized in seen:
            continue
        seen.add(normalized)
        score = _score_apply_url(normalized)
        if score > best_score:
            best = normalized
            best_score = score

    return best
_NAV_MENU_TERMS = set(_NAV_MENU_SEQUENCE + ["Careers"])

# Phrases that typically appear on error/expired job landing pages. We only
# evaluate the first few hundred characters of title+body to avoid false
# positives from legitimate descriptions that happen to contain similar
# language deeper in the text.
_ERROR_LANDING_PHRASES = (
    "page not found",
    "job not found",
    "posting not found",
    "we can't find what you're looking for",
    "we can’t find what you're looking for",
    "could not find what you're looking for",
    "couldn't find what you're looking for",
    "no longer available",
    "no longer accepting applications",
    "no longer taking applications",
    "position has been filled",
    "position filled",
    "job has been filled",
    "job posting has expired",
    "posting has expired",
    "job has expired",
    "job is closed",
    "posting is closed",
)
_LISTING_FILTER_TERMS = (
    "open positions",
    "open position",
    "search for opportunities",
    "search for jobs",
    "search jobs",
    "select department",
    "select country",
    "select location",
    "select city",
    "select state",
    "select category",
    "search category",
    "all locations",
    "all teams",
    "all roles",
    "all types",
    "view openings",
    "available in multiple locations",
    "job fairs",
    "work programs",
    "view all jobs",
    "filter by",
)
_LISTING_SELECT_RE = re.compile(
    r"\bselect\s+(department|country|location|city|state|category|team)\b",
    flags=re.IGNORECASE,
)
_LISTING_TABLE_HEADER_RE = re.compile(
    r"\|\s*\*\*\s*role\s*\*\*.*\|\s*\*\*\s*(team|department)\s*\*\*.*\|\s*\*\*\s*type\s*\*\*.*\|\s*\*\*\s*location\s*\*\*",
    flags=re.IGNORECASE,
)
_JOB_DETAIL_MARKERS = (
    "responsibilities",
    "requirements",
    "qualifications",
    "what you'll do",
    "what you will do",
    "about the role",
    "about the position",
    "who you are",
    "benefits",
    "compensation",
    "salary",
    "equal opportunity",
)

_NAV_BLOCK_REGEX = re.compile(NAV_BLOCK_PATTERN, flags=re.IGNORECASE)


def build_job_template() -> Dict[str, str]:
    return {
        "job_title": "str | None",
        "company": "str | None",
        "description": "str | None",
        "url": "str | None",
        "location": "str | None",
        "remote": "True | False | None",
        "level": "junior | mid | senior | staff | lead | principal | director | manager | vp | cxo | intern | None",
        "salary": "str | number | None",
        "total_compensation": "number | None",
        "posted_at": "datetime | date | str | None",
    }


class FirecrawlJobSchema(BaseModel):
    job_title: Optional[str] = Field(default=None, alias="job_title")
    title: Optional[str] = None
    company: Optional[str] = None
    description: Optional[str] = None
    url: Optional[str] = None
    location: Optional[str] = None
    remote: Optional[bool] = None
    level: Optional[str] = None
    salary: Optional[str | float | int] = Field(default=None, alias="salary")
    total_compensation: Optional[str | float | int] = Field(default=None, alias="total_compensation")
    posted_at: Optional[str] = Field(default=None, alias="posted_at")

    model_config = ConfigDict(populate_by_name=True, extra="allow")


def build_firecrawl_schema() -> Dict[str, Any]:
    return FirecrawlJobSchema.model_json_schema() if hasattr(FirecrawlJobSchema, "model_json_schema") else {}


async def fetch_seen_urls_for_site(source_url: str, pattern: Optional[str]) -> List[str]:
    from ...services.convex_client import convex_query

    payload: Dict[str, Any] = {"sourceUrl": source_url}
    if pattern is not None:
        payload["pattern"] = pattern

    try:
        res = await convex_query("router:listSeenJobUrlsForSite", payload)
    except Exception:
        return []

    urls = res.get("urls", []) if isinstance(res, dict) else []
    return [u for u in urls if isinstance(u, str)]


def extract_raw_body_from_fetchfox_result(result: Any) -> str:
    if isinstance(result, str):
        return result

    if isinstance(result, dict):
        for key in ("raw_html", "html", "content", "body", "text"):
            val = result.get(key)
            if isinstance(val, str) and val.strip():
                return val

        nested_results = result.get("results")
        if isinstance(nested_results, dict):
            for key in ("raw_html", "html", "content", "body", "text"):
                val = nested_results.get(key)
                if isinstance(val, str) and val.strip():
                    return val

        nested_items = result.get("items")
        if isinstance(nested_items, list) and nested_items:
            first = nested_items[0]
            if isinstance(first, dict):
                for key in ("raw_html", "html", "content", "body", "text"):
                    val = first.get(key)
                    if isinstance(val, str) and val.strip():
                        return val

    try:
        return json.dumps(result, ensure_ascii=False)
    except Exception:
        return str(result)


def stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value.strip()
    return str(value)


def strip_known_nav_blocks(markdown: str) -> str:
    """Remove repeated navigation/footer menus scraped into markdown bodies."""

    if not markdown:
        return markdown

    cleaned = _strip_cookie_banner(markdown)
    cleaned = _strip_html_tag_lines(cleaned)
    cleaned = _NAV_BLOCK_REGEX.sub("\n", cleaned)
    cleaned = _strip_avature_tail(cleaned)

    def _normalize_line(line: str) -> str:
        return line.strip().lstrip("#").strip()

    lines = cleaned.splitlines()
    nav_indices = [i for i, line in enumerate(lines[:200]) if _normalize_line(line) in _NAV_MENU_TERMS]
    if len(nav_indices) < 8:
        return cleaned

    start = nav_indices[0]
    end = nav_indices[-1]
    if start > 120 or end - start > 200:
        return cleaned

    segment = lines[start : end + 1]
    non_empty = [ln for ln in segment if ln.strip()]
    if not non_empty:
        return cleaned

    nav_like = sum(1 for ln in segment if _normalize_line(ln) in _NAV_MENU_TERMS)
    if nav_like < max(8, int(len(non_empty) * 0.6)):
        return cleaned

    while start > 0 and not lines[start - 1].strip():
        start -= 1
    stop = end + 1
    while stop < len(lines):
        normalized = _normalize_line(lines[stop])
        if not lines[stop].strip() or normalized in _NAV_MENU_TERMS:
            stop += 1
            continue
        break

    trimmed = lines[:start] + lines[stop:]
    return "\n".join(trimmed).strip("\n") or cleaned.strip("\n")


def _strip_embedded_theme_json(markdown: str) -> str:
    """Remove embedded JSON theme blobs that sometimes appear in job descriptions."""

    if not markdown:
        return markdown

    markers = ('"themeOptions"', '"customTheme"', '"varTheme"', '"micrositeConfig"')
    output = markdown
    for _ in range(3):
        marker_index = next((output.find(marker) for marker in markers if marker in output), -1)
        if marker_index == -1:
            break
        start = output.rfind("{", 0, marker_index + 1)
        if start == -1:
            break
        depth = 0
        in_string = False
        escaped = False
        end = -1
        for idx in range(start, len(output)):
            char = output[idx]
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    in_string = False
                continue
            if char == '"':
                in_string = True
                continue
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    end = idx
                    break
        if end == -1:
            break
        output = f"{output[:start]} {output[end + 1:]}"

    return re.sub(WHITESPACE_PATTERN, " ", output).strip()


def _strip_avature_tail(markdown: str) -> str:
    if not markdown:
        return markdown
    lines = markdown.splitlines()
    for idx, line in enumerate(lines):
        lower = line.strip().lower()
        if not lower:
            continue
        if any(marker in lower for marker in _AVATURE_TAIL_MARKERS):
            trimmed = "\n".join(lines[:idx]).strip("\n")
            return trimmed or markdown
    return markdown


def _strip_html_tag_lines(markdown: str) -> str:
    if not markdown:
        return markdown
    lines = [line for line in markdown.splitlines() if not _is_html_tag_line(line)]
    return "\n".join(lines).strip("\n")


def _strip_cookie_banner(markdown: str) -> str:
    if not markdown:
        return markdown

    lines = markdown.splitlines()
    signal_indices = [i for i, line in enumerate(lines) if _COOKIE_SIGNAL_RE.search(line)]
    if len(signal_indices) < 2:
        return markdown

    if sum(1 for line in lines if _COOKIE_WORD_RE.search(line)) < 2:
        return markdown

    start = signal_indices[0]
    end = signal_indices[-1]
    if end - start > 240:
        return markdown

    while start > 0 and (not lines[start - 1].strip() or _is_html_tag_line(lines[start - 1])):
        start -= 1

    while end + 1 < len(lines):
        candidate = lines[end + 1]
        candidate_stripped = candidate.strip()
        if not candidate_stripped:
            end += 1
            continue
        if _is_html_tag_line(candidate):
            end += 1
            continue
        if _COOKIE_UI_CONTROL_RE.match(candidate_stripped):
            end += 1
            continue
        if _COOKIE_SIGNAL_RE.search(candidate):
            end += 1
            continue
        break

    cleaned = lines[:start] + lines[end + 1 :]
    return "\n".join(cleaned).strip("\n") or markdown.strip("\n")


def _is_html_tag_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped or not stripped.startswith("<") or not stripped.endswith(">"):
        return False
    return not _HTML_TAG_RE.sub("", stripped).strip()


def looks_like_error_landing(title: str | None, description: str) -> bool:
    """Heuristically detect generic error/expired landing pages.

    Many career sites return a branded 404/"job closed" page that still contains
    navigation text. These pages shouldn't be stored as jobs. We look for strong
    error phrases and the presence of "404" near the top of the combined
    title+body.
    """

    haystack = f"{title or ''} {description or ''}".lower()
    sample = re.sub(WHITESPACE_PATTERN, " ", haystack)[:700]

    if re.search(ERROR_404_PATTERN, sample):
        return True

    for phrase in _ERROR_LANDING_PHRASES:
        if phrase in sample:
            return True

    return False


def _url_suggests_listing(url: str | None) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    segments = [seg for seg in (parsed.path or "").split("/") if seg]
    for idx, seg in enumerate(segments[:-1]):
        if seg not in {"job", "jobs", "career", "careers"}:
            continue
        slug = segments[idx + 1]
        if not slug or re.search(r"\d", slug):
            return False
        normalized = _normalize_location_key(slug.replace("-", " ").replace("_", " "))
        if not normalized:
            return False
        if normalized in _COUNTRY_KEY_TO_LABEL:
            return True
        for state_name in _STATE_ABBR_BY_NAME:
            if _normalize_location_key(state_name) in normalized:
                return True
        if "remote" in normalized:
            return True
    return False


def looks_like_job_listing_page(title: str | None, description: str, url: str | None = None) -> bool:
    """Heuristically detect job board listing/filter pages rather than a single job."""

    if not description:
        return False
    haystack = f"{title or ''} {description or ''}".lower()
    sample = re.sub(WHITESPACE_PATTERN, " ", haystack)[:2000]
    link_hits = description.count("](")
    marker_hits = sum(1 for marker in _LISTING_FILTER_TERMS if marker in sample)
    select_hits = len(_LISTING_SELECT_RE.findall(sample))
    table_hits = bool(_LISTING_TABLE_HEADER_RE.search(sample))
    detail_hits = sum(1 for marker in _JOB_DETAIL_MARKERS if marker in sample)

    if "open positions" in sample and ("search for opportunities" in sample or select_hits >= 1):
        return True
    if table_hits and link_hits >= 5:
        return True
    if marker_hits >= 4:
        return True
    if marker_hits >= 3 and select_hits >= 1:
        return True
    if select_hits >= 3 and marker_hits >= 1:
        return True
    if link_hits >= 8 and marker_hits >= 2:
        return True
    if marker_hits >= 2 and _url_suggests_listing(url):
        return True
    if detail_hits >= 2 and marker_hits <= 2 and select_hits < 2:
        return False

    return False


_TITLE_RE = re.compile(TITLE_PATTERN, flags=re.IGNORECASE | re.MULTILINE)
_LEVEL_RE = re.compile(LEVEL_PATTERN, flags=re.IGNORECASE)
_LOCATION_RE = re.compile(LOCATION_PATTERN, flags=re.IGNORECASE)
_SIMPLE_LOCATION_LINE_RE = re.compile(SIMPLE_LOCATION_LINE_PATTERN, flags=re.MULTILINE)
_WORK_FROM_RE = re.compile(WORK_FROM_PATTERN, flags=re.IGNORECASE)
_SALARY_RE = re.compile(SALARY_PATTERN, flags=re.IGNORECASE)
_SALARY_RANGE_LABEL_RE = re.compile(SALARY_RANGE_LABEL_PATTERN, flags=re.IGNORECASE)
_SALARY_K_RE = re.compile(SALARY_K_PATTERN, flags=re.IGNORECASE)
_REMOTE_RE = re.compile(REMOTE_PATTERN, flags=re.IGNORECASE)


def _normalize_location_key(value: str) -> str:
    lowered = value.lower()
    lowered = re.sub(PARENTHETICAL_PATTERN, " ", lowered)
    lowered = re.sub(NON_ALNUM_SPACE_PATTERN, " ", lowered)
    lowered = re.sub(WHITESPACE_PATTERN, " ", lowered)
    return lowered.strip()


_STATE_NAME_BY_ABBR: dict[str, str] = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DC": "District of Columbia",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "IA": "Iowa",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "MA": "Massachusetts",
    "MD": "Maryland",
    "ME": "Maine",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MO": "Missouri",
    "MS": "Mississippi",
    "MT": "Montana",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "NE": "Nebraska",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NV": "Nevada",
    "NY": "New York",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VA": "Virginia",
    "VT": "Vermont",
    "WA": "Washington",
    "WI": "Wisconsin",
    "WV": "West Virginia",
    "WY": "Wyoming",
}
_STATE_ABBR_BY_NAME: dict[str, str] = {name: abbr for abbr, name in _STATE_NAME_BY_ABBR.items()}


def _format_location_label(city: str | None, state: str | None, country: str | None = None) -> str:
    clean_city = (city or "").strip()
    clean_state = (state or "").strip()
    clean_country = (country or "").strip()

    country_lower = clean_country.lower()
    state_label = clean_state
    if clean_state and country_lower in {"united states", "usa", "us", "united states of america"}:
        state_label = _STATE_ABBR_BY_NAME.get(clean_state, clean_state)

    if clean_city.lower() == "remote" or clean_state.lower() == "remote":
        return "Remote"

    if clean_city and state_label and clean_city != "Unknown" and state_label != "Unknown":
        return f"{clean_city}, {state_label}"
    if clean_city and clean_country and clean_country != "Unknown":
        return f"{clean_city}, {clean_country}"
    if clean_city and clean_city != "Unknown":
        return clean_city
    if state_label and state_label != "Unknown":
        return state_label
    if clean_country and clean_country != "Unknown":
        return clean_country
    return "Unknown"


_LOCATION_DICT_PATH = Path(__file__).resolve().parents[3] / "job_board_application" / "convex" / "locationDictionary.json"
try:
    _LOCATION_ENTRIES: list[dict[str, Any]] = json.loads(_LOCATION_DICT_PATH.read_text(encoding="utf-8"))
except FileNotFoundError:
    _LOCATION_ENTRIES = []

_LOCATION_DICTIONARY: dict[str, dict[str, Any]] = {}
_CITY_KEYWORDS: dict[str, dict[str, Any]] = {}
_COUNTRY_KEY_TO_LABEL: dict[str, str] = {}


def _register_location_key(value: str, entry: dict[str, Any], track_city: bool = False) -> None:
    key = _normalize_location_key(value)
    if not key or key in _LOCATION_DICTIONARY:
        return
    _LOCATION_DICTIONARY[key] = entry
    if track_city and not entry.get("remoteOnly"):
        _CITY_KEYWORDS[key] = entry


for _entry in _LOCATION_ENTRIES:
    city = (_entry.get("city") or "").strip()
    state = (_entry.get("state") or "").strip() or "Unknown"
    country = (_entry.get("country") or "").strip() or None
    remote_only = bool(_entry.get("remoteOnly"))
    state_abbr = _STATE_ABBR_BY_NAME.get(state)
    record = {"city": city, "state": state, "country": country, "remoteOnly": remote_only}
    country_key = _normalize_location_key(country)
    if country_key and country_key not in _COUNTRY_KEY_TO_LABEL:
        _COUNTRY_KEY_TO_LABEL[country_key] = country
    aliases = set([city, *(_entry.get("aliases") or [])])
    for alias in aliases:
        _register_location_key(alias, record, track_city=True)
        _register_location_key(f"{alias}, {state}", record)
        if country:
            _register_location_key(f"{alias}, {country}", record)
        if state_abbr:
            _register_location_key(f"{alias}, {state_abbr}", record)

_LOCATION_DICTIONARY_KEYS: list[tuple[str, dict[str, Any]]] = sorted(
    _LOCATION_DICTIONARY.items(), key=lambda item: len(item[0]), reverse=True
)
_CITY_KEYWORD_KEYS: list[str] = sorted(_CITY_KEYWORDS.keys(), key=len, reverse=True)


def _resolve_location_from_dictionary(value: str, allow_remote: bool = True) -> Optional[dict[str, Any]]:
    normalized = _normalize_location_key(value)
    if not normalized:
        return None

    direct = _LOCATION_DICTIONARY.get(normalized)
    if direct and (allow_remote or not direct.get("remoteOnly")):
        return direct

    for key, entry in _LOCATION_DICTIONARY_KEYS:
        if not allow_remote and entry.get("remoteOnly"):
            continue
        if entry.get("remoteOnly"):
            if normalized == key:
                return entry
            continue
        if key and len(key) >= 3 and re.search(
            LOCATION_KEY_BOUNDARY_PATTERN_TEMPLATE.format(key=re.escape(key)),
            normalized,
        ):
            return entry
    return None


def _find_city_in_text(text: str) -> Optional[dict[str, Any]]:
    normalized_text = _normalize_location_key(text)
    for key in _CITY_KEYWORD_KEYS:
        idx = normalized_text.find(key)
        if idx == -1:
            continue
        before_ok = idx == 0 or normalized_text[idx - 1] == " "
        after_ok = idx + len(key) == len(normalized_text) or normalized_text[idx + len(key)] == " "
        if before_ok and after_ok:
            entry = _CITY_KEYWORDS.get(key)
            if entry:
                return entry
    return None


def _normalize_country_label(value: str) -> Optional[str]:
    key = _normalize_location_key(value)
    if not key:
        return None
    return _COUNTRY_KEY_TO_LABEL.get(key)


def _to_int(value: str) -> Optional[int]:
    try:
        digits = value.replace(",", "").replace(".", "")
        return int(digits)
    except Exception:
        return None


def _normalize_locations(locations: List[str]) -> List[str]:
    seen: set[str] = set()
    normalized: List[str] = []
    for raw in locations:
        if not raw:
            continue
        for part in re.split(LOCATION_SPLIT_PATTERN, raw):
            candidate = stringify(part)
            if not candidate:
                continue
            candidate = re.sub(WHITESPACE_PATTERN, " ", candidate).strip(" ,;/\t")
            if not candidate:
                continue
            if not _is_plausible_location(candidate):
                continue
            resolved = _resolve_location_from_dictionary(candidate)
            if not resolved:
                country_label = _normalize_country_label(candidate)
                if country_label and country_label not in seen:
                    seen.add(country_label)
                    normalized.append(country_label)
                continue
            label = _format_location_label(resolved.get("city"), resolved.get("state"), resolved.get("country"))
            if label and label not in seen:
                seen.add(label)
                normalized.append(label)
    normalized = _reorder_by_us_preference(normalized)
    return normalized


def _reorder_by_us_preference(locations: List[str]) -> List[str]:
    prioritized = list(locations)

    def find_index(allow_remote: bool) -> int:
        for idx, loc in enumerate(prioritized):
            resolved = _resolve_location_from_dictionary(loc)
            if not resolved:
                continue
            country = (resolved.get("country") or "").strip()
            is_remote = (resolved.get("city") or "").lower() == "remote" or (resolved.get("state") or "").lower() == "remote"
            if not allow_remote and is_remote:
                continue
            if country == "United States":
                return idx
        return -1

    non_remote_idx = find_index(False)
    if non_remote_idx > 0:
        hit = prioritized.pop(non_remote_idx)
        prioritized.insert(0, hit)
        return prioritized

    remote_idx = find_index(True)
    if remote_idx > 0:
        hit = prioritized.pop(remote_idx)
        prioritized.insert(0, hit)

    return prioritized


def _is_plausible_location(value: str) -> bool:
    if not value or len(value) < 2 or len(value) > 100:
        return False
    lowered = value.lower().strip()
    if lowered in ("unknown", "n/a", "na"):
        return False
    if any(token in lowered for token in ("diversity", "equity", "inclusion", "benefits", "culture", "salary", "compensation", "pay", "package", "bonus", "range")):
        return False
    if "$" in value or "401k" in lowered or "401(k" in lowered:
        return False
    if "," in value:
        segments = [p.strip() for p in value.split(",") if p.strip()]
        if len(segments) > 3:
            return False
        if any(len(seg.split()) > 3 for seg in segments):
            return False
        if any("remote" in seg.lower() for seg in segments[1:]):
            return True
        return True
    if "remote" in lowered:
        return True
    return len(value.split()) <= 4


def parse_markdown_hints(markdown: str) -> Dict[str, Any]:
    """
    Extract lightweight hints (title, level, location, compensation, remote) from markdown text.
    Best-effort only; callers should treat results as optional overrides.
    """

    hints: Dict[str, Any] = {}
    if not markdown:
        return hints

    markdown = strip_known_nav_blocks(markdown)

    title_lower = ""
    if m := _TITLE_RE.search(markdown):
        title = stringify(m.group("title"))
        if title:
            hints["title"] = title
            title_lower = title.lower()

    if m := _LEVEL_RE.search(markdown):
        lvl = stringify(m.group("level")).lower()
        level_map = {
            "sr": "senior",
            "mid-level": "mid",
            "chief technology officer": "cto",
        }
        hints["level"] = level_map.get(lvl, lvl)

    # Prefer a lightweight line-based location guess (line under heading, short, with comma).
    location_candidates: List[str] = []
    location_section = False

    def _add_location_candidate(raw: str) -> None:
        candidate = stringify(raw)
        if not candidate:
            return
        lower_candidate = candidate.lower()
        if "remote" in lower_candidate and "," in candidate:
            for part in candidate.split(","):
                part_clean = part.strip()
                if part_clean:
                    location_candidates.append(part_clean)
            return
        location_candidates.append(candidate)

    for line in markdown.splitlines():
        t = line.strip()
        if not t or t.startswith("#"):
            continue
        lower = t.lower()
        if lower == "locations":
            location_section = True
            continue
        if location_section:
            location_section = False
            _add_location_candidate(t)
            continue
        if work_match := _WORK_FROM_RE.search(t):
            _add_location_candidate(work_match.group("location"))
            continue
        if lower.startswith("job application for"):
            continue
        if "|" in t or "career" in lower:
            continue
        if "http" in t:
            continue
        if len(t.split()) > 8:
            continue
        if any(keyword in lower for keyword in ("engineer", "developer", "manager", "designer", "product", "software", "data", "security", "analyst")):
            continue
        if title_lower and title_lower in lower:
            continue
        country_label = _normalize_country_label(
            re.sub(LOCATION_PREFIX_PATTERN, "", t, flags=re.IGNORECASE)
        )
        if country_label:
            location_candidates.append(country_label)
            continue
        if "," in t:
            candidate_line = re.sub(LOCATION_PREFIX_PATTERN, "", t, flags=re.IGNORECASE)
            candidate = stringify(candidate_line)
            if candidate:
                for part in [p.strip() for p in re.split(LOCATION_SPLIT_PATTERN, candidate) if p.strip()]:
                    _add_location_candidate(part)
    if not location_candidates:
        loc_match = _LOCATION_RE.search(markdown) or _SIMPLE_LOCATION_LINE_RE.search(markdown)
        if loc_match:
            location_candidates.append(stringify(loc_match.group("location")))
    normalized_locations = _normalize_locations(location_candidates)
    if not normalized_locations:
        city_hit = _find_city_in_text(markdown)
        if city_hit:
            fallback_label = _format_location_label(city_hit.get("city"), city_hit.get("state"), city_hit.get("country"))
            if fallback_label and fallback_label != "Unknown":
                normalized_locations.append(fallback_label)
    if normalized_locations:
        hints["locations"] = normalized_locations
        hints["location"] = normalized_locations[0]

    has_physical_location = any("remote" not in loc.lower() for loc in normalized_locations)
    remote_match = _REMOTE_RE.search(markdown)
    if remote_match:
        token = remote_match.group(1).lower()
        remote_hint: Optional[bool]
        if "remote" in token:
            remote_hint = True
        elif "hybrid" in token:
            remote_hint = False
        else:
            remote_hint = False
        if remote_hint is True:
            if not has_physical_location or any("remote" in loc.lower() for loc in normalized_locations):
                hints["remote"] = True
        else:
            hints["remote"] = False

    comp_candidates: List[int] = []
    comp_ranges: List[tuple[Optional[int], Optional[int]]] = []
    for salary_match in _SALARY_RANGE_LABEL_RE.finditer(markdown):
        low = salary_match.group("low")
        high = salary_match.group("high")
        low_val = _to_int(low) if low else None
        high_val = _to_int(high) if high else None
        if low_val or high_val:
            comp_ranges.append((low_val, high_val))
        if high_val:
            comp_candidates.append(high_val)
        elif low_val:
            comp_candidates.append(low_val)
    for salary_match in _SALARY_RE.finditer(markdown):
        low = salary_match.group("low")
        high = salary_match.group("high")
        period = (salary_match.group("period") or "").lower()
        if "hour" in period:
            continue
        low_val = _to_int(low) if low else None
        high_val = _to_int(high) if high else None
        if low_val or high_val:
            comp_ranges.append((low_val, high_val))
        if low_val and high_val:
            comp_candidates.append(int((low_val + high_val) / 2))
        elif low_val:
            comp_candidates.append(low_val)
    for salary_match in _SALARY_K_RE.finditer(markdown):
        raw_match = salary_match.group(0) or ""
        if "401k" in raw_match.lower():
            continue
        low_val = _to_int(salary_match.group("low")) if salary_match.group("low") else None
        high_val = _to_int(salary_match.group("high")) if salary_match.group("high") else None
        if low_val:
            low_val *= 1000
        if high_val:
            high_val *= 1000
        if low_val or high_val:
            comp_ranges.append((low_val, high_val))
        if low_val and high_val:
            comp_candidates.append(int((low_val + high_val) / 2))
        elif low_val:
            comp_candidates.append(low_val)
    comp_val = max((c for c in comp_candidates if c >= 1000), default=None)
    if comp_val is not None:
        hints["compensation"] = comp_val
    if comp_ranges:
        best_low, best_high = max(comp_ranges, key=lambda pair: ((pair[1] or pair[0] or 0)))
        range_payload = {k: v for k, v in (("low", best_low), ("high", best_high)) if v is not None}
        if range_payload:
            hints["compensation_range"] = range_payload

    return hints


def derive_company_from_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        hostname = parsed.hostname or ""
    except Exception:
        return ""

    hostname = hostname.lower()
    # Greenhouse boards encode the company slug in the path: /{company}/jobs/...
    if hostname.endswith("greenhouse.io"):
        parts = [p for p in parsed.path.split("/") if p]
        if parts:
            slug = parts[0]
            cleaned_slug = re.sub(NON_ALNUM_PATTERN, " ", slug).strip()
            if cleaned_slug:
                return cleaned_slug.title()

    for prefix in ("careers.", "jobs.", "boards.", "boards-", "job-", "boards-"):
        if hostname.startswith(prefix):
            hostname = hostname[len(prefix) :]
            break

    parts = hostname.split(".")
    if len(parts) >= 2:
        name = parts[-2]
    elif parts:
        name = parts[0]
    else:
        return ""

    cleaned = re.sub(NON_ALNUM_PATTERN, " ", name).strip()
    return cleaned.title() if cleaned else ""


def coerce_remote(value: Any, location: str, title: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.lower()
        if lowered in {"true", "yes", "remote", "hybrid", "fully remote"}:
            return True
    loc_lower = (location or "").lower()
    title_lower = (title or "").lower()
    return "remote" in loc_lower or "remote" in title_lower


def coerce_level(value: Any, title: str) -> str:
    normalized = value.lower() if isinstance(value, str) else ""
    title_lower = title.lower()
    markers = normalized or title_lower
    if any(token in markers for token in ("staff", "principal")):
        return "staff"
    if any(token in markers for token in ("senior", "sr ", "sr.", "sr-", "sr/")):
        return "senior"
    if any(token in markers for token in ("lead", "manager", "director", "vp", "chief", "head")):
        return "senior"
    if "intern" in markers:
        return "junior"
    if "jr" in markers or "junior" in markers:
        return "junior"
    return "mid"


def parse_compensation(value: Any, *, with_meta: bool = False) -> int | tuple[int, bool]:
    if isinstance(value, (int, float)) and value > 0:
        return (int(value), False) if with_meta else int(value)
    if isinstance(value, str):
        cleaned = value.replace("\u00a0", " ")
        has_retirement_token = re.search(RETIREMENT_PLAN_PATTERN, cleaned, flags=re.IGNORECASE) is not None
        if has_retirement_token:
            cleaned = re.sub(RETIREMENT_PLAN_PATTERN, " ", cleaned, flags=re.IGNORECASE)
        numbers = re.findall(NUMBER_TOKEN_PATTERN, cleaned)
        if numbers:
            try:
                parsed = max(float(num.replace(",", "")) for num in numbers)
                if parsed > 0:
                    if has_retirement_token and parsed < 1000:
                        return (0, True) if with_meta else 0
                    return (int(parsed), False) if with_meta else int(parsed)
            except ValueError:
                pass
    return (0, True) if with_meta else 0


def extract_description(row: Dict[str, Any]) -> str:
    for key in ("description", "job_description", "desc", "body", "summary", "content"):
        val = row.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    try:
        return json.dumps(row, ensure_ascii=False)
    except Exception:
        return str(row)


def parse_posted_at(value: Any) -> int:
    now_ms = int(time.time() * 1000)
    if value is None:
        return now_ms

    if isinstance(value, (int, float)):
        if value > 1e12:
            return int(value)
        if value > 1e9:
            return int(value * 1000)
        return now_ms

    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except Exception:
            pass

    return now_ms


def _shrink_payload(value: Any, max_chars: int) -> Any:
    if value is None:
        return None

    try:
        serialized = json.dumps(value, ensure_ascii=False)
    except Exception:
        try:
            serialized = str(value)
        except Exception:
            return None

    if len(serialized) <= max_chars:
        return value

    return f"{serialized[:max_chars]}... (+{len(serialized) - max_chars} chars)"


def _trim_request_snapshot(raw_request: Any, max_chars: int) -> Any:
    if raw_request is None:
        return None

    if isinstance(raw_request, dict) and (
        "body" in raw_request or "headers" in raw_request or "url" in raw_request or "method" in raw_request
    ):
        trimmed: Dict[str, Any] = {}
        if raw_request.get("method"):
            trimmed["method"] = raw_request.get("method")
        if raw_request.get("url"):
            trimmed["url"] = raw_request.get("url")
        if "body" in raw_request:
            trimmed_body = _shrink_payload(raw_request.get("body"), max_chars)
            if trimmed_body is not None:
                trimmed["body"] = trimmed_body
        if "headers" in raw_request:
            headers = raw_request.get("headers")
            if isinstance(headers, dict):
                masked_headers: Dict[str, Any] = {}
                for k, v in headers.items():
                    if isinstance(v, str):
                        # Lightly redact secrets while keeping shape visible
                        masked_headers[k] = f"{v[:4]}...{v[-2:]}" if len(v) > 6 else "***"
                    else:
                        masked_headers[k] = v
                trimmed["headers"] = masked_headers
        for meta_key in ("provider", "label"):
            if raw_request.get(meta_key) is not None:
                trimmed[meta_key] = raw_request.get(meta_key)
        return trimmed if trimmed else None

    return _shrink_payload(raw_request, max_chars)


def trim_scrape_for_convex(
    scrape: Dict[str, Any],
    *,
    max_items: int = 400,
    max_description: int = MAX_SCRAPE_DESCRIPTION_CHARS,
    max_title_chars: int = MAX_TITLE_CHARS,
    raw_preview_chars: int = 8000,
    request_max_chars: int = 4000,
) -> Dict[str, Any]:
    items = scrape.get("items", {})
    normalized: list[Dict[str, Any]] = []
    page_links: list[str] = []

    if isinstance(items, dict):
        raw_normalized = items.get("normalized", [])
        if isinstance(raw_normalized, list):
            truncated = len(raw_normalized) > max_items
            for row in raw_normalized[: max_items]:
                if not isinstance(row, dict):
                    continue
                new_row = dict(row)
                new_row.pop("_raw", None)
                desc = stringify(new_row.get("description", ""))
                if len(desc) > max_description:
                    new_row["description"] = desc[:max_description]
                job_desc = stringify(
                    new_row.get("job_description")
                    or new_row.get("jobDescription")
                    or ""
                )
                if job_desc and len(job_desc) > max_description:
                    new_row["job_description"] = job_desc[:max_description]
                for title_key in ("title", "job_title", "jobTitle"):
                    title_val = new_row.get(title_key)
                    if isinstance(title_val, str) and len(title_val) > max_title_chars:
                        new_row[title_key] = title_val[:max_title_chars]
                normalized.append(new_row)
        else:
            truncated = False
    else:
        truncated = False

    raw_preview = None
    if isinstance(items, dict) and "raw" in items and raw_preview_chars > 0:
        try:
            raw_str = json.dumps(items["raw"], ensure_ascii=False)
            raw_preview = raw_str[:raw_preview_chars]
        except Exception:
            raw_preview = None

    trimmed_items: Dict[str, Any] = {"normalized": normalized}
    if isinstance(items, dict) and "raw" in items:
        try:
            page_links = extract_links_from_payload(items.get("raw"), collect_all=True)
        except Exception:
            page_links = []
        if page_links:
            trimmed_items["page_links"] = dedupe_str_list(page_links, limit=2000)

    def _copy_meta(key: str, value: Any) -> None:
        if value is None:
            return
        if key == "seedUrls" and isinstance(value, list):
            trimmed_items[key] = value[:200]
            return
        trimmed_items[key] = value

    request_payload = scrape.get("request")
    provider_request = scrape.get("providerRequest")
    for key in (
        "provider",
        "costMilliCents",
        "workflowName",
        "asyncState",
        "jobId",
        "webhookId",
        "metadata",
        "statusUrl",
        "status",
        "providerVersion",
        "kind",
    ):
        _copy_meta(key, scrape.get(key))

    if isinstance(items, dict):
        for key, value in items.items():
            if key in {"normalized", "raw"}:
                continue
            _copy_meta(key, value)

    if raw_preview:
        if truncated:
            trimmed_items["rawPreview"] = raw_preview
        else:
            trimmed_items["raw"] = raw_preview

    trimmed: Dict[str, Any] = {k: v for k, v in scrape.items() if k not in {"items", "response", "asyncResponse"}}
    trimmed["items"] = trimmed_items

    if provider_request is not None:
        trimmed["providerRequest"] = _shrink_payload(provider_request, request_max_chars)
    if request_payload is not None:
        trimmed_request = _trim_request_snapshot(request_payload, request_max_chars)
        trimmed["request"] = trimmed_request
        # Mirror request into items for downstream expectations (tests/UI)
        if isinstance(trimmed_items, dict) and trimmed_request is not None:
            trimmed_items["request"] = trimmed_request
    if scrape.get("response") is not None:
        trimmed["response"] = _shrink_payload(scrape.get("response"), raw_preview_chars)
    if scrape.get("asyncResponse") is not None:
        trimmed["asyncResponse"] = _shrink_payload(scrape.get("asyncResponse"), raw_preview_chars)

    return trimmed


def normalize_firecrawl_items(payload: Any) -> List[Dict[str, Any]]:
    rows = _rows_from_firecrawl_payload(payload)
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        norm = normalize_single_row(row)
        if norm:
            normalized.append(norm)
    return normalized


def normalize_fetchfox_items(payload: Any) -> List[Dict[str, Any]]:
    def collect_rows(obj: Any) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if isinstance(obj, list):
            rows.extend([r for r in obj if isinstance(r, dict)])
            return rows

        if isinstance(obj, dict):
            if isinstance(obj.get("normalized"), list):
                rows.extend([r for r in obj["normalized"] if isinstance(r, dict)])
            if isinstance(obj.get("items"), list):
                rows.extend([r for r in obj["items"] if isinstance(r, dict)])
            if isinstance(obj.get("results"), list):
                rows.extend([r for r in obj["results"] if isinstance(r, dict)])
            results_obj = obj.get("results")
            if isinstance(results_obj, dict):
                if isinstance(results_obj.get("items"), list):
                    rows.extend([r for r in results_obj["items"] if isinstance(r, dict)])
                if isinstance(results_obj.get("normalized"), list):
                    rows.extend([r for r in results_obj["normalized"] if isinstance(r, dict)])
            if isinstance(obj.get("data"), dict):
                rows.extend(collect_rows(obj.get("data")))

        return rows

    raw_rows = collect_rows(payload)
    normalized: List[Dict[str, Any]] = []
    for row in raw_rows:
        norm = normalize_single_row(row)
        if norm:
            normalized.append(norm)

    return normalized


def normalize_single_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    raw_title_value = row.get("job_title") or row.get("title")
    raw_title = stringify(raw_title_value) if raw_title_value is not None else ""
    title = raw_title or stringify(row.get("job_title") or row.get("title") or "Untitled")
    preferred_url = prefer_apply_url(row)
    url = stringify(preferred_url) if preferred_url is not None else ""
    if not url:
        return None
    if not title_matches_required_keywords(raw_title or None):
        return None

    company_raw = stringify(
        row.get("company") or row.get("company_name") or row.get("employer") or row.get("organization") or ""
    )
    company = company_raw or derive_company_from_url(url) or "Unknown"

    raw_location = row.get("location") or row.get("city") or row.get("region") or ""
    if isinstance(raw_location, dict):
        raw_location = raw_location.get("name") or raw_location.get("location") or ""
    location = stringify(raw_location)
    remote = coerce_remote(row.get("remote"), location, title)
    if not location:
        location = "Remote" if remote else "Unknown"

    level = coerce_level(row.get("level"), title)
    description = strip_known_nav_blocks(extract_description(row))
    description = _strip_embedded_theme_json(description)
    if looks_like_job_listing_page(raw_title or title, description, url):
        return None
    if looks_like_error_landing(raw_title or title, description):
        return None
    if len(description) > MAX_JOB_DESCRIPTION_CHARS:
        description = description[:MAX_JOB_DESCRIPTION_CHARS]
    # Use markdown hints to fill missing data.
    hints = parse_markdown_hints(description)
    hinted_title = hints.get("title")
    if hinted_title and title.lower().startswith("job application for"):
        title = hinted_title
    hinted_location = hints.get("location")
    if hinted_location and location == "Unknown":
        location = hinted_location
    hinted_level = hints.get("level")
    if hinted_level:
        level = hinted_level
    if hints.get("remote") is True:
        remote = True
    elif hints.get("remote") is False:
        remote = False
    if is_remote_company(company):
        remote = True
        if not location or location == "Unknown":
            location = "Remote"
    hinted_comp = hints.get("compensation")
    total_comp, used_default_comp = parse_compensation(
        row.get("total_compensation") or row.get("salary") or row.get("compensation"),
        with_meta=True,
    )
    if hinted_comp is not None and total_comp <= 0:
        total_comp = hinted_comp
        used_default_comp = False
    posted_at = parse_posted_at(
        row.get("posted_at") or row.get("postedAt") or row.get("date") or row.get("_timestamp")
    )

    normalized_row: Dict[str, Any] = {
        "job_title": title,
        "title": title,
        "company": company,
        "location": location,
        "remote": remote,
        "level": level,
        "total_compensation": total_comp,
        "url": url,
        "description": description,
        "posted_at": posted_at,
    }
    raw_reason = row.get("compensation_reason") or row.get("compensationReason")
    if isinstance(raw_reason, str) and raw_reason.strip():
        normalized_row["compensation_reason"] = raw_reason.strip()
    normalized_row["compensation_unknown"] = used_default_comp
    if hinted_comp is not None and total_comp > 0:
        normalized_row["compensation_reason"] = "parsed from description"

    return normalized_row


def _parse_firecrawl_json(payload: Any) -> Any:
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except Exception:
            return None
    return payload


def _rows_from_firecrawl_payload(payload: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if isinstance(payload, list):
        rows.extend([item for item in payload if isinstance(item, dict)])
    elif isinstance(payload, dict):
        if "json" in payload:
            json_val = payload.get("json")
            if isinstance(json_val, dict):
                items_list = json_val.get("items") if isinstance(json_val, dict) else None
                if isinstance(items_list, list):
                    rows.extend([i for i in items_list if isinstance(i, dict)])
                else:
                    rows.append(json_val)
            elif isinstance(json_val, list):
                rows.extend([j for j in json_val if isinstance(j, dict)])
        items = payload.get("items")
        if isinstance(items, list):
            rows.extend([i for i in items if isinstance(i, dict)])
        else:
            rows.append(payload)
        data_block = payload.get("data")
        if isinstance(data_block, list):
            for entry in data_block:
                rows.extend(_rows_from_firecrawl_payload(entry))
        elif isinstance(data_block, dict):
            rows.extend(_rows_from_firecrawl_payload(data_block))
    return rows


def _jobs_from_scrape_items(
    items: Any,
    *,
    default_posted_at: int,
    scraped_at: Optional[int] = None,
    scraped_with: Optional[str] = None,
    workflow_name: Optional[str] = None,
    scraped_cost_milli_cents: Optional[int] = None,
) -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    normalized = None
    if isinstance(items, dict):
        normalized = items.get("normalized")
    if not isinstance(normalized, list):
        return jobs

    for row in normalized:
        if not isinstance(row, dict):
            continue
        description = stringify(row.get("description") or "")
        if len(description) > MAX_JOB_DESCRIPTION_CHARS:
            description = description[:MAX_JOB_DESCRIPTION_CHARS]
        hints = parse_markdown_hints(description)
        compensation_unknown = bool(row.get("compensation_unknown"))
        raw_comp_reason = row.get("compensation_reason")
        reason = raw_comp_reason.strip() if isinstance(raw_comp_reason, str) else None
        if not compensation_unknown:
            total_comp_value = row.get("total_compensation")
            if not isinstance(total_comp_value, (int, float)) or total_comp_value <= 0:
                compensation_unknown = True
        title_val = row.get("title") or row.get("job_title") or "Untitled"
        if hints.get("title") and str(title_val).lower().startswith("job application for"):
            title_val = hints["title"]
        location_val = row.get("location") or ""
        if not location_val and hints.get("location"):
            location_val = hints["location"]
        level_val = hints.get("level") or row.get("level") or "mid"
        level_val = coerce_level(level_val, str(title_val))
        total_comp_val = row.get("total_compensation") or 0
        if (not total_comp_val or total_comp_val <= 0) and hints.get("compensation"):
            total_comp_val = hints["compensation"]
            compensation_unknown = False
            reason = "parsed from description"
        company_val = row.get("company") or "Unknown"
        remote_val = bool(row.get("remote"))
        if hints.get("remote") is True:
            remote_val = True
        elif hints.get("remote") is False:
            remote_val = False
        if is_remote_company(company_val):
            remote_val = True
            if not location_val:
                location_val = "Remote"

        preferred_url = prefer_apply_url(row)
        apply_url = stringify(preferred_url) if preferred_url is not None else ""

        job = {
            "title": title_val,
            "company": company_val,
            "description": description,
            "location": location_val,
            "remote": remote_val,
            "level": level_val,
            "totalCompensation": int(total_comp_val or 0),
            "url": apply_url or row.get("url") or "",
            "postedAt": int(row.get("posted_at") or default_posted_at),
        }
        if not job["url"]:
            continue
        if scraped_at:
            job["scrapedAt"] = scraped_at
        if scraped_with:
            job["scrapedWith"] = scraped_with
        if workflow_name:
            job["workflowName"] = workflow_name
        if scraped_cost_milli_cents is not None:
            job["scrapedCostMilliCents"] = scraped_cost_milli_cents
        if compensation_unknown:
            job["compensationUnknown"] = True
        if reason:
            job["compensationReason"] = reason
        elif compensation_unknown:
            job["compensationReason"] = UNKNOWN_COMPENSATION_REASON
        elif scraped_with:
            job["compensationReason"] = f"{scraped_with} extracted compensation"
        else:
            job["compensationReason"] = "compensation provided in scrape payload"
        jobs.append(job)

    return jobs


__all__ = [
    "DEFAULT_TOTAL_COMPENSATION",
    "MAX_JOB_DESCRIPTION_CHARS",
    "MAX_DESCRIPTION_CHARS",
    "UNKNOWN_COMPENSATION_REASON",
    "build_firecrawl_schema",
    "build_job_template",
    "coerce_level",
    "coerce_remote",
    "derive_company_from_url",
    "extract_description",
    "extract_raw_body_from_fetchfox_result",
    "fetch_seen_urls_for_site",
    "looks_like_job_listing_page",
    "normalize_fetchfox_items",
    "normalize_firecrawl_items",
    "normalize_single_row",
    "parse_compensation",
    "parse_posted_at",
    "prefer_apply_url",
    "stringify",
    "trim_scrape_for_convex",
    "_jobs_from_scrape_items",
    "_shrink_payload",
    "_trim_request_snapshot",
]
