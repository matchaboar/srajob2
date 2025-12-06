from __future__ import annotations

import json
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from ...constants import title_matches_required_keywords
from pydantic import BaseModel, ConfigDict, Field

DEFAULT_TOTAL_COMPENSATION = 0
MAX_DESCRIPTION_CHARS = 8000  # allow longer descriptions while fitting Convex document limits
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
_NAV_MENU_TERMS = set(_NAV_MENU_SEQUENCE + ["Careers"])

_NAV_BLOCK_REGEX = re.compile(
    r"(?:"
    + r"\s+".join(re.escape(term) for term in _NAV_MENU_SEQUENCE)
    + r")(?:\s+###\s*Careers)?(?:\s+"
    + r"\s+".join(re.escape(term) for term in _NAV_MENU_SEQUENCE)
    + r")?",
    flags=re.IGNORECASE,
)


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

    cleaned = _NAV_BLOCK_REGEX.sub("\n", markdown)

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


_TITLE_RE = re.compile(r"^[ \t]*#{1,6}\s+(?P<title>.+)$", flags=re.IGNORECASE | re.MULTILINE)
_LEVEL_RE = re.compile(
    r"\b(?P<level>intern|junior|mid(?:-level)?|mid|sr|senior|staff|principal|lead|manager|director|vp|cto|chief technology officer)\b",
    flags=re.IGNORECASE,
)
_LOCATION_RE = re.compile(
    r"\b(?:location|office|based\s+in)\s*[:\-–]\s*(?P<location>[^\n,;]+(?:,\s*[^\n,;]+)?)",
    flags=re.IGNORECASE,
)
_SIMPLE_LOCATION_LINE_RE = re.compile(
    r"^[ \t]*(?P<location>[A-Z][\w .'-]+,\s*[A-Z][\w .'-]+)\s*$", flags=re.MULTILINE
)
_SALARY_RE = re.compile(
    r"\$\s*(?P<low>\d{2,3}(?:[.,]\d{3})*)(?:\s*[-–]\s*\$?\s*(?P<high>\d{2,3}(?:[.,]\d{3})*))?"
    r"\s*(?P<period>per\s+year|per\s+annum|annual|yr|year|/year|per\s+hour|hr|hour)?",
    flags=re.IGNORECASE,
)
_SALARY_K_RE = re.compile(
    r"(?P<currency>[$£€])?\s*(?P<low>\d{2,3})\s*[kK]\s*(?:[-–]\s*(?P<high>\d{2,3})\s*[kK])?"
    r"\s*(?P<code>USD|EUR|GBP)?",
    flags=re.IGNORECASE,
)
_REMOTE_RE = re.compile(r"\b(remote(-first)?|hybrid|onsite|on-site)\b", flags=re.IGNORECASE)


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
        for part in re.split(r"[;|/]", raw):
            candidate = stringify(part)
            if not candidate:
                continue
            candidate = re.sub(r"\s+", " ", candidate).strip(" ,;/\t")
            if not candidate:
                continue
            lowered = candidate.lower()
            if lowered in ("unknown", "n/a", "na"):
                continue
            if len(candidate) < 3 or len(candidate) > 100:
                continue
            if not _is_plausible_location(candidate):
                continue
            if candidate not in seen:
                seen.add(candidate)
                normalized.append(candidate)
    return normalized


def _is_plausible_location(value: str) -> bool:
    lowered = value.lower()
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
    for line in markdown.splitlines():
        t = line.strip()
        if not t or t.startswith("#"):
            continue
        lower = t.lower()
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
        if "," in t:
            candidate_line = re.sub(r"^(?:location|office|based in)\s*[:\-–]\s*", "", t, flags=re.IGNORECASE)
            candidate = stringify(candidate_line)
            if candidate:
                location_candidates.extend([p.strip() for p in re.split(r"[;|/]", candidate) if p.strip()])
    if not location_candidates:
        loc_match = _LOCATION_RE.search(markdown) or _SIMPLE_LOCATION_LINE_RE.search(markdown)
        if loc_match:
            location_candidates.append(stringify(loc_match.group("location")))
    normalized_locations = _normalize_locations(location_candidates)
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
            cleaned_slug = re.sub(r"[^a-z0-9]+", " ", slug).strip()
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

    cleaned = re.sub(r"[^a-z0-9]+", " ", name).strip()
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
        numbers = re.findall(r"[0-9][0-9,\.]+", value.replace("\u00a0", " "))
        if numbers:
            try:
                parsed = max(float(num.replace(",", "")) for num in numbers)
                if parsed > 0:
                    return (int(parsed), False) if with_meta else int(parsed)
            except ValueError:
                pass
    return (0, True) if with_meta else 0


def extract_description(row: Dict[str, Any]) -> str:
    for key in ("description", "job_description", "desc", "body", "summary"):
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
    max_description: int = MAX_DESCRIPTION_CHARS,
    raw_preview_chars: int = 8000,
    request_max_chars: int = 4000,
) -> Dict[str, Any]:
    items = scrape.get("items", {})
    normalized: list[Dict[str, Any]] = []

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
    url = stringify(row.get("url") or row.get("link") or row.get("href") or row.get("_url") or "")
    if not url:
        return None
    if not title_matches_required_keywords(raw_title or None):
        return None

    company_raw = stringify(
        row.get("company") or row.get("employer") or row.get("organization") or ""
    )
    company = company_raw or derive_company_from_url(url) or "Unknown"

    location = stringify(row.get("location") or row.get("city") or row.get("region") or "")
    remote = coerce_remote(row.get("remote"), location, title)
    if not location:
        location = "Remote" if remote else "Unknown"

    level = coerce_level(row.get("level"), title)
    description = strip_known_nav_blocks(extract_description(row))
    if len(description) > MAX_DESCRIPTION_CHARS:
        description = description[:MAX_DESCRIPTION_CHARS]
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
        level_val = row.get("level") or "mid"
        if hints.get("level"):
            level_val = hints["level"]
        total_comp_val = row.get("total_compensation") or 0
        if (not total_comp_val or total_comp_val <= 0) and hints.get("compensation"):
            total_comp_val = hints["compensation"]
            compensation_unknown = False
            reason = "parsed from description"
        remote_val = bool(row.get("remote"))
        if hints.get("remote") is True:
            remote_val = True
        elif hints.get("remote") is False:
            remote_val = False

        job = {
            "title": title_val,
            "company": row.get("company") or "Unknown",
            "description": description,
            "location": location_val,
            "remote": remote_val,
            "level": level_val,
            "totalCompensation": int(total_comp_val or 0),
            "url": row.get("url") or "",
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
    "normalize_fetchfox_items",
    "normalize_firecrawl_items",
    "normalize_single_row",
    "parse_compensation",
    "parse_posted_at",
    "stringify",
    "trim_scrape_for_convex",
    "_jobs_from_scrape_items",
    "_shrink_payload",
    "_trim_request_snapshot",
]
