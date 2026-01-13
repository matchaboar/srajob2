from __future__ import annotations

import html as html_lib
import json
import re
import time
import unicodedata
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from ...constants import is_remote_company, title_matches_required_keywords
from pydantic import BaseModel, ConfigDict, Field

from .link_extractors import dedupe_str_list, extract_links_from_payload
from .compensation_parsing import (
    DEFAULT_TOTAL_COMPENSATION,
    HOURLY_TO_ANNUAL_MULTIPLIER,
    MAX_TOTAL_COMPENSATION,
    MIN_TOTAL_COMPENSATION,
    UNKNOWN_COMPENSATION_REASON,
    normalize_compensation_value,
    parse_compensation,
)
from .timestamp_parsing import (
    _RELATIVE_POSTED_MIN_DAYS,
    _RELATIVE_TIME_RE,
    _parse_relative_posted_at,
    parse_posted_at,
    parse_posted_at_with_unknown,
)
from .company_normalization import (
    _COMPANY_SUFFIX_RE,
    _GENERIC_COMPANY_HINTS,
    _JOB_BOARD_COMPANY_TOKENS,
    apply_company_hint,
    derive_company_from_url,
    is_generic_company_name,
    normalize_company_hint,
    normalize_title_from_bar,
)
from .location_normalization import (
    _CITY_KEYWORD_KEYS,
    _CITY_KEYWORDS,
    _COUNTRY_KEY_TO_LABEL,
    _LOCATION_DICTIONARY,
    _LOCATION_DICTIONARY_KEYS,
    _LOCATION_ENTRIES,
    _STATE_ABBR_BY_KEY,
    _STATE_ABBR_BY_NAME,
    _STATE_NAME_BY_ABBR,
    _find_city_in_text,
    _format_location_label,
    _is_plausible_location,
    _normalize_country_label,
    _normalize_location_key,
    _normalize_locations,
    _normalize_us_city_state,
    _register_location_key,
    _reorder_by_us_preference,
    _resolve_location_from_dictionary,
)
from .url_handling import (
    _apply_url_candidates,
    _first_url,
    _score_apply_url,
    _strip_ashby_application_url,
    prefer_apply_url,
)
from .page_detection import (
    _ERROR_LANDING_PHRASES,
    _INVALID_TITLE_PATTERNS,
    _INVALID_TITLE_RE,
    _JOB_DETAIL_MARKERS,
    _LISTING_CARD_APPLY_MARKERS,
    _LISTING_CARD_POSTED_RE,
    _LISTING_FILTER_TERMS,
    _LISTING_URL_TOKENS,
    _NON_JOB_DOMAINS,
    _NON_JOB_URL_PATTERNS,
    _description_mentions_listing_url,
    _looks_like_listing_card_snippet,
    _url_is_listing_root,
    _url_suggests_listing,
    is_invalid_job_title,
    is_invalid_job_url,
    looks_like_error_landing,
    looks_like_job_listing_page,
    looks_like_non_job_page,
)
from .regex_patterns import (
    DIGIT_PATTERN,
    ERROR_404_PATTERN,
    INVALID_JSON_ESCAPE_PATTERN,
    JSON_OBJECT_PATTERN,
    LOCATION_KEY_BOUNDARY_PATTERN_TEMPLATE,
    LOCATION_PREFIX_PATTERN,
    LOCATION_SPLIT_PATTERN,
    MARKDOWN_HEADING_PREFIX_PATTERN,
    NON_ALNUM_PATTERN,
    NON_ALNUM_SPACE_PATTERN,
    NUMBER_TOKEN_PATTERN,
    PARENTHETICAL_PATTERN,
    RETIREMENT_PLAN_PATTERN,
    WHITESPACE_PATTERN,
    _COOKIE_SIGNAL_RE,
    _COOKIE_UI_CONTROL_RE,
    _COOKIE_WORD_RE,
    _HTML_TAG_RE,
    _LEVEL_RE,
    _LISTING_SELECT_RE,
    _LISTING_TABLE_HEADER_RE,
    _LOCATION_RE,
    _NAV_BLOCK_REGEX,
    _NAV_MENU_SEQUENCE,
    _REMOTE_RE,
    _SALARY_BETWEEN_RE,
    _SALARY_HOURLY_RANGE_RE,
    _SALARY_K_RE,
    _SALARY_RANGE_LABEL_RE,
    _SALARY_RE,
    _SIMPLE_LOCATION_LINE_RE,
    _TITLE_RE,
    _TITLE_BAR_RE,
    _TITLE_IN_BAR_RE,
    _TITLE_IN_BAR_COMPANY_RE,
    _TITLE_LOCATION_PAREN_RE,
    _WORK_FROM_RE,
)
# Limit used when persisting entire scrape payloads to Convex (keep scrape docs <1MB).
MAX_SCRAPE_DESCRIPTION_CHARS = 8000
# Higher ceiling for the actual job documents so the UI can render full descriptions.
MAX_JOB_DESCRIPTION_CHARS = 200_000
# Titles should be short; cap aggressively to prevent oversized payloads.
MAX_TITLE_CHARS = 500
# Backward compat alias (used only inside this module previously).
MAX_DESCRIPTION_CHARS = MAX_SCRAPE_DESCRIPTION_CHARS

# Patterns for HTML to text conversion
_HTML_BR_PATTERN = re.compile(r"<br\s*/?>", flags=re.IGNORECASE)
_HTML_P_OPEN_PATTERN = re.compile(r"<p[^>]*>", flags=re.IGNORECASE)
_HTML_P_CLOSE_PATTERN = re.compile(r"</p>", flags=re.IGNORECASE)
_HTML_LI_PATTERN = re.compile(r"<li[^>]*>", flags=re.IGNORECASE)
_HTML_HEADING_PATTERN = re.compile(r"<h[1-6][^>]*>", flags=re.IGNORECASE)
_HTML_DIV_CLOSE_PATTERN = re.compile(r"</(?:div|section|article)>", flags=re.IGNORECASE)
_HTML_ALL_TAGS_PATTERN = re.compile(r"<[^>]+>")


def _html_description_to_text(html_content: str) -> str:
    """Convert HTML job description to readable plain text.

    This handles common HTML patterns from job board APIs (Greenhouse, Workday)
    where the description contains HTML tags like <p>, <li>, <br>, etc.
    Also handles double-escaped unicode sequences like \\u0026lt; from Greenhouse API.
    """
    if not html_content:
        return html_content

    text = html_content

    # Handle literal \uXXXX escape sequences in the content
    # These appear when APIs return encoded content that wasn't properly decoded
    if "\\u00" in text:
        try:
            # Decode literal unicode escapes (e.g., \u0026 -> &)
            text = text.encode("utf-8").decode("unicode_escape")
            # Then re-decode UTF-8 multi-byte sequences that were split
            text = text.encode("latin-1").decode("utf-8")
        except (UnicodeDecodeError, UnicodeEncodeError):
            # Fallback: just decode unicode escapes
            try:
                text = text.encode("utf-8").decode("unicode_escape")
            except (UnicodeDecodeError, UnicodeEncodeError):
                pass
    else:
        # Handle improperly decoded UTF-8 bytes (chars in 0x80-0xff range)
        try:
            text = text.encode("latin-1").decode("utf-8")
        except (UnicodeDecodeError, UnicodeEncodeError):
            pass

    # Unescape HTML entities (handles &lt; &gt; &amp; etc.)
    text = html_lib.unescape(text)

    # Check if it still looks like HTML after unescaping
    if "<" not in text or ">" not in text:
        return text.strip()

    # Convert line breaks
    text = _HTML_BR_PATTERN.sub("\n", text)

    # Convert paragraph boundaries to double newlines
    text = _HTML_P_OPEN_PATTERN.sub("\n\n", text)
    text = _HTML_P_CLOSE_PATTERN.sub("\n", text)

    # Convert list items to bullet points
    text = _HTML_LI_PATTERN.sub("\n- ", text)

    # Convert headings to newlines with emphasis
    text = _HTML_HEADING_PATTERN.sub("\n\n## ", text)

    # Convert div/section closes to newlines
    text = _HTML_DIV_CLOSE_PATTERN.sub("\n", text)

    # Strip all remaining HTML tags
    text = _HTML_ALL_TAGS_PATTERN.sub("", text)

    # Decode any remaining HTML entities (in case of double-encoding)
    text = html_lib.unescape(text)

    # Normalize whitespace: collapse multiple spaces (but preserve newlines)
    lines = text.splitlines()
    cleaned_lines = []
    for line in lines:
        cleaned = " ".join(line.split())
        cleaned_lines.append(cleaned)

    # Collapse multiple consecutive empty lines to at most 2
    result_lines: List[str] = []
    empty_count = 0
    for line in cleaned_lines:
        if not line:
            empty_count += 1
            if empty_count <= 2:
                result_lines.append(line)
        else:
            empty_count = 0
            result_lines.append(line)

    return "\n".join(result_lines).strip()


_AVATURE_TAIL_MARKERS = (
    "back to job search",
    "similar jobs",
)
_SNAP_TAIL_MARKERS = (
    "ready to join team snap",
    "life at snap",
)
_EMBEDDED_JSON_ALWAYS_DROP_MARKERS = (
    '"display_banner"',
    '"display_text"',
)
_EMBEDDED_JSON_BLOB_MARKERS = (
    '"domain"',
    '"positions"',
    '"branding"',
    '"candidate"',
    '"custom_html"',
    '"custom_style"',
    '"customNavbarItems"',
    '"themeOptions"',
    '"customTheme"',
    '"varTheme"',
    '"micrositeConfig"',
    '"i18n_overrides_master"',
)
_JIBEAPPLY_DROP_LINES = {
    "back",
    "carousel_paragraph",
    "get future jobs matching this search",
    "job_description.share.html",
    "loginorregister",
    "mail_outline",
}
_GENERIC_DROP_LINE_SUBSTRINGS = (
    "enable javascript to run this app",
)
_JUNK_UPPER_LINE_RE = re.compile(r"^[A-Z0-9_.]{8,}$")
_EMBEDDED_JSON_MIN_LEN = 200
_EMBEDDED_JSON_HUGE_LEN = 1200
_DESCRIPTION_SECTION_MARKERS = {
    "job description",
    "job details",
    "job detail",
    "job summary",
    "job overview",
    "role description",
    "position description",
    "position details",
    "description and requirements",
    "description and requirement",
    "description and responsibilities",
    "description & requirements",
    "description & requirement",
    "description & responsibilities",
    "description",
}
_METADATA_LABEL_KEYS = {
    "location",
    "locations",
    "business area",
    "business unit",
    "department",
    "team",
    "function",
    "work type",
    "time type",
    "employment type",
    "shift",
    "job id",
    "job number",
    "job req id",
    "requisition id",
    "posting id",
    "position id",
    "reference",
    "ref",
    "req",
    "req id",
    "ref id",
    "ref #",
    "salary",
    "compensation",
    "pay",
    "rate",
    "office",
    "city",
    "state",
    "country",
}


_NAV_MENU_TERMS = set(_NAV_MENU_SEQUENCE + ["Careers"])

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


async def fetch_seen_urls_for_site(
    source_url: str,
    pattern: Optional[str],
    candidate_urls: Optional[List[str]] = None,
) -> List[str]:
    from ...services.convex_client import convex_query

    payload: Dict[str, Any] = {"sourceUrl": source_url}
    if pattern is not None:
        payload["pattern"] = pattern
    if candidate_urls is not None:
        cleaned_candidates = [
            url.strip() for url in candidate_urls if isinstance(url, str) and url.strip()
        ]
        if cleaned_candidates:
            payload["urls"] = cleaned_candidates

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


def _parse_lenient_json(text: str) -> Any | None:
    cleaned = text.strip()
    if not cleaned:
        return None
    try:
        return json.loads(cleaned, strict=False)
    except Exception:
        pass
    try:
        unescaped = cleaned.encode("utf-8", errors="ignore").decode("unicode_escape")
    except Exception:
        unescaped = ""
    if unescaped:
        try:
            return json.loads(unescaped, strict=False)
        except Exception:
            return None
    return None


def _first_string(value: Any) -> Optional[str]:
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned:
            return cleaned
        return None
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str) and item.strip():
                return item.strip()
    return None


def _is_job_detail_payload(node: dict[str, Any]) -> bool:
    desc = node.get("jobDescription") or node.get("description") or node.get("content")
    if not isinstance(desc, str) or len(desc.strip()) < 80:
        return False
    if "jobDescription" in node or "content" in node:
        return True
    for key in (
        "positionUrl",
        "publicUrl",
        "atsJobId",
        "jobId",
        "jobPostingId",
        "displayJobId",
        "workLocationOption",
        "standardizedLocations",
        "locations",
        "location",
    ):
        if key in node:
            return True
    if any(key in node for key in ("name", "title", "jobTitle", "job_title")):
        return True
    return False


def _find_job_detail_payload(node: Any) -> Optional[dict[str, Any]]:
    if isinstance(node, dict):
        if _is_job_detail_payload(node):
            return node
        for val in node.values():
            found = _find_job_detail_payload(val)
            if found:
                return found
    if isinstance(node, list):
        for child in node:
            found = _find_job_detail_payload(child)
            if found:
                return found
    return None


def _extract_job_markdown_from_json(markdown: str) -> Optional[str]:
    if not markdown:
        return None
    trimmed = markdown.strip()
    if not trimmed:
        return None
    # Strip code fence markers if present (e.g., ```json\n{...}\n```)
    if trimmed.startswith("```"):
        # Remove opening fence line
        lines = trimmed.split("\n", 1)
        if len(lines) > 1:
            trimmed = lines[1]
        else:
            trimmed = ""
        # Remove closing fence
        if trimmed.rstrip().endswith("```"):
            trimmed = trimmed.rstrip()[:-3].rstrip()
        trimmed = trimmed.strip()
        # Strip markdown escape sequences (e.g., \_ -> _) common in code blocks
        trimmed = trimmed.replace("\\_", "_")
    if not trimmed or not trimmed.startswith(("{", "[")):
        return None
    payload = _parse_lenient_json(trimmed)
    if payload is None:
        return None
    job_payload = _find_job_detail_payload(payload)
    if not job_payload:
        return None
    raw_description = (
        job_payload.get("jobDescription")
        or job_payload.get("description")
        or job_payload.get("content")  # Greenhouse API uses 'content' field
    )
    if not isinstance(raw_description, str) or not raw_description.strip():
        return None
    # Convert HTML description to readable text (handles HTML entities and tags)
    description = _html_description_to_text(raw_description)
    if not description:
        return None
    title = _first_string(job_payload.get("name"))
    if not title:
        title = _first_string(job_payload.get("title"))
    if not title:
        title = _first_string(job_payload.get("jobTitle"))
    if not title:
        title = _first_string(job_payload.get("job_title"))
    # Normalize title whitespace and decode HTML entities
    if title:
        title = " ".join(title.split())
        title = html_lib.unescape(title)
    location = (
        _first_string(job_payload.get("standardizedLocations"))
        or _first_string(job_payload.get("locations"))
        or _first_string(job_payload.get("location"))
        or _first_string(job_payload.get("jobLocation"))
    )
    url = (
        _first_url(job_payload.get("publicUrl"))
        or _first_url(job_payload.get("positionUrl"))
        or _first_url(job_payload.get("canonicalPositionUrl"))
        or _first_url(job_payload.get("jobUrl"))
        or _first_url(job_payload.get("url"))
    )
    company = derive_company_from_url(url) if url else None
    if company and is_generic_company_name(company):
        company = None
    work_site = (
        _first_string(job_payload.get("workLocationOption"))
        or _first_string(job_payload.get("locationFlexibility"))
        or _first_string(job_payload.get("workLocation"))
        or _first_string(job_payload.get("workLocationType"))
    )

    header_lines: List[str] = []
    if title and location and company:
        header_lines.append(f"{title} in {location} | {company}")
    else:
        if title:
            header_lines.append(f"# {title}")
        if location:
            header_lines.append(f"Location: {location}")
        if company:
            header_lines.append(f"Company: {company}")
    if work_site:
        header_lines.append(f"Work Location: {work_site}")

    body = description.strip()
    parts = [line for line in header_lines if line]
    if body:
        parts.append(body)
    return "\n\n".join(parts).strip() if parts else None


def strip_known_nav_blocks(markdown: str) -> str:
    """Remove repeated navigation/footer menus scraped into markdown bodies."""

    if not markdown:
        return markdown

    extracted_json = _extract_job_markdown_from_json(markdown)
    if extracted_json:
        markdown = extracted_json

    cleaned = _strip_cookie_banner(markdown)
    cleaned = _strip_html_tag_lines(cleaned)
    cleaned = _NAV_BLOCK_REGEX.sub("\n", cleaned)
    cleaned = _strip_avature_tail(cleaned)
    cleaned = _strip_snap_tail(cleaned)
    cleaned = _strip_embedded_json_blobs(cleaned)
    cleaned = _strip_empty_link_lines(cleaned)
    cleaned = _strip_platform_tokens(cleaned)
    cleaned = _strip_application_form_sections(cleaned)

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


def _strip_application_form_sections(markdown: str) -> str:
    if not markdown:
        return markdown

    def _normalize_line(value: str) -> str:
        trimmed = value.strip()
        trimmed = re.sub(MARKDOWN_HEADING_PREFIX_PATTERN, "", trimmed).strip()
        trimmed = trimmed.strip("*").strip()
        trimmed = re.sub(WHITESPACE_PATTERN, " ", trimmed).strip().lower()
        return trimmed

    stop_markers = {
        "apply for this job",
        "create a job alert",
        "submit application",
        "autofill with mygreenhouse",
        "voluntary self-identification",
        "equal opportunity employment information",
    }

    lines = markdown.splitlines()
    stop_idx = None
    for idx, line in enumerate(lines):
        normalized = _normalize_line(line)
        if not normalized:
            continue
        if normalized in stop_markers:
            stop_idx = idx
            break
        if normalized.startswith("apply for this job") or normalized.startswith("create a job alert"):
            stop_idx = idx
            break
    if stop_idx is None:
        return markdown
    return "\n".join(lines[:stop_idx]).strip()


def _strip_empty_link_lines(markdown: str) -> str:
    if not markdown:
        return markdown

    def _is_empty_link_line(value: str) -> bool:
        if not value:
            return False
        stripped = value.strip()
        if stripped in {"[", "* [", "- [", "• ["}:
            return True
        if re.fullmatch(r"[\*\-•]?\s*\[\s*\]", stripped):
            return True
        if re.fullmatch(r"\]\(\s*#?\s*\)", stripped):
            return True
        if re.fullmatch(r"\[\s*\]\(\s*#?\s*\)", stripped):
            return True
        return False

    lines = []
    for line in markdown.splitlines():
        if _is_empty_link_line(line.strip()):
            continue
        lines.append(line)
    return "\n".join(lines)


def _strip_platform_tokens(markdown: str) -> str:
    if not markdown:
        return markdown

    jibe_substrings = [token for token in _JIBEAPPLY_DROP_LINES if token != "back"]
    cleaned_lines: List[str] = []
    for line in markdown.splitlines():
        stripped = line.strip()
        if not stripped:
            cleaned_lines.append(line)
            continue
        normalized = re.sub(WHITESPACE_PATTERN, " ", stripped).strip().lower()
        if any(token in normalized for token in _GENERIC_DROP_LINE_SUBSTRINGS):
            continue
        if any(token in normalized for token in jibe_substrings):
            continue
        if normalized in _JIBEAPPLY_DROP_LINES:
            continue
        if _JUNK_UPPER_LINE_RE.fullmatch(stripped):
            continue
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines)


def _is_separator_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return True
    if all(ch == "#" for ch in stripped):
        return True
    if re.fullmatch(r"[-*_]{3,}", stripped):
        return True
    return False


def _trim_separator_lines(lines: List[str]) -> List[str]:
    if not lines:
        return lines
    start = 0
    end = len(lines)
    while start < end and _is_separator_line(lines[start]):
        start += 1
    while end > start and _is_separator_line(lines[end - 1]):
        end -= 1
    return lines[start:end]


def _normalize_section_heading(line: str) -> str:
    text = line.strip()
    if not text:
        return ""
    text = html_lib.unescape(text)
    text = re.sub(r"^[#*\-\u2022]+\s*", "", text)
    text = text.strip().rstrip(":").strip()
    if not text:
        return ""
    text = text.replace("&", "and")
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def _line_is_metadata_label(line: str) -> bool:
    normalized = _normalize_section_heading(line)
    if not normalized:
        return False
    if normalized in _METADATA_LABEL_KEYS:
        return True
    if normalized.endswith(" id") and len(normalized.split()) <= 3:
        return True
    if normalized.startswith(("ref", "req")) and len(normalized.split()) <= 2:
        return True
    return False


def _line_is_numeric(line: str) -> bool:
    stripped = re.sub(r"^[#*\-\u2022]+\s*", "", line).strip()
    if not stripped:
        return False
    return re.fullmatch(r"[A-Za-z]{0,3}\d{3,}", stripped) is not None


def _looks_like_metadata_block(lines: List[str]) -> bool:
    cleaned = [line.strip() for line in lines if line.strip() and not _is_separator_line(line)]
    if len(cleaned) < 3 or len(cleaned) > 40:
        return False
    label_hits = 0
    numeric_hits = 0
    short_hits = 0
    for line in cleaned:
        stripped = re.sub(r"^[#*\-\u2022]+\s*", "", line).strip()
        if not stripped:
            continue
        if _line_is_metadata_label(stripped):
            label_hits += 1
        if _line_is_numeric(stripped):
            numeric_hits += 1
        if len(stripped) <= 80 and not stripped.endswith("."):
            short_hits += 1
    short_ratio = short_hits / max(len(cleaned), 1)
    if label_hits >= 2 and short_ratio >= 0.6:
        return True
    if label_hits >= 1 and numeric_hits >= 1 and short_ratio >= 0.6:
        return True
    if numeric_hits >= 1 and short_ratio >= 0.8 and len(cleaned) >= 4:
        return True
    return False


def split_description_metadata(markdown: str) -> tuple[str, Optional[str]]:
    """Split metadata-like headers from descriptions.

    Returns a tuple of (cleaned_description, metadata_block). The metadata block
    is only returned when the header section looks like a short list of labels
    (e.g. Location/Ref #) preceding a description heading.
    """

    if not markdown:
        return markdown, None

    lines = markdown.splitlines()
    heading_idx: Optional[int] = None
    for idx, line in enumerate(lines):
        normalized = _normalize_section_heading(line)
        if not normalized:
            continue
        if normalized in _DESCRIPTION_SECTION_MARKERS:
            heading_idx = idx
            break

    if heading_idx is None:
        return markdown, None

    prefix_lines = _trim_separator_lines(lines[:heading_idx])
    suffix_lines = _trim_separator_lines(lines[heading_idx + 1 :])

    prefix_text = "\n".join(prefix_lines).strip("\n")
    suffix_text = "\n".join(suffix_lines).strip("\n")

    if prefix_text and suffix_text and _looks_like_metadata_block(prefix_lines):
        return suffix_text, prefix_text

    merged: List[str] = []
    if prefix_lines:
        merged.extend(prefix_lines)
    if prefix_lines and suffix_lines:
        merged.append("")
    if suffix_lines:
        merged.extend(suffix_lines)

    description = "\n".join(merged).strip("\n") or markdown.strip("\n")
    return description, None


def _strip_embedded_theme_json(markdown: str) -> str:
    """Remove embedded JSON theme blobs that sometimes appear in job descriptions."""

    if not markdown:
        return markdown

    markers = ("themeOptions", "customTheme", "varTheme", "micrositeConfig")

    def _is_escaped_quote(text: str, index: int) -> bool:
        if index <= 0 or text[index] != '"':
            return False
        backslashes = 0
        cursor = index - 1
        while cursor >= 0 and text[cursor] == "\\":
            backslashes += 1
            cursor -= 1
        return (backslashes % 2) == 1

    def _find_marker_index(text: str) -> int:
        earliest: Optional[int] = None
        for token in [f'"{marker}"' for marker in markers] + [f'\\"{marker}\\"' for marker in markers]:
            idx = text.find(token)
            if idx != -1 and (earliest is None or idx < earliest):
                earliest = idx
        if earliest is not None:
            return earliest
        for marker in markers:
            idx = text.find(marker)
            if idx != -1 and (earliest is None or idx < earliest):
                earliest = idx
        return earliest if earliest is not None else -1

    def _find_json_span(text: str, marker_index: int) -> Optional[tuple[int, int]]:
        start = text.rfind("{", 0, marker_index + 1)
        if start == -1:
            return None
        depth = 0
        in_string = False
        end = None
        for idx in range(start, len(text)):
            char = text[idx]
            if char == '"' and not _is_escaped_quote(text, idx):
                in_string = not in_string
                continue
            if in_string:
                continue
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    end = idx
                    break
        if end is None:
            return None
        return start, end

    output = markdown
    trimmed = False
    for _ in range(3):
        marker_index = _find_marker_index(output)
        if marker_index == -1:
            break
        span = _find_json_span(output, marker_index)
        if span is None:
            break
        start, end = span
        output = f"{output[:start]} {output[end + 1:]}"
        trimmed = True

    if trimmed:
        output = re.sub(r"[ \t]+", " ", output)
        output = re.sub(r"[ \t]*\n[ \t]*", "\n", output)
        output = re.sub(r"\n{3,}", "\n\n", output)
    return output.strip()


def _strip_embedded_json_blobs(markdown: str) -> str:
    """Remove large inline JSON blobs that are not part of the job description."""

    if not markdown:
        return markdown

    def _unwrap_backticks(text: str) -> str:
        stripped = text.strip()
        if stripped.startswith("`") and stripped.endswith("`") and len(stripped) >= 2:
            return stripped.strip("`").strip()
        return stripped

    def _looks_like_json_block(text: str) -> bool:
        candidate = _unwrap_backticks(text)
        if not candidate:
            return False
        if not (candidate.startswith("{") or candidate.startswith("[")):
            return False
        if not (candidate.endswith("}") or candidate.endswith("]")):
            return False
        return True

    def _should_drop_json_block(text: str) -> bool:
        candidate = _unwrap_backticks(text)
        if not candidate:
            return False
        if not (candidate.startswith("{") or candidate.startswith("[")):
            return False
        quote_hits = candidate.count('":')
        if quote_hits < 2:
            return False
        if any(marker in candidate for marker in _EMBEDDED_JSON_ALWAYS_DROP_MARKERS):
            return True
        if len(candidate) < _EMBEDDED_JSON_MIN_LEN:
            return False
        if any(marker in candidate for marker in _EMBEDDED_JSON_BLOB_MARKERS):
            return True
        if len(candidate) >= _EMBEDDED_JSON_HUGE_LEN and (candidate.count("{") + candidate.count("[")) >= 2:
            return True
        return False

    cleaned_lines: List[str] = []
    buffer: List[str] = []
    in_block = False

    for line in markdown.splitlines():
        stripped = line.strip()
        if not in_block:
            if stripped and stripped.lstrip().startswith(("`{", "`[", "{", "[")):
                buffer = [line]
                if _looks_like_json_block(stripped):
                    if not _should_drop_json_block(stripped):
                        cleaned_lines.append(line)
                    buffer = []
                else:
                    in_block = True
                continue
            cleaned_lines.append(line)
            continue

        buffer.append(line)
        if _looks_like_json_block(stripped):
            block_text = "\n".join(buffer)
            if not _should_drop_json_block(block_text):
                cleaned_lines.extend(buffer)
            buffer = []
            in_block = False

    if buffer:
        cleaned_lines.extend(buffer)

    return "\n".join(cleaned_lines).strip("\n") or markdown.strip("\n")


def _strip_avature_tail(markdown: str) -> str:
    if not markdown:
        return markdown
    lines = markdown.splitlines()
    lower_lines = [line.strip().lower() for line in lines]

    action_link_idx = None
    action_tokens = ("/careers/login", "/careers/savejob", "/careers/searchjobs")
    for idx, line in enumerate(lower_lines):
        if any(token in line for token in action_tokens):
            action_link_idx = idx
            break

    if action_link_idx is not None:
        min_idx = max(8, int(len(lines) * 0.2))
        if action_link_idx >= min_idx:
            start_idx = action_link_idx
            bracket_tokens = {"[", "* [", "- [", "• ["}
            label_tokens = {"apply now", "save this job", "back to job search"}

            if lower_lines[action_link_idx] in label_tokens:
                start_idx = action_link_idx
            elif action_link_idx > 0 and lower_lines[action_link_idx - 1] in label_tokens:
                start_idx = action_link_idx - 1
                if action_link_idx > 1 and lower_lines[action_link_idx - 2] in bracket_tokens:
                    start_idx = action_link_idx - 2
            elif action_link_idx > 1 and lower_lines[action_link_idx - 2] in label_tokens:
                start_idx = action_link_idx - 2
                if lower_lines[action_link_idx - 1] in bracket_tokens:
                    start_idx = action_link_idx - 2
            elif action_link_idx > 0 and lower_lines[action_link_idx - 1] in bracket_tokens:
                start_idx = action_link_idx - 1

            trimmed = "\n".join(lines[:start_idx]).strip("\n")
            if trimmed:
                return trimmed
    for idx, line in enumerate(lines):
        lower = line.strip().lower()
        if not lower:
            continue
        if any(marker in lower for marker in _AVATURE_TAIL_MARKERS):
            trimmed = "\n".join(lines[:idx]).strip("\n")
            return trimmed or markdown
    return markdown


def _strip_snap_tail(markdown: str) -> str:
    if not markdown:
        return markdown
    lines = markdown.splitlines()

    def _normalize_line(line: str) -> str:
        return line.strip().lstrip("#").strip().lower()

    for idx, line in enumerate(lines):
        normalized = _normalize_line(line)
        if not normalized:
            continue
        if any(marker in normalized for marker in _SNAP_TAIL_MARKERS):
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


def _to_int(value: str) -> Optional[int]:
    try:
        cleaned = value.strip()
        if "." in cleaned:
            head, tail = cleaned.rsplit(".", 1)
            if tail.isdigit() and len(tail) == 2:
                cleaned = head
        if "," in cleaned and "." not in cleaned:
            head, tail = cleaned.rsplit(",", 1)
            if tail.isdigit() and len(tail) == 2:
                cleaned = head
        digits = cleaned.replace(",", "").replace(".", "")
        return int(digits)
    except Exception:
        return None


def parse_markdown_hints(markdown: str) -> Dict[str, Any]:
    """
    Extract lightweight hints (title, level, location, compensation, remote) from markdown text.
    Best-effort only; callers should treat results as optional overrides.
    """

    hints: Dict[str, Any] = {}
    if not markdown:
        return hints

    seed_description, seed_hints = _extract_job_detail_seed_from_json(markdown)
    if seed_description:
        markdown = seed_description

    markdown = strip_known_nav_blocks(markdown)
    markdown = html_lib.unescape(markdown)
    _description_body, metadata_block = split_description_metadata(markdown)

    def _is_generic_heading_title(value: str) -> bool:
        lower = value.strip().lower().rstrip(":.")
        lower = lower.replace("&amp;", "&").replace("&", "and")
        return lower in {
            "job description",
            "description",
            "description and requirements",
            "minimum qualifications",
            "preferred qualifications",
            "qualifications",
            "minimum requirements",
            "preferred requirements",
            "requirements",
            "the role",
            "our team",
            "the team",
            "about the team",
            "about stripe",
            "what's in it for you",
            "whats in it for you",
            "why this matters",
            "we'll trust you to",
            "well trust you to",
            "you'll need to have",
            "youll need to have",
            "we'd love to see",
            "wed love to see",
            "why join us",
            "stay in the loop",
        }

    def _looks_like_sentence(value: str) -> bool:
        trimmed = value.strip()
        if not trimmed:
            return False
        check_text = trimmed
        if " | " in trimmed:
            check_text = trimmed.split(" | ", 1)[0].strip()
        lowered = trimmed.lower()
        check_lower = check_text.lower()
        if check_lower.endswith((".", "!", "?")):
            return True
        if re.search(r"\b(?:we|our|you|your|you'll|you\u2019ll|join us)\b", lowered):
            return True
        if len(lowered.split()) > 20:
            return True
        if len(lowered.split()) > 14 and any(token in lowered for token in (",", ";")):
            return True
        return False

    title_lower = ""
    title_location_hint: Optional[str] = None
    company_hint: Optional[str] = None
    job_application_title: Optional[str] = None

    def _record_company(value: Optional[str]) -> None:
        nonlocal company_hint
        if not value or company_hint:
            return
        cleaned = normalize_company_hint(html_lib.unescape(value))
        if cleaned:
            company_hint = cleaned

    seed_title = seed_hints.get("title") if isinstance(seed_hints, dict) else None
    if seed_title and isinstance(seed_title, str):
        if not markdown.lstrip().lower().startswith(seed_title.lower()):
            markdown = f"# {seed_title}\n\n{markdown}".strip()
    seed_company = seed_hints.get("company") if isinstance(seed_hints, dict) else None
    if isinstance(seed_company, str) and seed_company.strip():
        _record_company(seed_company)
    seed_location = seed_hints.get("location") if isinstance(seed_hints, dict) else None
    if isinstance(seed_location, str) and seed_location.strip() and not title_location_hint:
        title_location_hint = seed_location

    def _looks_like_title_location(value: str) -> bool:
        if not value:
            return False
        lowered = value.lower()
        if "remote" in lowered:
            return True
        if "," in value:
            return True
        if _normalize_country_label(value):
            return True
        if _resolve_location_from_dictionary(value) is not None:
            return True
        return False

    def _looks_like_title_line(value: str) -> bool:
        trimmed = value.strip()
        lowered = trimmed.lower()
        if not lowered:
            return False
        check_text = trimmed
        if " | " in trimmed:
            check_text = trimmed.split(" | ", 1)[0].strip()
        check_lower = check_text.lower()
        if lowered in {
            "location",
            "locations",
            "business area",
            "ref #",
            "ref#",
            "description",
            "description & requirements",
            "requirements",
        }:
            return False
        if lowered.startswith(("location", "business area", "ref #", "ref#", "description")):
            return False
        if lowered.startswith("chat ") or "chat with" in lowered or "chat now" in lowered:
            return False
        if "salary" in lowered or "compensation" in lowered:
            return False
        if _SALARY_RE.search(value) or _SALARY_K_RE.search(value):
            return False
        if _SALARY_RANGE_LABEL_RE.search(value) or _SALARY_BETWEEN_RE.search(value):
            return False
        if any(token in lowered for token in ("apply", "direct apply", "apply with ai", "apply now", "back to job search", "save this job")):
            return False
        if any(token in lowered for token in ("cookie", "privacy", "consent")):
            return False
        if re.fullmatch(r"\[[^\]]+\]\([^)]+\)", trimmed):
            return False
        if "posted" in lowered and ("ago" in lowered or re.search(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", lowered)):
            return False
        if re.fullmatch(r"\d+\s+words?", lowered):
            return False
        if check_lower.endswith((".", "!", "?")):
            return False
        if len(check_lower.split()) < 2:
            return False
        if len(check_lower.split()) > 12:
            return False
        if len(check_lower) > 120:
            return False
        title_keywords = (
            "engineer",
            "executive",
            "developer",
            "sales",
            "account",
            "manager",
            "marketer",
            "marketing",
            "designer",
            "product",
            "software",
            "data",
            "security",
            "analyst",
            "program",
            "director",
            "lead",
            "specialist",
            "architect",
            "scientist",
        )
        if _looks_like_title_location(value) and not any(keyword in lowered for keyword in title_keywords):
            return False
        return True

    def _extract_company_from_metadata(block: str) -> Optional[str]:
        if not block:
            return None
        label_pending = False
        bullet_candidates: List[str] = []
        other_candidates: List[str] = []

        for raw_line in block.splitlines():
            if not raw_line.strip():
                continue
            stripped = re.sub(MARKDOWN_HEADING_PREFIX_PATTERN, "", raw_line).strip()
            if not stripped:
                continue
            cleaned = re.sub(r"^[#*\-\u2022]+", "", stripped).strip()
            if not cleaned:
                continue
            if _line_is_metadata_label(cleaned):
                label_pending = True
                continue
            if label_pending:
                label_pending = False
                continue
            if _line_is_numeric(cleaned):
                continue
            if cleaned.strip("#") == "":
                continue
            cleaned_lower = cleaned.lower()
            if title_lower and (cleaned_lower == title_lower or title_lower in cleaned_lower):
                continue
            if _looks_like_title_line(cleaned):
                continue
            if _resolve_location_from_dictionary(cleaned) is not None:
                continue
            if len(cleaned.split()) > 4:
                continue
            if raw_line.lstrip().startswith(("-", "*", "•")):
                bullet_candidates.append(cleaned)
            else:
                other_candidates.append(cleaned)

        if bullet_candidates:
            return bullet_candidates[0]
        if other_candidates:
            return other_candidates[0]
        return None

    job_application_pattern = re.compile(
        r"job\s+application\s+for\s+(?P<title>.+?)(?:\s+at\s+(?P<company>.+))?$",
        flags=re.IGNORECASE,
    )
    for line in markdown.splitlines()[:10]:
        cleaned = re.sub(MARKDOWN_HEADING_PREFIX_PATTERN, "", line).strip()
        match = job_application_pattern.match(cleaned)
        if not match:
            continue
        job_application_title = stringify(match.group("title"))
        _record_company(stringify(match.group("company")))
        break

    def _extract_title_and_location_from_line(
        line: str,
    ) -> tuple[Optional[str], Optional[str], Optional[str]]:
        if not line:
            return None, None, None
        company = None
        bar_company_match = _TITLE_IN_BAR_COMPANY_RE.match(line)
        if bar_company_match:
            title = stringify(bar_company_match.group("title"))
            location = stringify(bar_company_match.group("location"))
            company = stringify(bar_company_match.group("company"))
            return title, location, company
        bar_match = _TITLE_IN_BAR_RE.match(line) or _TITLE_BAR_RE.match(line)
        if bar_match:
            title = stringify(bar_match.group("title"))
            location = stringify(bar_match.groupdict().get("location")) if bar_match.groupdict() else None
            return title, location, None
        paren_match = _TITLE_LOCATION_PAREN_RE.match(line)
        if paren_match:
            paren_title = stringify(paren_match.group(1))
            paren_location = stringify(paren_match.group(2))
            if _looks_like_title_location(paren_location):
                return paren_title, paren_location, None
            stripped_line = line.strip()
            if stripped_line.startswith("](") or re.match(r"^\]\([^)]+\)$", stripped_line):
                return paren_title, None, None
            return stringify(line), None, None
        return None, None, None

    for match in _TITLE_RE.finditer(markdown):
        raw_title = stringify(match.group("title"))
        raw_title = html_lib.unescape(raw_title)
        if not raw_title:
            continue
        if not raw_title.strip().strip("#"):
            continue
        if _is_generic_heading_title(raw_title) or _looks_like_sentence(raw_title):
            continue
        if not _looks_like_title_line(raw_title):
            continue
        title, location, parsed_company = _extract_title_and_location_from_line(raw_title)
        title = title or raw_title
        hints["title"] = title
        title_lower = title.lower()
        _record_company(parsed_company)
        if location and _looks_like_title_location(location):
            title_location_hint = location
        elif " in " in title and not title_location_hint:
            head, tail = title.rsplit(" in ", 1)
            if tail and _looks_like_title_location(tail):
                hints["title"] = head.strip() or title
                title_lower = hints["title"].lower()
                title_location_hint = tail
        break
    if "title" not in hints:
        label_markers = {
            "location",
            "locations",
            "business area",
            "ref #",
            "ref#",
        }
        prev_label: Optional[str] = None
        for line in markdown.splitlines()[:12]:
            t = line.strip()
            if not t:
                continue
            lower = t.lower()
            if lower in ("job description", "description"):
                continue
            if lower.startswith(("back", "[ back")):
                continue
            if lower in label_markers:
                prev_label = lower
                continue
            if t.startswith(("#", "*", "-", "•")):
                continue
            if prev_label:
                prev_label = None
                continue
            candidate_title, candidate_location, parsed_company = _extract_title_and_location_from_line(t)
            if not candidate_title:
                if _looks_like_title_line(t):
                    hints["title"] = t
                    title_lower = t.lower()
                    break
                continue
            hints["title"] = candidate_title
            title_lower = candidate_title.lower()
            _record_company(parsed_company)
            if candidate_location and _looks_like_title_location(candidate_location):
                title_location_hint = candidate_location
            break
    if "title" not in hints and job_application_title:
        hints["title"] = job_application_title
        title_lower = job_application_title.lower()
    if not company_hint:
        company_link_re = re.compile(
            r"^\s*\[(?P<company>[^\]]+)\]\([^)]+\)\s+is\s+(?:a|an|the)\b",
            flags=re.IGNORECASE,
        )
        for line in markdown.splitlines()[:40]:
            t = line.strip()
            if not t:
                continue
            cleaned = re.sub(r"^[#*\-\u2022]+", "", t).strip()
            lower = cleaned.lower()
            link_match = company_link_re.match(cleaned)
            if link_match:
                _record_company(stringify(link_match.group("company")))
                if company_hint:
                    break
            if lower.startswith("about "):
                _record_company(cleaned[6:].strip())
            elif lower.startswith("about:"):
                _record_company(cleaned.split(":", 1)[-1].strip())
            if company_hint:
                break
    if not company_hint and metadata_block:
        metadata_company = _extract_company_from_metadata(metadata_block)
        if metadata_company:
            _record_company(metadata_company)
    if company_hint:
        hints["company"] = company_hint

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

    def _trim_inline_location(value: str) -> str:
        cleaned = stringify(value)
        if not cleaned:
            return ""
        cleaned = re.sub(WHITESPACE_PATTERN, " ", cleaned).strip()
        if not cleaned:
            return ""
        for marker in (
            "Overview",
            "About",
            "Responsibilities",
            "Qualifications",
            "Compensation",
            "Benefits",
            "Summary",
            "Description",
            "Job Description",
        ):
            idx = cleaned.find(marker)
            if idx > 0:
                cleaned = cleaned[:idx].strip()
                break
        return cleaned

    def _clean_location_candidate(value: str) -> str:
        cleaned = stringify(value)
        if not cleaned:
            return ""
        cleaned = re.sub(r"^remote\s*[-–—]\s*", "remote ", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\b(headquarters|hq)\b", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(WHITESPACE_PATTERN, " ", cleaned).strip(" ,;/\t")
        remote_match = re.match(r"^remote\s*(?:in|for)?\s+(?P<rest>.+)$", cleaned, flags=re.IGNORECASE)
        if remote_match:
            cleaned = remote_match.group("rest").strip(" ,;/\t") or "Remote"
        return cleaned

    def _add_location_candidate(raw: str) -> None:
        candidate = _clean_location_candidate(raw)
        if not candidate:
            return
        lower_candidate = candidate.lower()
        if re.search(r"\s+(?:or|and)\s+", lower_candidate):
            for part in re.split(r"\s+(?:or|and)\s+", candidate):
                part_clean = _clean_location_candidate(part)
                if part_clean:
                    location_candidates.append(part_clean)
            return
        if "remote" in lower_candidate and "," in candidate:
            for part in candidate.split(","):
                part_clean = part.strip()
                if part_clean:
                    location_candidates.append(part_clean)
            return
        if "remote" in lower_candidate and any(token in candidate for token in (" - ", " – ", " — ")):
            for part in re.split(r"\s*[-–—]\s*", candidate):
                part_clean = part.strip()
                if part_clean:
                    location_candidates.append(part_clean)
            return
        location_candidates.append(candidate)

    if title_location_hint and _looks_like_title_location(title_location_hint):
        _add_location_candidate(title_location_hint)

    for line in markdown.splitlines():
        t = line.strip()
        if not t:
            continue
        if t.startswith("#"):
            heading_value = re.sub(MARKDOWN_HEADING_PREFIX_PATTERN, "", t).strip()
            if heading_value and _looks_like_title_location(heading_value):
                _add_location_candidate(heading_value)
            continue
        lower = t.lower()
        if lower in {"locations", "office location", "office locations", "remote location", "remote locations"}:
            location_section = True
            continue
        if location_section:
            location_section = False
            _add_location_candidate(t)
            continue
        if lower.startswith("remote ") and len(t.split()) <= 6:
            _add_location_candidate(t)
            continue
        if lower.startswith("remote-") and len(t.split()) <= 6:
            _add_location_candidate(t.replace("remote-", "remote ").strip())
            continue
        if work_match := _WORK_FROM_RE.search(t):
            location_text = _trim_inline_location(work_match.group("location"))
            _add_location_candidate(location_text)
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
    for candidate in location_candidates:
        candidate_text = stringify(candidate)
        if not candidate_text or not _is_plausible_location(candidate_text):
            continue
        resolved = _resolve_location_from_dictionary(candidate_text)
        if not resolved:
            country_label = _normalize_country_label(candidate_text)
            if not country_label:
                continue
            candidate_text = country_label
            resolved = _resolve_location_from_dictionary(candidate_text)
        break

    normalized_locations = _normalize_locations(location_candidates)
    if not normalized_locations:
        city_hit = _find_city_in_text(markdown)
        if city_hit:
            fallback_label = _format_location_label(city_hit.get("city"), city_hit.get("state"), city_hit.get("country"))
            if fallback_label and fallback_label != "Unknown":
                normalized_locations.append(fallback_label)
    if normalized_locations:
        hints["locations"] = normalized_locations
        non_remote_locations = [loc for loc in normalized_locations if "remote" not in loc.lower()]
        hints["location"] = non_remote_locations[0] if non_remote_locations else normalized_locations[0]

    has_physical_location = any("remote" not in loc.lower() for loc in normalized_locations)
    remote_tokens: List[str] = []
    remote_false_positive_re = re.compile(
        r"\bremote\s+(?:access|control|controls|monitoring|sensing|desktop|operation|operations|support)\b",
        flags=re.IGNORECASE,
    )
    # LinkedIn recruiter tags (#LI-REMOTE, #LI-HYBRID, etc.) are not reliable indicators
    linkedin_tag_re = re.compile(r"#LI-\w*(?:remote|hybrid|onsite)\w*", flags=re.IGNORECASE)
    for line in markdown.splitlines():
        lowered_line = line.lower()
        if "remote" not in lowered_line and "hybrid" not in lowered_line and "onsite" not in lowered_line:
            continue
        if remote_false_positive_re.search(line):
            continue
        # Skip lines containing LinkedIn recruiter tags
        if linkedin_tag_re.search(line):
            continue
        stripped_line = lowered_line.strip()
        context_ok = (
            len(stripped_line) <= 80
            or any(
                token in stripped_line
                for token in (
                    "remote work",
                    "work remotely",
                    "work from",
                    "remote role",
                    "remote position",
                    "remote location",
                    "based in",
                )
            )
            or stripped_line.startswith("remote")
        )
        for match in _REMOTE_RE.finditer(line):
            token = match.group(1).lower()
            if token in {"hybrid", "onsite", "on-site"} and not context_ok:
                continue
            if "remote" in token and not context_ok:
                continue
            remote_tokens.append(token)
    has_non_remote_clue = any(token in ("hybrid", "onsite", "on-site") for token in remote_tokens)
    has_remote_clue = any("remote" in token for token in remote_tokens)
    if has_non_remote_clue:
        hints["remote"] = False
    elif has_remote_clue:
        hints["remote"] = True
    elif has_physical_location:
        hints["remote"] = False

    if hints.get("remote") is True and "location" not in hints:
        hints["location"] = "Remote"
        if not hints.get("locations"):
            hints["locations"] = ["Remote"]

    comp_candidates: List[int] = []
    comp_ranges: List[tuple[Optional[int], Optional[int]]] = []

    def _record_comp_range(low_val: Optional[int], high_val: Optional[int], *, prefer_high: bool = False) -> None:
        low_norm = normalize_compensation_value(low_val) if low_val is not None else None
        high_norm = normalize_compensation_value(high_val) if high_val is not None else None
        if not low_norm and not high_norm:
            return
        comp_ranges.append((low_norm, high_norm))
        if low_norm and high_norm:
            if prefer_high:
                comp_candidates.append(high_norm)
            else:
                candidate = normalize_compensation_value(int((low_norm + high_norm) / 2))
                if candidate is not None:
                    comp_candidates.append(candidate)
        elif low_norm:
            comp_candidates.append(low_norm)
        elif high_norm:
            comp_candidates.append(high_norm)
    for salary_match in _SALARY_BETWEEN_RE.finditer(markdown):
        low = salary_match.group("low")
        high = salary_match.group("high")
        low_val = _to_int(low) if low else None
        high_val = _to_int(high) if high else None
        _record_comp_range(low_val, high_val, prefer_high=True)
    for salary_match in _SALARY_RANGE_LABEL_RE.finditer(markdown):
        low = salary_match.group("low")
        high = salary_match.group("high")
        low_val = _to_int(low) if low else None
        high_val = _to_int(high) if high else None
        _record_comp_range(low_val, high_val, prefer_high=True)
    for salary_match in _SALARY_RE.finditer(markdown):
        low = salary_match.group("low")
        high = salary_match.group("high")
        period = (salary_match.group("period") or "").lower()
        if "hour" in period:
            continue
        low_val = _to_int(low) if low else None
        high_val = _to_int(high) if high else None
        _record_comp_range(low_val, high_val)
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
        _record_comp_range(low_val, high_val)
    for salary_match in _SALARY_HOURLY_RANGE_RE.finditer(markdown):
        low_raw = salary_match.group("low")
        high_raw = salary_match.group("high")
        try:
            low_val = float(low_raw.replace(",", "")) if low_raw else None
        except Exception:
            low_val = None
        try:
            high_val = float(high_raw.replace(",", "")) if high_raw else None
        except Exception:
            high_val = None
        annual_low = int(round(low_val * HOURLY_TO_ANNUAL_MULTIPLIER)) if low_val is not None else None
        annual_high = int(round(high_val * HOURLY_TO_ANNUAL_MULTIPLIER)) if high_val is not None else None
        _record_comp_range(annual_low, annual_high, prefer_high=True)
    comp_val = max(comp_candidates, default=None)
    if comp_val is not None:
        hints["compensation"] = comp_val
    if comp_ranges:
        ranged = [pair for pair in comp_ranges if pair[0] is not None and pair[1] is not None]
        if ranged:
            best_low, best_high = max(ranged, key=lambda pair: (pair[1] or 0))
        else:
            best_low, best_high = max(comp_ranges, key=lambda pair: ((pair[1] or pair[0] or 0)))
        range_payload = {k: v for k, v in (("low", best_low), ("high", best_high)) if v is not None}
        if range_payload:
            hints["compensation_range"] = range_payload

    if isinstance(seed_hints, dict):
        for key, value in seed_hints.items():
            if value in (None, "", [], {}):
                continue
            if key == "locations":
                if not hints.get("locations") and isinstance(value, list):
                    hints["locations"] = value
                    if "location" not in hints and value:
                        hints["location"] = value[0]
                continue
            if key == "location":
                if not hints.get("location") and isinstance(value, str):
                    hints["location"] = value
                continue
            if key == "company":
                if not hints.get("company") and isinstance(value, str):
                    hints["company"] = value
                continue
            if key == "title":
                if not hints.get("title") and isinstance(value, str):
                    hints["title"] = value
                continue
            if key == "remote":
                if "remote" not in hints and isinstance(value, bool):
                    hints["remote"] = value

    return hints


def _extract_job_detail_seed_from_json(markdown: str) -> tuple[Optional[str], Dict[str, Any]]:
    if not isinstance(markdown, str):
        return None, {}
    raw_text = markdown.strip()
    if not raw_text:
        return None, {}

    # Strip code fence markers if present (e.g., ```json...``` or ```...```)
    if raw_text.startswith("```"):
        lines = raw_text.split("\n")
        # Find start of content (skip opening fence line)
        start_idx = 1 if len(lines) > 1 else 0
        # Find closing fence
        end_idx = len(lines)
        for i in range(len(lines) - 1, 0, -1):
            if lines[i].strip() == "```":
                end_idx = i
                break
        raw_text = "\n".join(lines[start_idx:end_idx]).strip()

    # Handle raw HTML with JSON in <pre> tags (Workday API responses)
    if "<pre>{" in raw_text and "}</pre>" in raw_text:
        pre_match = re.search(r"<pre>({.+})</pre>", raw_text, flags=re.DOTALL)
        if pre_match:
            raw_text = pre_match.group(1)

    def _escape_control_chars_in_strings(value: str) -> str:
        output: List[str] = []
        in_string = False
        escaped = False
        for char in value:
            if escaped:
                output.append(char)
                escaped = False
                continue
            if char == "\\":
                output.append(char)
                escaped = True
                continue
            if char == "\"":
                output.append(char)
                in_string = not in_string
                continue
            if in_string and char in ("\n", "\r", "\t"):
                if char == "\n":
                    output.append("\\n")
                elif char == "\r":
                    output.append("\\r")
                else:
                    output.append("\\t")
                continue
            output.append(char)
        return "".join(output)

    def _try_parse_json_blob(text: str) -> Any | None:
        cleaned = text.strip()
        if not cleaned:
            return None
        if not cleaned.startswith("{"):
            match = re.search(JSON_OBJECT_PATTERN, cleaned, flags=re.DOTALL)
            if match:
                cleaned = match.group(0)
            else:
                return None
        cleaned = re.sub(INVALID_JSON_ESCAPE_PATTERN, "", cleaned)
        try:
            return json.loads(cleaned)
        except Exception:
            escaped = _escape_control_chars_in_strings(cleaned)
            if escaped != cleaned:
                try:
                    return json.loads(escaped)
                except Exception:
                    return None
        return None

    def _select_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        for key in ("data", "response", "result", "job", "position", "jobPostingInfo"):
            candidate = payload.get(key)
            if isinstance(candidate, dict):
                if any(k in candidate for k in ("jobDescription", "description", "name", "title")):
                    return candidate
        return payload

    def _first_string(payload: Dict[str, Any], keys: List[str]) -> Optional[str]:
        for key in keys:
            val = payload.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
        return None

    def _extract_location(payload: Dict[str, Any]) -> Optional[str]:
        for key in ("standardizedLocations", "locations"):
            values = payload.get(key)
            if isinstance(values, list):
                for item in values:
                    if isinstance(item, str) and item.strip():
                        return item.strip()
        location_val = payload.get("location")
        primary_location: Optional[str] = None
        if isinstance(location_val, str) and location_val.strip():
            primary_location = location_val.strip()
        # Handle location as dict with name key (Greenhouse API format)
        if isinstance(location_val, dict):
            name = location_val.get("name")
            if isinstance(name, str) and name.strip():
                primary_location = name.strip()
        # Handle Workday additionalLocations to combine with primary location
        additional_locations = payload.get("additionalLocations")
        if isinstance(additional_locations, list) and additional_locations:
            additional_strs = [loc for loc in additional_locations if isinstance(loc, str) and loc.strip()]
            if primary_location and additional_strs:
                return "; ".join([primary_location] + additional_strs)
            elif additional_strs:
                return "; ".join(additional_strs)
        return primary_location

    def _extract_remote(payload: Dict[str, Any]) -> Optional[bool]:
        for key in ("remote", "isRemote", "remoteAllowed"):
            val = payload.get(key)
            if isinstance(val, bool):
                return val
        for key in ("workLocationOption", "locationFlexibility", "workplaceType", "workLocationType"):
            val = payload.get(key)
            if isinstance(val, str) and val.strip():
                lowered = val.lower()
                if "remote" in lowered:
                    return True
                if "hybrid" in lowered:
                    return False
                if "on" in lowered or "site" in lowered or "office" in lowered:
                    return False
        return None

    if not any(token in raw_text for token in ("\"jobDescription\"", "\"positionUrl\"", "\"publicUrl\"")):
        if not raw_text.lstrip().startswith("{"):
            return None, {}

    parsed = _try_parse_json_blob(raw_text)
    if not isinstance(parsed, dict):
        return None, {}
    payload = _select_payload(parsed)

    raw_description = _first_string(payload, ["jobDescription", "description"])
    description: Optional[str] = None
    if raw_description:
        # Convert HTML description to readable text (handles HTML entities and tags)
        description = _html_description_to_text(raw_description.replace("\u00a0", " "))

    hints: Dict[str, Any] = {}
    title = _first_string(payload, ["name", "title", "jobTitle", "positionTitle"])
    if title:
        # Normalize title whitespace and decode HTML entities
        title = " ".join(title.split())
        hints["title"] = html_lib.unescape(title.replace("\u00a0", " ")).strip()
    company = _first_string(payload, ["company", "companyName", "employer", "brand"])
    if not company:
        url = _first_string(payload, ["publicUrl", "applyUrl", "jobUrl", "url"])
        if url:
            company = derive_company_from_url(url)
    if company:
        hints["company"] = company
    location = _extract_location(payload)
    if location:
        hints["locations"] = [location]
        hints["location"] = location
    remote = _extract_remote(payload)
    if remote is not None:
        hints["remote"] = remote

    return description, hints


def coerce_remote(value: Any, location: str, title: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.lower()
        if lowered in {"true", "yes", "remote", "hybrid", "fully remote"}:
            return True
    loc_lower = (location or "").lower()
    title_lower = (title or "").lower()
    if loc_lower and loc_lower not in {"unknown"} and "remote" not in loc_lower:
        return False
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


def extract_description(row: Dict[str, Any]) -> str:
    for key in ("job_description", "description", "desc", "body", "summary", "content"):
        val = row.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    try:
        return json.dumps(row, ensure_ascii=False)
    except Exception:
        return str(row)


def looks_like_truncated_description(
    description: str,
    *,
    min_chars: int = 300,
    min_words: int = 60,
) -> bool:
    if not isinstance(description, str):
        return False
    cleaned = description.strip()
    if not cleaned:
        return False
    if not cleaned.rstrip().endswith(("...", "…")):
        return False
    word_count = len(re.findall(r"\w+", cleaned))
    return len(cleaned) < min_chars or word_count < min_words


@dataclass(frozen=True)
class _HintApplicationConfig:
    apply_location_when_empty: bool
    apply_location_when_unknown: bool
    coerce_level_after_hint: bool
    override_comp_reason_on_hint: bool
    remote_company_location_when_empty: bool
    remote_company_location_when_unknown: bool
    title_prefix: str = "job application for"


@dataclass(frozen=True)
class _JobHintState:
    title: str
    location: str
    level: str
    remote: bool
    total_compensation: int
    compensation_unknown: bool
    compensation_reason: Optional[str]


class _JobHintApplier(ABC):
    @abstractmethod
    def apply(
        self,
        *,
        state: _JobHintState,
        hints: Dict[str, Any],
        company: str,
        config: _HintApplicationConfig,
    ) -> _JobHintState:
        raise NotImplementedError


class _DefaultJobHintApplier(_JobHintApplier):
    def apply(
        self,
        *,
        state: _JobHintState,
        hints: Dict[str, Any],
        company: str,
        config: _HintApplicationConfig,
    ) -> _JobHintState:
        title = state.title
        hinted_title = hints.get("title")
        if hinted_title:
            title_lowered = title.lower()
            hinted_lower = hinted_title.lower()
            if title_lowered.startswith(config.title_prefix) or (
                hinted_lower in title_lowered and ("|" in title or " in " in title_lowered)
            ):
                title = hinted_title

        location = state.location or ""
        hinted_location = hints.get("location")
        if hinted_location:
            if (config.apply_location_when_empty and not location) or (
                config.apply_location_when_unknown and location == "Unknown"
            ):
                location = hinted_location

        level = state.level
        hinted_level = hints.get("level")
        if hinted_level:
            level = hinted_level
        if config.coerce_level_after_hint:
            level = coerce_level(level, title)

        total_comp = state.total_compensation
        compensation_unknown = state.compensation_unknown
        reason = state.compensation_reason
        hinted_comp = hints.get("compensation")
        hinted_comp_norm = normalize_compensation_value(hinted_comp) if hinted_comp is not None else None
        if hinted_comp_norm is not None and (not total_comp or total_comp <= 0):
            total_comp = hinted_comp_norm
            compensation_unknown = False
            reason = "parsed from description"

        remote = state.remote
        hinted_remote = hints.get("remote")
        if hinted_remote is True:
            remote = True
        elif hinted_remote is False:
            remote = False

        if is_remote_company(company):
            remote = True
            if (config.remote_company_location_when_empty and not location) or (
                config.remote_company_location_when_unknown and location == "Unknown"
            ):
                location = "Remote"

        if hinted_comp_norm is not None and total_comp > 0 and config.override_comp_reason_on_hint:
            reason = "parsed from description"

        return _JobHintState(
            title=title,
            location=location,
            level=level,
            remote=remote,
            total_compensation=int(total_comp or 0),
            compensation_unknown=compensation_unknown,
            compensation_reason=reason,
        )


_NORMALIZED_HINT_CONFIG = _HintApplicationConfig(
    apply_location_when_empty=False,
    apply_location_when_unknown=True,
    coerce_level_after_hint=False,
    override_comp_reason_on_hint=True,
    remote_company_location_when_empty=True,
    remote_company_location_when_unknown=True,
)

_JOB_HINT_CONFIG = _HintApplicationConfig(
    apply_location_when_empty=True,
    apply_location_when_unknown=False,
    coerce_level_after_hint=True,
    override_comp_reason_on_hint=False,
    remote_company_location_when_empty=True,
    remote_company_location_when_unknown=False,
)


@dataclass(frozen=True)
class _JobBuildContext:
    default_posted_at: int
    scraped_at: Optional[int] = None
    scraped_with: Optional[str] = None
    workflow_name: Optional[str] = None
    scraped_cost_milli_cents: Optional[int] = None


class _JobRowNormalizer:
    def __init__(
        self,
        *,
        hint_applier: Optional[_JobHintApplier] = None,
        normalized_hint_config: _HintApplicationConfig = _NORMALIZED_HINT_CONFIG,
        job_hint_config: _HintApplicationConfig = _JOB_HINT_CONFIG,
        max_description_chars: int = MAX_JOB_DESCRIPTION_CHARS,
    ) -> None:
        self.hint_applier = hint_applier or _DefaultJobHintApplier()
        self.normalized_hint_config = normalized_hint_config
        self.job_hint_config = job_hint_config
        self.max_description_chars = max_description_chars

    def normalize_row(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        raw_title_value = row.get("job_title") or row.get("title")
        raw_title = stringify(raw_title_value) if raw_title_value is not None else ""
        title = normalize_title_from_bar(raw_title) if raw_title else ""
        if not title:
            title = stringify(row.get("job_title") or row.get("title") or "Untitled")

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

        hints = parse_markdown_hints(description)
        company = apply_company_hint(company, hints)
        compensation_result = parse_compensation(
            row.get("total_compensation") or row.get("salary") or row.get("compensation"),
            with_meta=True,
        )
        if isinstance(compensation_result, tuple):
            total_comp, used_default_comp = compensation_result
        else:
            total_comp, used_default_comp = compensation_result, True
        raw_reason = row.get("compensation_reason") or row.get("compensationReason")
        reason = raw_reason.strip() if isinstance(raw_reason, str) and raw_reason.strip() else None

        state = _JobHintState(
            title=title,
            location=location,
            level=level,
            remote=remote,
            total_compensation=int(total_comp or 0),
            compensation_unknown=bool(used_default_comp),
            compensation_reason=reason,
        )
        state = self.hint_applier.apply(
            state=state,
            hints=hints,
            company=company,
            config=self.normalized_hint_config,
        )

        raw_posted_value = row.get("posted_at") or row.get("postedAt") or row.get("date") or row.get("_timestamp")
        posted_at_unknown = row.get("posted_at_unknown") if isinstance(row, dict) else None
        if posted_at_unknown is None:
            posted_at_unknown = row.get("postedAtUnknown") if isinstance(row, dict) else None
        if isinstance(posted_at_unknown, bool):
            posted_at = parse_posted_at(raw_posted_value)
        else:
            posted_at, posted_at_unknown = parse_posted_at_with_unknown(raw_posted_value)
        normalized_row: Dict[str, Any] = {
            "job_title": state.title,
            "title": state.title,
            "company": company,
            "location": state.location,
            "remote": state.remote,
            "level": state.level,
            "total_compensation": state.total_compensation,
            "url": url,
            "description": description,
            "posted_at": posted_at,
            "posted_at_unknown": bool(posted_at_unknown),
        }
        if state.compensation_reason:
            normalized_row["compensation_reason"] = state.compensation_reason
        normalized_row["compensation_unknown"] = state.compensation_unknown

        return normalized_row

    def build_job_from_normalized(
        self,
        row: Dict[str, Any],
        *,
        context: _JobBuildContext,
    ) -> Optional[Dict[str, Any]]:
        description = stringify(row.get("description") or "")
        hints = parse_markdown_hints(description)
        compensation_unknown = bool(row.get("compensation_unknown"))
        raw_reason = row.get("compensation_reason") or row.get("compensationReason")
        reason = raw_reason.strip() if isinstance(raw_reason, str) and raw_reason.strip() else None
        if not compensation_unknown:
            total_comp_value = row.get("total_compensation")
            if not isinstance(total_comp_value, (int, float)) or total_comp_value <= 0:
                compensation_unknown = True

        title_val = row.get("title") or row.get("job_title") or "Untitled"
        location_val = row.get("location") or ""
        level_val = row.get("level") or "mid"
        total_comp_val = row.get("total_compensation") or 0
        company_val = row.get("company") or "Unknown"
        remote_val = bool(row.get("remote"))

        state = _JobHintState(
            title=str(title_val),
            location=str(location_val),
            level=str(level_val),
            remote=remote_val,
            total_compensation=int(total_comp_val or 0),
            compensation_unknown=compensation_unknown,
            compensation_reason=reason,
        )
        state = self.hint_applier.apply(
            state=state,
            hints=hints,
            company=company_val,
            config=self.job_hint_config,
        )

        preferred_url = prefer_apply_url(row)
        apply_url = stringify(preferred_url) if preferred_url is not None else ""
        url = apply_url or row.get("url") or ""
        if not url:
            return None

        posted_at_unknown = row.get("posted_at_unknown") if isinstance(row, dict) else None
        if posted_at_unknown is None:
            posted_at_unknown = row.get("postedAtUnknown") if isinstance(row, dict) else None
        job = {
            "title": state.title,
            "company": company_val,
            "description": description,
            "location": state.location,
            "remote": state.remote,
            "level": state.level,
            "totalCompensation": int(state.total_compensation or 0),
            "url": url,
            "postedAt": int(row.get("posted_at") or context.default_posted_at),
            "postedAtUnknown": bool(posted_at_unknown),
        }
        if context.scraped_at:
            job["scrapedAt"] = context.scraped_at
        if context.scraped_with:
            job["scrapedWith"] = context.scraped_with
        if context.workflow_name:
            job["workflowName"] = context.workflow_name
        if context.scraped_cost_milli_cents is not None:
            job["scrapedCostMilliCents"] = context.scraped_cost_milli_cents
        if state.compensation_unknown:
            job["compensationUnknown"] = True
        if state.compensation_reason:
            job["compensationReason"] = state.compensation_reason
        elif state.compensation_unknown:
            job["compensationReason"] = UNKNOWN_COMPENSATION_REASON
        elif context.scraped_with:
            job["compensationReason"] = f"{context.scraped_with} extracted compensation"
        else:
            job["compensationReason"] = "compensation provided in scrape payload"

        return job


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
    collect_page_links: bool = True,
) -> Dict[str, Any]:
    items = scrape.get("items", {})
    normalized_urls: list[Dict[str, Any]] = []
    normalized_samples: list[Dict[str, Any]] = []
    normalized_count = 0
    ignored_items: list[Any] = []
    failed_items: list[Any] = []
    page_links: list[str] = []
    sample_limit = min(10, max_items)
    sample_string_limit = 400
    normalized_limit = max(max_items, 400)
    seen_urls: set[str] = set()

    def _extract_url(value: Any) -> str | None:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if not isinstance(value, dict):
            return None
        for key in (
            "url",
            "job_url",
            "jobUrl",
            "absolute_url",
            "absoluteUrl",
            "apply_url",
            "applyUrl",
            "link",
            "href",
            "_url",
            "_rawUrl",
        ):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        return None

    def _looks_like_apply_or_auth_url(url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        segments = [seg for seg in (parsed.path or "").lower().split("/") if seg]
        apply_segments = {"apply", "application", "hvhapply"}
        auth_segments = {
            "login",
            "signin",
            "sign-in",
            "sign_in",
            "logout",
            "signout",
            "sign-out",
            "sign_out",
            "register",
            "signup",
            "sign-up",
        }
        return any(seg in apply_segments or seg in auth_segments for seg in segments)

    def _looks_like_job_detail_url(url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        query = (parsed.query or "").lower()
        if "gh_jid=" in query:
            return True
        host = (parsed.hostname or "").lower()
        path = (parsed.path or "").lower()
        if host.endswith("ashbyhq.com"):
            segments = [seg for seg in path.split("/") if seg]
            return len(segments) >= 2
        if not any(token in path for token in ("/job", "/jobs", "/career", "/careers", "/position", "/positions")):
            return False
        segments = [seg for seg in path.split("/") if seg]
        for idx, seg in enumerate(segments):
            if seg in {"job", "jobs", "career", "careers", "position", "positions"}:
                return idx + 1 < len(segments)
        return False

    def _looks_like_listing_url(url: str) -> bool:
        if _url_is_listing_root(url):
            return True
        if _url_suggests_listing(url):
            return True
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        path = (parsed.path or "").lower()
        if path.endswith("/jobs") or path.endswith("/jobs/"):
            return True
        if "page=" in (parsed.query or "") and "/jobs" in path and "/jobs/job/" not in path:
            return True
        return False

    def _should_keep_normalized_url(url: str) -> bool:
        if _looks_like_apply_or_auth_url(url):
            return False
        return _looks_like_job_detail_url(url) or _looks_like_listing_url(url)

    drop_sample_keys = {
        "_raw",
        "raw",
        "raw_html",
        "html",
        "markdown",
        "commonmark",
        "content",
        "page_content",
        "pageContent",
        "full_text",
        "fullText",
    }

    def _trim_sample_row(row: Dict[str, Any]) -> Dict[str, Any]:
        new_row = dict(row)
        new_row.pop("_raw", None)
        desc = stringify(new_row.get("description", ""))
        if desc:
            new_row["description"] = desc
        job_desc = stringify(
            new_row.get("job_description")
            or new_row.get("jobDescription")
            or ""
        )
        if job_desc:
            new_row["job_description"] = job_desc
        for title_key in ("title", "job_title", "jobTitle"):
            title_val = new_row.get(title_key)
            if isinstance(title_val, str) and len(title_val) > max_title_chars:
                new_row[title_key] = title_val[:max_title_chars]

        for key in list(new_row.keys()):
            if key in drop_sample_keys:
                new_row.pop(key, None)
                continue
            value = new_row.get(key)
            if key in {"description", "job_description", "jobDescription"}:
                continue
            if isinstance(value, str) and len(value) > sample_string_limit:
                new_row[key] = value[:sample_string_limit]
            elif isinstance(value, (dict, list)):
                new_row[key] = _shrink_payload(value, sample_string_limit)

        return new_row

    if isinstance(items, dict):
        raw_normalized = items.get("normalized", [])
        if isinstance(raw_normalized, list):
            normalized_count = len(raw_normalized)
            truncated = normalized_count > max_items
            for row in raw_normalized:
                url_val = _extract_url(row)
                if url_val and url_val not in seen_urls:
                    seen_urls.add(url_val)
                    if len(normalized_urls) < normalized_limit:
                        normalized_urls.append({"url": url_val})
                if len(normalized_samples) < sample_limit and isinstance(row, dict):
                    normalized_samples.append(_trim_sample_row(row))
        else:
            truncated = False
        raw_ignored = items.get("ignored")
        if isinstance(raw_ignored, list):
            ignored_items = raw_ignored
        raw_failed = items.get("failed")
        if isinstance(raw_failed, list):
            failed_items = raw_failed
    else:
        truncated = False

    raw_preview = None
    if isinstance(items, dict) and "raw" in items and raw_preview_chars > 0:
        try:
            raw_str = json.dumps(items["raw"], ensure_ascii=False)
            raw_preview = raw_str[:raw_preview_chars]
        except Exception:
            raw_preview = None

    trimmed_items: Dict[str, Any] = {"normalized": normalized_urls}
    if normalized_count:
        trimmed_items["normalizedCount"] = normalized_count
    if normalized_samples:
        trimmed_items["normalizedSample"] = normalized_samples
    if collect_page_links and isinstance(items, dict) and "raw" in items:
        try:
            page_links = extract_links_from_payload(
                items.get("raw"),
                collect_all=True,
                scan_strings=True,
            )
        except Exception:
            page_links = []
        if page_links:
            trimmed_items["page_links"] = dedupe_str_list(page_links)

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
        for key in (
            "provider",
            "crawlProvider",
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
            "requestedFormat",
            "seedUrls",
        ):
            if key in items:
                _copy_meta(key, items.get(key))
        raw_job_urls = items.get("job_urls") if "job_urls" in items else items.get("jobUrls")
        if isinstance(raw_job_urls, list):
            deduped_job_urls = dedupe_str_list(raw_job_urls)
            if deduped_job_urls:
                trimmed_items["job_urls"] = deduped_job_urls
        aux_limit = min(max_items, 50)
        if ignored_items:
            trimmed_items["ignored"] = ignored_items[:aux_limit]
            trimmed_items["ignoredCount"] = len(ignored_items)
        elif isinstance(items.get("ignoredCount"), int):
            trimmed_items["ignoredCount"] = items.get("ignoredCount")
        if failed_items:
            trimmed_items["failed"] = failed_items[:aux_limit]
            trimmed_items["failedCount"] = len(failed_items)
        elif isinstance(items.get("failedCount"), int):
            trimmed_items["failedCount"] = items.get("failedCount")

    if page_links:
        existing_job_urls = trimmed_items.get("job_urls") if isinstance(trimmed_items, dict) else None
        if not isinstance(existing_job_urls, list) or not existing_job_urls:
            trimmed_items["job_urls"] = dedupe_str_list(page_links)

    if raw_preview:
        if truncated:
            trimmed_items["rawPreview"] = raw_preview
        else:
            trimmed_items["raw"] = raw_preview

    trimmed: Dict[str, Any] = {k: v for k, v in scrape.items() if k not in {"items", "response", "asyncResponse"}}
    trimmed["items"] = trimmed_items

    if request_max_chars > 0:
        if provider_request is not None:
            trimmed["providerRequest"] = _shrink_payload(provider_request, request_max_chars)
        if request_payload is not None:
            trimmed_request = _trim_request_snapshot(request_payload, request_max_chars)
            trimmed["request"] = trimmed_request
            # Mirror request into items for downstream expectations (tests/UI)
            if isinstance(trimmed_items, dict) and trimmed_request is not None:
                trimmed_items["request"] = trimmed_request
    if raw_preview_chars > 0:
        if scrape.get("response") is not None:
            trimmed["response"] = _shrink_payload(scrape.get("response"), raw_preview_chars)
        if scrape.get("asyncResponse") is not None:
            trimmed["asyncResponse"] = _shrink_payload(scrape.get("asyncResponse"), raw_preview_chars)

    return trimmed


class _PayloadRowCollector(ABC):
    @abstractmethod
    def collect_rows(self, payload: Any) -> List[Dict[str, Any]]:
        raise NotImplementedError


class _FirecrawlRowCollector(_PayloadRowCollector):
    def collect_rows(self, payload: Any) -> List[Dict[str, Any]]:
        parsed = _parse_firecrawl_json(payload)
        if parsed is None:
            parsed = payload
        return _rows_from_firecrawl_payload(parsed)


class _FetchfoxRowCollector(_PayloadRowCollector):
    def collect_rows(self, payload: Any) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if isinstance(payload, list):
            rows.extend([r for r in payload if isinstance(r, dict)])
            return rows

        if isinstance(payload, dict):
            if isinstance(payload.get("normalized"), list):
                rows.extend([r for r in payload["normalized"] if isinstance(r, dict)])
            if isinstance(payload.get("items"), list):
                rows.extend([r for r in payload["items"] if isinstance(r, dict)])
            if isinstance(payload.get("results"), list):
                rows.extend([r for r in payload["results"] if isinstance(r, dict)])
            results_obj = payload.get("results")
            if isinstance(results_obj, dict):
                if isinstance(results_obj.get("items"), list):
                    rows.extend([r for r in results_obj["items"] if isinstance(r, dict)])
                if isinstance(results_obj.get("normalized"), list):
                    rows.extend([r for r in results_obj["normalized"] if isinstance(r, dict)])
            if isinstance(payload.get("data"), dict):
                rows.extend(self.collect_rows(payload.get("data")))

        return rows


_DEFAULT_JOB_NORMALIZER = _JobRowNormalizer()
_FIRECRAWL_COLLECTOR = _FirecrawlRowCollector()
_FETCHFOX_COLLECTOR = _FetchfoxRowCollector()


def _normalize_payload_items(
    payload: Any,
    *,
    collector: _PayloadRowCollector,
    normalizer: _JobRowNormalizer = _DEFAULT_JOB_NORMALIZER,
) -> List[Dict[str, Any]]:
    rows = collector.collect_rows(payload)
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        norm = normalizer.normalize_row(row)
        if norm:
            normalized.append(norm)
    return normalized


def normalize_firecrawl_items(payload: Any) -> List[Dict[str, Any]]:
    return _normalize_payload_items(payload, collector=_FIRECRAWL_COLLECTOR, normalizer=_DEFAULT_JOB_NORMALIZER)


def normalize_fetchfox_items(payload: Any) -> List[Dict[str, Any]]:
    return _normalize_payload_items(payload, collector=_FETCHFOX_COLLECTOR, normalizer=_DEFAULT_JOB_NORMALIZER)


def normalize_single_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return _DEFAULT_JOB_NORMALIZER.normalize_row(row)


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
    normalized_sample = None
    if isinstance(items, dict):
        normalized = items.get("normalized")
        normalized_sample = items.get("normalizedSample")
    if not isinstance(normalized, list):
        return jobs

    def _row_has_details(row: Any) -> bool:
        if not isinstance(row, dict):
            return False
        detail_keys = (
            "title",
            "job_title",
            "jobTitle",
            "company",
            "description",
            "job_description",
            "jobDescription",
            "location",
            "remote",
            "level",
        )
        return any(row.get(key) for key in detail_keys)

    if isinstance(normalized_sample, list) and normalized_sample:
        has_details = any(_row_has_details(row) for row in normalized)
        sample_has_details = any(_row_has_details(row) for row in normalized_sample)
        if sample_has_details and (not normalized or not has_details):
            normalized = normalized_sample

    context = _JobBuildContext(
        default_posted_at=default_posted_at,
        scraped_at=scraped_at,
        scraped_with=scraped_with,
        workflow_name=workflow_name,
        scraped_cost_milli_cents=scraped_cost_milli_cents,
    )
    for row in normalized:
        if not isinstance(row, dict):
            continue
        job = _DEFAULT_JOB_NORMALIZER.build_job_from_normalized(row, context=context)
        if job:
            jobs.append(job)

    return jobs


__all__ = [
    "DEFAULT_TOTAL_COMPENSATION",
    "MIN_TOTAL_COMPENSATION",
    "MAX_TOTAL_COMPENSATION",
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
    "looks_like_truncated_description",
    "fetch_seen_urls_for_site",
    "looks_like_job_listing_page",
    "normalize_fetchfox_items",
    "normalize_firecrawl_items",
    "normalize_compensation_value",
    "normalize_single_row",
    "parse_compensation",
    "parse_posted_at",
    "parse_posted_at_with_unknown",
    "prefer_apply_url",
    "split_description_metadata",
    "stringify",
    "trim_scrape_for_convex",
    "_jobs_from_scrape_items",
    "_shrink_payload",
    "_trim_request_snapshot",
]
