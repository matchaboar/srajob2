from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, TypedDict, NotRequired
from urllib.parse import urlparse

from firecrawl import Firecrawl
from firecrawl.v2.types import ScrapeOptions
from fetchfox_sdk import FetchFox
from pydantic import BaseModel, ConfigDict, Field
from temporalio import activity
from temporalio.exceptions import ApplicationError

from .config import settings
from .models import (
    FetchFoxPriority,
    FetchFoxScrapeRequest,
    GreenhouseBoardResponse,
    MAX_FETCHFOX_VISITS,
    extract_greenhouse_job_urls,
    load_greenhouse_board,
)


DEFAULT_TOTAL_COMPENSATION = 151000
MAX_DESCRIPTION_CHARS = 1200  # keep payloads small enough for Convex document limits
MAX_FIRECRAWL_VISITS = MAX_FETCHFOX_VISITS
FIRECRAWL_CACHE_MAX_AGE_MS = 600_000
JOB_TEMPLATE: Dict[str, str] = {
    # Keep simple strings so FetchFox LLM extractor can infer fields from detail pages
    "job_title": "str | None",
    "company": "str | None",
    "description": "str | None",
    "url": "str | None",
    "location": "str | None",
    "remote": "True | False | None",
    "level": (
        "junior | mid | senior | staff | lead | principal | director | manager | vp | cxo | intern | None"
    ),
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
    total_compensation: Optional[str | float | int] = Field(
        default=None, alias="total_compensation"
    )
    posted_at: Optional[str] = Field(default=None, alias="posted_at")

    model_config = ConfigDict(populate_by_name=True, extra="allow")


def build_job_template() -> Dict[str, str]:
    """Return a fresh FetchFox template for job detail extraction."""

    return dict(JOB_TEMPLATE)


def build_firecrawl_schema() -> Dict[str, Any]:
    """JSON schema used to ask Firecrawl for structured job fields."""

    return FirecrawlJobSchema.model_json_schema()


class Site(TypedDict):
    """Site record stored in Convex.

    `_id` and `url` are always present, the rest are optional flags/metadata returned
    by the API. Using ``NotRequired`` keeps type safety while matching runtime
    payloads.
    """

    _id: str
    url: str
    name: NotRequired[Optional[str]]
    type: NotRequired[Optional[str]]
    pattern: NotRequired[Optional[str]]
    enabled: NotRequired[bool]
    lastRunAt: NotRequired[Optional[int]]
    lockedBy: NotRequired[Optional[str]]
    lockExpiresAt: NotRequired[Optional[int]]
    completed: NotRequired[Optional[bool]]


async def fetch_seen_urls_for_site(source_url: str, pattern: Optional[str]) -> List[str]:
    """Return every URL we've already scraped for the site so scrapers can skip them."""

    from .convex_client import convex_query

    try:
        res = await convex_query(
            "router:listSeenJobUrlsForSite",
            {"sourceUrl": source_url, "pattern": pattern},
        )
    except Exception:
        return []

    urls = res.get("urls", []) if isinstance(res, dict) else []
    return [u for u in urls if isinstance(u, str)]


def extract_raw_body_from_fetchfox_result(result: Any) -> str:
    """Best-effort extraction of the primary body/html from a FetchFox scrape result."""

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


@activity.defn
async def fetch_sites() -> List[Site]:
    from .convex_client import convex_query

    res = await convex_query("router:listSites", {"enabledOnly": True})
    if not isinstance(res, list):
        raise RuntimeError(f"Unexpected sites payload: {res!r}")
    return res  # type: ignore[return-value]


@activity.defn
async def lease_site(worker_id: str, lock_seconds: int = 300, site_type: Optional[str] = None) -> Optional[Site]:
    from .convex_client import convex_mutation

    payload: Dict[str, Any] = {"workerId": worker_id, "lockSeconds": lock_seconds}
    if site_type:
        payload["siteType"] = site_type

    res = await convex_mutation("router:leaseSite", payload)
    if res is None:
        return None
    if not isinstance(res, dict):
        raise RuntimeError(f"Unexpected lease payload: {res!r}")
    return res  # type: ignore[return-value]


@activity.defn
async def scrape_site(site: Site) -> Dict[str, Any]:
    """Scrape a site, preferring Firecrawl when configured."""

    if settings.firecrawl_api_key:
        skip_urls = await fetch_seen_urls_for_site(site["url"], site.get("pattern"))
        return await scrape_site_firecrawl(site, skip_urls)
    if settings.fetchfox_api_key:
        return await scrape_site_fetchfox(site)

    raise ApplicationError(
        "FIRECRAWL_API_KEY or FETCHFOX_API_KEY env var is required",
        non_retryable=True,
    )




@activity.defn
async def scrape_site_fetchfox(site: Site) -> Dict[str, Any]:
    if not settings.fetchfox_api_key:
        # Mark as non-retryable to avoid endless attempts for a known config issue
        raise ApplicationError(
            "FETCHFOX_API_KEY env var is required for FetchFox", non_retryable=True
        )

    pattern = site.get("pattern")
    skip_urls = await fetch_seen_urls_for_site(site["url"], pattern)
    start_urls = [site["url"]]
    template = build_job_template()

    request = FetchFoxScrapeRequest(
        pattern=pattern,
        start_urls=start_urls,
        max_depth=5,
        max_visits=MAX_FETCHFOX_VISITS,
        template=template,
        priority=FetchFoxPriority(skip=skip_urls),
        content_transform="text_only",
    ).model_dump(exclude_none=True)

    # Run blocking FetchFox init and scrape in a thread
    def _do_scrape(scrape_payload: Dict[str, Any]):
        fox = FetchFox(api_key=settings.fetchfox_api_key)
        return fox.scrape(scrape_payload)

    started_at = int(time.time() * 1000)

    # FetchFox may return dict or JSON string depending on version
    try:
        result = await asyncio.to_thread(_do_scrape, request)
        result_obj: Dict[str, Any] = result if isinstance(result, dict) else json.loads(result)
    except Exception:
        # Last resort: wrap opaque content
        result_obj = {"raw": "Scrape failed or returned invalid data"}

    normalized_items = normalize_fetchfox_items(result_obj)

    completed_at = int(time.time() * 1000)

    scrape_payload = {
        "sourceUrl": site["url"],
        "pattern": pattern,
        "startedAt": started_at,
        "completedAt": completed_at,
        "items": {"normalized": normalized_items, "raw": result_obj},
    }

    # Trim heavy fields before sending to Convex
    return trim_scrape_for_convex(scrape_payload)


@activity.defn
async def scrape_site_firecrawl(site: Site, skip_urls: Optional[List[str]] = None) -> Dict[str, Any]:
    if not settings.firecrawl_api_key:
        raise ApplicationError(
            "FIRECRAWL_API_KEY env var is required for Firecrawl", non_retryable=True
        )

    pattern = site.get("pattern")
    skip_urls = skip_urls or []
    job_schema = build_firecrawl_schema()

    scrape_options = ScrapeOptions(
        formats=[
            "markdown",
            {"type": "json", "schema": job_schema},
        ],
        only_main_content=True,
        proxy="auto",
        max_age=FIRECRAWL_CACHE_MAX_AGE_MS,
        store_in_cache=True,
    )

    def _do_crawl() -> Any:
        client = Firecrawl(api_key=settings.firecrawl_api_key)
        return client.crawl(
            site["url"],
            include_paths=[pattern] if pattern else None,
            exclude_paths=skip_urls if skip_urls else None,
            max_discovery_depth=5,
            ignore_sitemap=True,
            limit=MAX_FIRECRAWL_VISITS,
            crawl_entire_domain=False,
            allow_subdomains=True,
            allow_external_links=False,
            scrape_options=scrape_options,
            timeout=240,
        )

    started_at = int(time.time() * 1000)
    crawl_job: Any
    try:
        crawl_job = await asyncio.to_thread(_do_crawl)
    except Exception as exc:  # noqa: BLE001
        raise ApplicationError(f"Firecrawl scrape failed: {exc}") from exc

    normalized_items = normalize_firecrawl_items(crawl_job)
    completed_at = int(time.time() * 1000)

    raw_payload = (
        crawl_job.model_dump(mode="json", exclude_none=True)
        if hasattr(crawl_job, "model_dump")
        else crawl_job
    )

    scrape_payload = {
        "sourceUrl": site["url"],
        "pattern": pattern,
        "startedAt": started_at,
        "completedAt": completed_at,
        "items": {
            "normalized": normalized_items,
            "raw": raw_payload,
            "provider": "firecrawl",
        },
    }

    return trim_scrape_for_convex(scrape_payload)


@activity.defn
async def fetch_greenhouse_listing(site: Site) -> Dict[str, Any]:
    """Fetch a Greenhouse board JSON payload using FetchFox and parse job URLs."""

    if settings.firecrawl_api_key:
        return await fetch_greenhouse_listing_firecrawl(site)
    if not settings.fetchfox_api_key:
        raise ApplicationError(
            "FIRECRAWL_API_KEY or FETCHFOX_API_KEY env var is required",
            non_retryable=True,
        )

    request = FetchFoxScrapeRequest(
        start_urls=[site["url"]],
        max_depth=0,
        max_visits=1,
        template={"raw": "str | None"},
        priority=FetchFoxPriority(skip=[]),
        content_transform="full_html",
    ).model_dump(exclude_none=True)

    def _do_scrape(scrape_payload: Dict[str, Any]):
        fox = FetchFox(api_key=settings.fetchfox_api_key)
        return fox.scrape(scrape_payload)

    started_at = int(time.time() * 1000)

    try:
        result = await asyncio.to_thread(_do_scrape, request)
        result_obj: Dict[str, Any] = result if isinstance(result, dict) else json.loads(result)
    except Exception as exc:  # noqa: BLE001
        raise ApplicationError(f"Failed to fetch Greenhouse board: {exc}") from exc

    raw_text = extract_raw_body_from_fetchfox_result(result_obj)

    try:
        board: GreenhouseBoardResponse = load_greenhouse_board(raw_text or result_obj)
        job_urls = extract_greenhouse_job_urls(board)
    except Exception as exc:  # noqa: BLE001
        raise ApplicationError(f"Unable to parse Greenhouse board payload: {exc}") from exc

    completed_at = int(time.time() * 1000)

    return {
        "raw": raw_text,
        "job_urls": job_urls,
        "startedAt": started_at,
        "completedAt": completed_at,
    }


@activity.defn
async def fetch_greenhouse_listing_firecrawl(site: Site) -> Dict[str, Any]:
    """Fetch a Greenhouse board JSON payload using Firecrawl and parse job URLs."""

    def _do_scrape() -> Any:
        client = Firecrawl(api_key=settings.firecrawl_api_key)
        return client.scrape(site["url"], formats=["json"])

    started_at = int(time.time() * 1000)
    doc = await asyncio.to_thread(_do_scrape)
    raw_json = getattr(doc, "json", None)
    raw_text = raw_json if isinstance(raw_json, str) else json.dumps(raw_json or {}, ensure_ascii=False)

    try:
        board: GreenhouseBoardResponse = load_greenhouse_board(raw_text or doc)
        job_urls = extract_greenhouse_job_urls(board)
    except Exception as exc:  # noqa: BLE001
        raise ApplicationError(f"Unable to parse Greenhouse board payload (Firecrawl): {exc}") from exc

    return {
        "raw": raw_text,
        "job_urls": job_urls,
        "startedAt": started_at,
        "completedAt": int(time.time() * 1000),
    }


@activity.defn
async def filter_existing_job_urls(urls: List[str]) -> List[str]:
    """Return the subset of URLs that already exist in Convex jobs table."""

    if not urls:
        return []
    from .convex_client import convex_query

    try:
        data = await convex_query("router:findExistingJobUrls", {"urls": urls})
    except Exception:
        return []

    existing = data.get("existing", []) if isinstance(data, dict) else []
    if not isinstance(existing, list):
        return []

    return [u for u in existing if isinstance(u, str)]


@activity.defn
async def scrape_greenhouse_jobs(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Scrape new Greenhouse job URLs with a single FetchFox request."""

    if settings.firecrawl_api_key:
        return await scrape_greenhouse_jobs_firecrawl(payload)

    if not settings.fetchfox_api_key:
        raise ApplicationError(
            "FETCHFOX_API_KEY env var is required for FetchFox",
            non_retryable=True,
        )

    urls: List[str] = [u for u in payload.get("urls", []) if isinstance(u, str) and u.strip()]
    source_url: str = payload.get("source_url") or (urls[0] if urls else "")

    if not urls:
        return {"scrape": None, "jobsScraped": 0}

    template = build_job_template()
    request = FetchFoxScrapeRequest(
        pattern=None,
        start_urls=urls,
        max_depth=1,
        max_visits=min(MAX_FETCHFOX_VISITS, max(len(urls), 1)),
        template=template,
        priority=FetchFoxPriority(skip=[]),
        content_transform="text_only",
    ).model_dump(exclude_none=True)

    def _do_scrape(scrape_payload: Dict[str, Any]):
        fox = FetchFox(api_key=settings.fetchfox_api_key)
        return fox.scrape(scrape_payload)

    started_at = int(time.time() * 1000)

    try:
        result = await asyncio.to_thread(_do_scrape, request)
        result_obj: Dict[str, Any] = result if isinstance(result, dict) else json.loads(result)
    except Exception as exc:  # noqa: BLE001
        raise ApplicationError(f"Greenhouse detail scrape failed: {exc}") from exc

    normalized_items = normalize_fetchfox_items(result_obj)
    completed_at = int(time.time() * 1000)

    scrape_payload = {
        "sourceUrl": source_url,
        "pattern": None,
        "startedAt": started_at,
        "completedAt": completed_at,
        "items": {"normalized": normalized_items, "raw": result_obj, "seedUrls": urls},
    }

    trimmed = trim_scrape_for_convex(scrape_payload)
    items = trimmed.get("items", {})
    if isinstance(items, dict):
        items.setdefault("seedUrls", urls)
        trimmed["items"] = items

    return {"scrape": trimmed, "jobsScraped": len(normalized_items)}


@activity.defn
async def scrape_greenhouse_jobs_firecrawl(payload: Dict[str, Any]) -> Dict[str, Any]:
    urls: List[str] = [u for u in payload.get("urls", []) if isinstance(u, str) and u.strip()]
    source_url: str = payload.get("source_url") or (urls[0] if urls else "")

    if not urls:
        return {"scrape": None, "jobsScraped": 0}

    schema = build_firecrawl_schema()
    scrape_options = ScrapeOptions(
        formats=[
            "markdown",
            {"type": "json", "schema": schema},
        ]
    )

    documents: List[Any] = []

    def _scrape_one(url: str) -> Any:
        client = Firecrawl(api_key=settings.firecrawl_api_key)
        return client.scrape(url, formats=scrape_options.formats)  # type: ignore[arg-type]

    for url in urls:
        doc = await asyncio.to_thread(_scrape_one, url)
        documents.append(doc)

    payload_raw = {
        "data": [
            doc.model_dump(mode="json", exclude_none=True) if hasattr(doc, "model_dump") else doc
            for doc in documents
        ]
    }
    normalized_items = normalize_firecrawl_items(payload_raw)
    completed_at = int(time.time() * 1000)

    scrape_payload = {
        "sourceUrl": source_url,
        "pattern": None,
        "startedAt": int(time.time() * 1000),
        "completedAt": completed_at,
        "items": {"normalized": normalized_items, "raw": payload_raw, "seedUrls": urls, "provider": "firecrawl"},
    }

    trimmed = trim_scrape_for_convex(scrape_payload)
    items = trimmed.get("items", {})
    if isinstance(items, dict):
        items.setdefault("seedUrls", urls)
        trimmed["items"] = items

    return {"scrape": trimmed, "jobsScraped": len(normalized_items)}


def normalize_firecrawl_items(payload: Any) -> List[Dict[str, Any]]:
    """Convert a Firecrawl crawl job into normalized job rows."""

    documents: List[Any] = []
    if hasattr(payload, "data"):
        documents = getattr(payload, "data") or []
    elif isinstance(payload, dict):
        documents = payload.get("data", []) or []

    normalized: List[Dict[str, Any]] = []
    for doc in documents:
        doc_dict: Dict[str, Any] | None = None
        if hasattr(doc, "model_dump"):
            doc_dict = doc.model_dump(mode="json", exclude_none=True)
        elif isinstance(doc, dict):
            doc_dict = doc
        if not doc_dict:
            continue

        json_payload = doc_dict.get("json") or doc_dict.get("data")
        parsed_payload = _parse_firecrawl_json(json_payload)
        for row in _rows_from_firecrawl_payload(parsed_payload):
            norm = normalize_single_row(row)
            if norm:
                normalized.append(norm)

    return normalized


def _jobs_from_scrape_items(items: Any, *, default_posted_at: int) -> List[Dict[str, Any]]:
    """Convert trimmed scrape items into Convex job ingest shape."""

    jobs: List[Dict[str, Any]] = []
    normalized = None
    if isinstance(items, dict):
        normalized = items.get("normalized")
    if not isinstance(normalized, list):
        return jobs

    for row in normalized:
        if not isinstance(row, dict):
            continue
        job = {
            "title": row.get("title") or row.get("job_title") or "Untitled",
            "company": row.get("company") or "Unknown",
            "description": row.get("description") or "",
            "location": row.get("location") or "",
            "remote": bool(row.get("remote")),
            "level": row.get("level") or "mid",
            "totalCompensation": int(row.get("total_compensation") or DEFAULT_TOTAL_COMPENSATION),
            "url": row.get("url") or "",
            "postedAt": int(row.get("posted_at") or default_posted_at),
        }
        if not job["url"]:
            continue
        jobs.append(job)

    return jobs


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
        items = payload.get("items")
        if isinstance(items, list):
            rows.extend([i for i in items if isinstance(i, dict)])
        else:
            rows.append(payload)
    return rows


def normalize_fetchfox_items(payload: Any) -> List[Dict[str, Any]]:
    """Convert a FetchFox scrape response into normalized job objects for Convex ingestion.

    We keep the raw payload alongside normalized rows so the UI can still render the raw JSON
    block while top-level fields stay clean.
    """

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
            # Some payloads nest under a "data" key
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
    title = stringify(row.get("job_title") or row.get("title") or "Untitled")
    url = stringify(row.get("url") or row.get("link") or row.get("href") or row.get("_url") or "")
    if not url:
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
    description = extract_description(row)
    if len(description) > MAX_DESCRIPTION_CHARS:
        description = description[:MAX_DESCRIPTION_CHARS]
    total_comp = parse_compensation(
        row.get("total_compensation") or row.get("salary") or row.get("compensation")
    )
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

    return normalized_row


def stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value.strip()
    return str(value)


def derive_company_from_url(url: str) -> str:
    try:
        hostname = urlparse(url).hostname or ""
    except Exception:
        return ""

    hostname = hostname.lower()
    # Strip common subdomains seen on career sites
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

    # Convert hyphenated hostnames to title case words
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
    if isinstance(value, str):
        normalized = value.lower()
    else:
        normalized = ""

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


def parse_compensation(value: Any) -> int:
    if isinstance(value, (int, float)) and value > 0:
        return int(value)
    if isinstance(value, str):
        numbers = re.findall(r"[0-9][0-9,\.]+", value.replace("\u00a0", " "))
        if numbers:
            try:
                # Choose the highest number to approximate total comp if range provided
                parsed = max(float(num.replace(",", "")) for num in numbers)
                if parsed > 0:
                    return int(parsed)
            except ValueError:
                pass
    return DEFAULT_TOTAL_COMPENSATION


def extract_description(row: Dict[str, Any]) -> str:
    for key in ("description", "job_description", "desc", "body", "summary"):
        val = row.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    # Fallback to raw JSON if nothing else is available
    try:
        return json.dumps(row, ensure_ascii=False)
    except Exception:
        return str(row)


def parse_posted_at(value: Any) -> int:
    """Return a UNIX epoch (ms). Defaults to current time if missing."""

    now_ms = int(time.time() * 1000)
    if value is None:
        return now_ms

    if isinstance(value, (int, float)):
        # Heuristic: numbers above 10^12 are already ms, otherwise treat as seconds
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


def trim_scrape_for_convex(
    scrape: Dict[str, Any],
    *,
    max_items: int = 400,
    max_description: int = MAX_DESCRIPTION_CHARS,
    raw_preview_chars: int = 8000,
) -> Dict[str, Any]:
    """
    Reduce scrape payload size so it fits Convex document limits.

    - Limits number of normalized rows
    - Truncates descriptions
    - Drops embedded _raw blobs
    - Keeps an optional raw preview string instead of full raw object
    """

    items = scrape.get("items", {})
    normalized: list[Dict[str, Any]] = []

    if isinstance(items, dict):
        raw_normalized = items.get("normalized", [])
        if isinstance(raw_normalized, list):
            for row in raw_normalized[: max_items]:
                if not isinstance(row, dict):
                    continue
                new_row = dict(row)
                # Strip heavy fields
                new_row.pop("_raw", None)
                desc = stringify(new_row.get("description", ""))
                if len(desc) > max_description:
                    new_row["description"] = desc[:max_description]
                normalized.append(new_row)

    raw_preview = None
    if isinstance(items, dict) and "raw" in items and raw_preview_chars > 0:
        try:
            raw_str = json.dumps(items["raw"], ensure_ascii=False)
            raw_preview = raw_str[:raw_preview_chars]
        except Exception:
            raw_preview = None

    trimmed_items: Dict[str, Any] = {"normalized": normalized}
    if raw_preview:
        trimmed_items["rawPreview"] = raw_preview

    trimmed = {k: v for k, v in scrape.items() if k != "items"}
    trimmed["items"] = trimmed_items
    return trimmed


@activity.defn
async def store_scrape(scrape: Dict[str, Any]) -> str:
    from .convex_client import convex_mutation

    payload = trim_scrape_for_convex(scrape)
    now = int(time.time() * 1000)

    try:
        scrape_id = await convex_mutation(
            "router:insertScrapeRecord",
            {
                "sourceUrl": payload["sourceUrl"],
                "pattern": payload.get("pattern"),
                "startedAt": payload.get("startedAt", now),
                "completedAt": payload.get("completedAt", now),
                "items": payload.get("items"),
            },
        )
    except Exception:
        # Fallback: aggressively trim and retry once so we still record the run
        fallback = trim_scrape_for_convex(
            scrape,
            max_items=100,
            max_description=400,
            raw_preview_chars=0,
        )
        if isinstance(fallback.get("items"), dict):
            fallback["items"]["truncated"] = True
        scrape_id = await convex_mutation(
            "router:insertScrapeRecord",
            {
                "sourceUrl": fallback["sourceUrl"],
                "pattern": fallback.get("pattern"),
                "startedAt": fallback.get("startedAt", now),
                "completedAt": fallback.get("completedAt", now),
                "items": fallback.get("items"),
            },
        )

    # Best-effort job ingestion (mimics router.ts behavior)
    try:
        jobs = _jobs_from_scrape_items(payload.get("items"), default_posted_at=now)
        if jobs:
            await convex_mutation("router:ingestJobsFromScrape", {"jobs": jobs})
    except Exception:
        # Non-fatal: ingestion failures shouldn't block scrape recording
        pass

    return str(scrape_id)


@activity.defn
async def complete_site(site_id: str) -> None:
    from .convex_client import convex_mutation

    await convex_mutation("router:completeSite", {"id": site_id})


@activity.defn
async def fail_site(payload: Dict[str, Any]) -> None:
    from .convex_client import convex_mutation

    await convex_mutation("router:failSite", {"id": payload["id"], "error": payload.get("error")})


@activity.defn
async def record_workflow_run(run: Dict[str, Any]) -> None:
    from .convex_client import convex_mutation

    payload = {k: v for k, v in run.items() if v is not None}
    try:
        await convex_mutation("temporal:recordWorkflowRun", payload)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to record workflow run: {e}") from e
