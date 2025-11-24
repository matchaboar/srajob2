from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, TypedDict
from urllib.parse import urlparse

import httpx
from fetchfox_sdk import FetchFox
from pydantic import BaseModel, Field, field_validator
from temporalio import activity
from temporalio.exceptions import ApplicationError

from .config import settings


MAX_FETCHFOX_VISITS = 20
DEFAULT_TOTAL_COMPENSATION = 151000


class FetchFoxPriority(BaseModel):
    """Priority block passed to FetchFox crawl API.

    Mirrors the cURL example from the FetchFox docs: the `skip` array contains URLs we
    have already stored as jobs in Convex, so the crawler should avoid visiting them.
    """

    skip: List[str] = Field(
        default_factory=list,
        description=(
            "Job detail URLs already persisted for this site; always send the full set so FetchFox skips them."
        ),
    )
    only: Optional[List[str]] = None
    high: Optional[List[str]] = None
    low: Optional[List[str]] = None


class FetchFoxScrapeRequest(BaseModel):
    pattern: Optional[str] = None
    start_urls: List[str]
    max_depth: int = 5
    max_visits: int = Field(
        default=MAX_FETCHFOX_VISITS,
        description="Hard cap per run to avoid excessive crawling; forced to 20 for scraper workloads.",
    )
    template: Dict[str, str]
    priority: FetchFoxPriority

    @field_validator("max_visits")
    @classmethod
    def cap_visits(cls, value: int) -> int:
        return min(MAX_FETCHFOX_VISITS, value)


class Site(TypedDict, total=False):
    _id: str
    name: Optional[str]
    url: str
    pattern: Optional[str]
    enabled: bool
    lastRunAt: Optional[int]
    lockedBy: Optional[str]
    lockExpiresAt: Optional[int]
    completed: Optional[bool]


async def fetch_seen_urls_for_site(source_url: str, pattern: Optional[str]) -> List[str]:
    """Return every URL we've already scraped for the site so FetchFox can skip them."""

    if not settings.convex_http_url:
        return []

    url = settings.convex_http_url.rstrip("/") + "/api/sites/skip-urls"
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, json={"sourceUrl": source_url, "pattern": pattern})
            resp.raise_for_status()
            payload = resp.json()
    except Exception:
        return []

    urls = payload.get("urls", []) if isinstance(payload, dict) else []
    if not isinstance(urls, list):
        return []

    return [u for u in urls if isinstance(u, str)]


@activity.defn
async def fetch_sites() -> List[Site]:
    if not settings.convex_http_url:
        raise RuntimeError("CONVEX_HTTP_URL env var is required")
    url = settings.convex_http_url.rstrip("/") + "/api/sites"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected sites payload: {data!r}")
        return data  # type: ignore[return-value]


@activity.defn
async def lease_site(worker_id: str, lock_seconds: int = 300) -> Optional[Site]:
    if not settings.convex_http_url:
        raise RuntimeError("CONVEX_HTTP_URL env var is required")
    url = settings.convex_http_url.rstrip("/") + "/api/sites/lease"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, json={"workerId": worker_id, "lockSeconds": lock_seconds})
        resp.raise_for_status()
        data = resp.json()
        if data is None:
            return None
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected lease payload: {data!r}")
        return data  # type: ignore[return-value]


@activity.defn
async def scrape_site(site: Site) -> Dict[str, Any]:
    if not settings.fetchfox_api_key:
        # Mark as non-retryable to avoid endless attempts for a known config issue
        raise ApplicationError("FETCHFOX_API_KEY env var is required for FetchFox", non_retryable=True)

    pattern = site.get("pattern")
    skip_urls = await fetch_seen_urls_for_site(site["url"], pattern)
    start_urls = [site["url"]]
    template = {
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

    request = FetchFoxScrapeRequest(
        pattern=pattern,
        start_urls=start_urls,
        max_depth=5,
        max_visits=MAX_FETCHFOX_VISITS,
        template=template,
        priority=FetchFoxPriority(skip=skip_urls),
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

    return {
        "sourceUrl": site["url"],
        "pattern": pattern,
        "startedAt": started_at,
        "completedAt": completed_at,
        "items": {"normalized": normalized_items, "raw": result_obj},
    }


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

    company_raw = stringify(row.get("company") or row.get("employer") or row.get("organization") or "")
    company = company_raw or derive_company_from_url(url) or "Unknown"

    location = stringify(row.get("location") or row.get("city") or row.get("region") or "")
    remote = coerce_remote(row.get("remote"), location, title)
    if not location:
        location = "Remote" if remote else "Unknown"

    level = coerce_level(row.get("level"), title)
    description = extract_description(row)
    total_comp = parse_compensation(row.get("total_compensation") or row.get("salary") or row.get("compensation"))
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
        "_raw": row,
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


@activity.defn
async def store_scrape(scrape: Dict[str, Any]) -> str:
    if not settings.convex_http_url:
        raise RuntimeError("CONVEX_HTTP_URL env var is required")
    url = settings.convex_http_url.rstrip("/") + "/api/scrapes"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, json=scrape)
        resp.raise_for_status()
        data = resp.json()
        return str(data.get("scrapeId"))


@activity.defn
async def complete_site(site_id: str) -> None:
    if not settings.convex_http_url:
        raise RuntimeError("CONVEX_HTTP_URL env var is required")
    url = settings.convex_http_url.rstrip("/") + "/api/sites/complete"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, json={"id": site_id})
        resp.raise_for_status()
        _ = resp.json()


@activity.defn
async def fail_site(payload: Dict[str, Any]) -> None:
    if not settings.convex_http_url:
        raise RuntimeError("CONVEX_HTTP_URL env var is required")
    url = settings.convex_http_url.rstrip("/") + "/api/sites/fail"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, json={"id": payload["id"], "error": payload.get("error")})
        resp.raise_for_status()
        _ = resp.json()


@activity.defn
async def record_workflow_run(run: Dict[str, Any]) -> None:
    if not settings.convex_http_url:
        raise RuntimeError("CONVEX_HTTP_URL env var is required")
    url = settings.convex_http_url.rstrip("/") + "/api/temporal/workflow-run"
    payload = {k: v for k, v in run.items() if v is not None}
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            body = e.response.text if e.response else ""
            raise RuntimeError(f"Failed to record workflow run ({e.response.status_code if e.response else '???'}): {body}") from e
