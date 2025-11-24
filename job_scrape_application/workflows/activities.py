from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, List, Optional, TypedDict

import httpx
from fetchfox_sdk import FetchFox
from pydantic import BaseModel, Field, field_validator
from temporalio import activity
from temporalio.exceptions import ApplicationError

from .config import settings


MAX_FETCHFOX_VISITS = 20


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
        "job_title": "str | None",
        "url": "str | None",
        "location": "str | None",
        "remote": "True | False | None",
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

    completed_at = int(time.time() * 1000)

    return {
        "sourceUrl": site["url"],
        "pattern": pattern,
        "startedAt": started_at,
        "completedAt": completed_at,
        "items": result_obj,
    }


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
