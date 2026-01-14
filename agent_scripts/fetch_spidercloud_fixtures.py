"""
Fetch live SpiderCloud scrape payloads and write them into our test fixtures.

Usage:
    uv run agent_scripts/fetch_spidercloud_fixtures.py

Requires:
    - SPIDER_API_KEY in .env (loaded via python-dotenv)
Outputs:
    - tests/job_scrape_application/workflows/fixtures/spidercloud_greenhouse_api_raw.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_greenhouse_api_commonmark.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_datadog_greenhouse_listing_raw.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_pinterest_marketing_commonmark.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_bloomberg_avature_search_page_1.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_bloomberg_avature_search_page_2.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_bloomberg_avature_search_page_3.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_godaddy_search_page_1.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_godaddy_job_detail_commonmark.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_confluent_engineering_commonmark.json
    - tests/fixtures/spidercloud_lambda_ai_careers.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urljoin

import yaml
from dotenv import load_dotenv
from spider import AsyncSpider as SpiderAsyncSpider

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.config import get_env_dir
from job_scrape_application.workflows import activities as workflow_activities
from job_scrape_application.workflows.core import CapturingSpiderClient
from job_scrape_application.workflows.scrapers import spidercloud_scraper
from job_scrape_application.workflows.site_handlers import get_site_handler
from job_scrape_application.workflows.site_handlers import AvatureHandler

GH_JOB_URL = "https://boards-api.greenhouse.io/v1/boards/pinterest/jobs/5572858"
DATADOG_GREENHOUSE_LISTING_URL = "https://api.greenhouse.io/v1/boards/datadog/jobs"
PINTEREST_MARKETING_URL = "https://www.pinterestcareers.com/jobs/?gh_jid=5572858"
CONFLUENT_ENGINEERING_URL = (
    "https://careers.confluent.io/jobs/united_states-engineering?engineering=engineering"
)
LAMBDA_CAREERS_URL = "https://lambda.ai/careers"
AVATURE_BLOOMBERG_URL = (
    "https://bloomberg.avature.net/careers/SearchJobs/engineer?"
    "1845=%5B162508%5D&1845_format=3996&1686=%5B57029%5D&1686_format=2312&"
    "listFilterMode=1&jobRecordsPerPage=12"
)
AVATURE_BLOOMBERG_PAGE_2_URL = f"{AVATURE_BLOOMBERG_URL}&jobOffset=12"
AVATURE_BLOOMBERG_PAGE_3_URL = f"{AVATURE_BLOOMBERG_URL}&jobOffset=24"
GODADDY_SEARCH_URL = "https://careers.godaddy/jobs/search?page=1&query=engineer&country_codes%5B%5D=US"
GODADDY_DETAIL_URL = "https://careers.godaddy/jobs/principal-security-engineer-florida-united-states"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

FIXTURES: Tuple[Tuple[str, str, Dict[str, Any]], ...] = (
    (
        "spidercloud_greenhouse_api_raw.json",
        "/scrape",
        {"return_format": "raw_html", "url": GH_JOB_URL, "limit": 1},
    ),
    (
        "spidercloud_greenhouse_api_commonmark.json",
        "/scrape",
        {"return_format": "commonmark", "url": GH_JOB_URL, "limit": 1},
    ),
    (
        "spidercloud_pinterest_marketing_commonmark.json",
        "/scrape",
        {"return_format": "commonmark", "url": PINTEREST_MARKETING_URL, "limit": 1},
    ),
    (
        "spidercloud_bloomberg_avature_search_page_1.json",
        "/scrape",
        {"return_format": "raw_html", "url": AVATURE_BLOOMBERG_URL, "limit": 1},
    ),
    (
        "spidercloud_bloomberg_avature_search_page_2.json",
        "/scrape",
        {"return_format": "raw_html", "url": AVATURE_BLOOMBERG_PAGE_2_URL, "limit": 1},
    ),
    (
        "spidercloud_bloomberg_avature_search_page_3.json",
        "/scrape",
        {"return_format": "raw_html", "url": AVATURE_BLOOMBERG_PAGE_3_URL, "limit": 1},
    ),
    (
        "spidercloud_godaddy_search_page_1.json",
        "/scrape",
        {"return_format": "raw_html", "url": GODADDY_SEARCH_URL, "limit": 1},
    ),
    (
        "spidercloud_godaddy_job_detail_commonmark.json",
        "/scrape",
        {"return_format": "commonmark", "url": GODADDY_DETAIL_URL, "limit": 1},
    ),
    (
        "spidercloud_datadog_greenhouse_listing_raw.json",
        "/scrape",
        {
            "url": DATADOG_GREENHOUSE_LISTING_URL,
            "return_format": ["raw"],
            "request": "basic",
            "metadata": True,
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
            "limit": 1,
        },
    ),
    (
        "spidercloud_confluent_engineering_commonmark.json",
        "/scrape",
        {
            "url": CONFLUENT_ENGINEERING_URL,
            "return_format": ["commonmark", "raw_html"],
            "request": "chrome",
            "metadata": True,
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
            "limit": 1,
        },
    ),
    (
        "tests/fixtures/spidercloud_lambda_ai_careers.json",
        "/scrape",
        {
            "url": LAMBDA_CAREERS_URL,
            "return_format": ["commonmark"],
            "request": "chrome",
            "metadata": True,
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
            "limit": 1,
        },
    ),
)

FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures")
SCHEDULE_FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/dbos_schedule")
SINGLE_REQUEST_FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/single_request")


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    return cleaned.strip("_") or "site"


def _load_schedule_entries(env: str) -> List[Dict[str, Any]]:
    schedule_path = get_env_dir(env) / "site_schedules.yml"
    if not schedule_path.exists():
        raise SystemExit(f"Missing site schedule config: {schedule_path}")
    payload = yaml.safe_load(schedule_path.read_text(encoding="utf-8")) or {}
    entries = payload if isinstance(payload, list) else payload.get("site_schedules", [])
    if not isinstance(entries, list):
        return []
    return [entry for entry in entries if isinstance(entry, dict) and entry.get("enabled", True)]


def _detail_url_from_scrape(scrape_payload: Dict[str, Any], source_url: str) -> str | None:
    items = scrape_payload.get("items") if isinstance(scrape_payload, dict) else None
    if not isinstance(items, dict):
        return None
    normalized = items.get("normalized")
    if not isinstance(normalized, list):
        return None
    saw_description = False
    for row in normalized:
        if not isinstance(row, dict):
            continue
        description = row.get("description") or row.get("job_description")
        if not isinstance(description, str) or not description.strip():
            continue
        saw_description = True
        for key in ("url", "job_url", "absolute_url", "apply_url"):
            value = row.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    if saw_description and source_url:
        return source_url
    return None


def _extract_listing_job_urls(
    payload: Dict[str, Any],
    source_url: str,
    pattern: str | None,
) -> List[str]:
    if not isinstance(payload, dict):
        return []
    extracted = workflow_activities._extract_job_urls_from_scrape(payload)  # noqa: SLF001
    handler = get_site_handler(source_url) if source_url else None
    detail_override = _detail_url_from_scrape(payload, source_url) if not extracted else None
    if detail_override:
        return [detail_override]
    return workflow_activities._filter_job_urls(  # noqa: SLF001
        extracted,
        handler,
        workflow_activities._is_probable_listing_url,  # noqa: SLF001
        pattern=pattern,
        source_url=source_url,
    )


class _FixtureCaptureSpiderFactory:
    """Factory that creates CapturingSpiderClient instances for fixture generation.

    This wrapper is needed because the monkeypatch expects a callable that takes api_key
    and returns a spider client. It uses CapturingSpiderClient from the core module.
    """

    def __init__(self, captures: List[Dict[str, Any]]) -> None:
        self._captures = captures

    def __call__(self, api_key: str) -> CapturingSpiderClient:
        real_client = SpiderAsyncSpider(api_key=api_key)
        return CapturingSpiderClient(real_client, self._captures)


async def _collect_response(response: Any) -> Any:
    if hasattr(response, "__aiter__"):
        items = []
        async for item in response:
            items.append(item)
        return items
    if hasattr(response, "__await__"):
        return await response
    return response


def _normalize_capture(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {key: _normalize_capture(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_normalize_capture(item) for item in value]
    return value


async def _capture_workflow_scrape(
    url: str,
    *,
    source_url: str,
    pattern: str | None,
    label: str,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Run workflow scrape and capture the SpiderCloud request/response.

    Uses CapturingSpiderClient from the core module to capture all SpiderCloud
    interactions for fixture generation.
    """
    captures: List[Dict[str, Any]] = []
    original_async_spider = spidercloud_scraper.AsyncSpider
    try:
        # Use the factory to create CapturingSpiderClient instances
        spidercloud_scraper.AsyncSpider = _FixtureCaptureSpiderFactory(captures)
        scraper = workflow_activities._make_spidercloud_scraper()
        payload = await scraper._scrape_urls_batch(
            [url],
            source_url=source_url,
            pattern=pattern,
        )
    finally:
        spidercloud_scraper.AsyncSpider = original_async_spider
    if not captures:
        raise SystemExit(f"No SpiderCloud response captured for {label} {url}")
    capture = _normalize_capture(captures[0])
    if not isinstance(capture, dict):
        raise SystemExit(f"Unexpected SpiderCloud capture for {label} {url}")
    return capture, payload


async def _fetch_schedule_fixtures(
    *,
    env: str,
    output_dir: Path,
    only: List[str] | None,
    limit: int | None,
) -> None:
    entries = _load_schedule_entries(env)
    if only:
        allow = {_slugify(val) for val in only}
        entries = [
            entry
            for entry in entries
            if _slugify(str(entry.get("name") or entry.get("url") or "")) in allow
        ]
    if limit is not None:
        entries = entries[: max(limit, 0)]

    output_dir.mkdir(parents=True, exist_ok=True)

    for entry in entries:
        url = entry.get("url")
        if not isinstance(url, str) or not url.strip():
            continue
        slug = _slugify(str(entry.get("name") or url))
        pattern = entry.get("pattern") if isinstance(entry.get("pattern"), str) else None
        listing_path = output_dir / f"{slug}_listing.json"
        detail_path = output_dir / f"{slug}_detail.json"

        listing_capture_clean, listing_payload = await _capture_workflow_scrape(
            url,
            source_url=url,
            pattern=pattern,
            label="listing",
        )
        listing_path.write_text(
            json.dumps(listing_capture_clean, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        listing_job_urls = _extract_listing_job_urls(listing_payload, url, pattern)
        if not listing_job_urls:
            raw_response = listing_capture_clean.get("response")
            if raw_response:
                listing_job_urls = _extract_listing_job_urls(
                    {"items": {"raw": raw_response}, "sourceUrl": url},
                    url,
                    pattern,
                )
        listing_url_key = url.rstrip("/").lower()
        listing_job_urls = [
            job_url
            for job_url in listing_job_urls
            if isinstance(job_url, str) and job_url.rstrip("/").lower() != listing_url_key
        ]
        if not listing_job_urls:
            raise SystemExit(f"No job URLs extracted for listing {url}")

        handler = get_site_handler(url)
        detail_url: str | None = None
        for candidate in listing_job_urls:
            if not isinstance(candidate, str) or not candidate.strip():
                continue
            normalized = (
                candidate
                if candidate.startswith(("http://", "https://"))
                else urljoin(url, candidate)
            )
            candidate_handler = handler or get_site_handler(normalized)
            if candidate_handler:
                if candidate_handler.is_listing_url(normalized):
                    continue
            elif workflow_activities._is_probable_listing_url(normalized):  # noqa: SLF001
                continue
            detail_url = normalized
            break
        if detail_url is None:
            detail_url = listing_job_urls[0]
            if not detail_url.startswith(("http://", "https://")):
                detail_url = urljoin(url, detail_url)
        handler = get_site_handler(detail_url) or handler
        if handler and handler.name == "greenhouse":
            api_url = handler.get_api_uri(detail_url, source_url=url)
            if api_url:
                detail_url = api_url
        detail_capture_clean, _ = await _capture_workflow_scrape(
            detail_url,
            source_url=url,
            pattern=pattern,
            label="detail",
        )
        detail_path.write_text(
            json.dumps(detail_capture_clean, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info(f"Wrote {listing_path} and {detail_path}")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch SpiderCloud fixtures for tests.")
    parser.add_argument(
        "--only",
        nargs="*",
        help="Optional fixture filenames to refresh (e.g. spidercloud_godaddy_search_page_1.json).",
    )
    parser.add_argument(
        "--schedule-env",
        choices=["dev", "prod"],
        help="Generate fixtures for the site schedule in this environment.",
    )
    parser.add_argument(
        "--schedule-only",
        nargs="*",
        help="Optional site names/slugs to include when fetching schedule fixtures.",
    )
    parser.add_argument(
        "--schedule-limit",
        type=int,
        help="Optional limit on number of schedule sites to fetch.",
    )
    parser.add_argument(
        "--schedule-out",
        default=str(SCHEDULE_FIXTURE_DIR),
        help="Output directory for schedule fixtures.",
    )
    parser.add_argument(
        "--single-request-mode",
        action="store_true",
        default=True,
        help="Generate fixtures for single request mode (stream=False). Default: True",
    )
    parser.add_argument(
        "--legacy-stream-mode",
        action="store_true",
        help="Generate fixtures for legacy streaming mode (stream=True, JSONL).",
    )
    args = parser.parse_args()

    load_dotenv()
    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        raise SystemExit("SPIDER_API_KEY (or SPIDER_KEY) is not set in environment/.env")

    # Determine fixture mode (single request vs legacy streaming)
    use_single_request = args.single_request_mode and not args.legacy_stream_mode

    # Set runtime config to match requested mode
    # This affects how _capture_workflow_scrape calls the scraper
    from job_scrape_application.config import runtime_config as _rc
    object.__setattr__(_rc, "spidercloud_single_request_mode", use_single_request)

    if args.schedule_env:
        # Determine output directory based on mode
        if use_single_request and args.schedule_out == str(SCHEDULE_FIXTURE_DIR):
            output_dir = SINGLE_REQUEST_FIXTURE_DIR
            logger.info("Using single request mode, output to: %s", output_dir)
        else:
            output_dir = Path(args.schedule_out)
            logger.info("Using %s mode, output to: %s",
                       "single request" if use_single_request else "legacy stream",
                       output_dir)

        await _fetch_schedule_fixtures(
            env=args.schedule_env,
            output_dir=output_dir,
            only=args.schedule_only,
            limit=args.schedule_limit,
        )
        return

    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)

    async with SpiderAsyncSpider(api_key=api_key) as client:
        avature_handler = AvatureHandler()
        fixtures = list(FIXTURES)
        if args.only:
            allow = set(args.only)
            fixtures = [entry for entry in fixtures if entry[0] in allow]
            missing = allow.difference({entry[0] for entry in fixtures})
            if missing:
                raise SystemExit(f"Unknown fixture(s): {', '.join(sorted(missing))}")
        for filename, endpoint, payload in fixtures:
            path = Path(filename)
            if not path.is_absolute() and not str(path).startswith("tests/"):
                path = FIXTURE_DIR / filename
            try:
                url = payload.get("url")
                if not isinstance(url, str) or not url:
                    raise SystemExit(f"Fixture {filename} missing URL")
                params = {k: v for k, v in payload.items() if k != "url"}
                if avature_handler.matches_url(url):
                    params.update(avature_handler.get_spidercloud_config(url))
                return_format = params.get("return_format")
                if isinstance(return_format, str):
                    params["return_format"] = [return_format]
                logger.info(f"Fetching {url} -> {filename} via {endpoint}")
                response = await _collect_response(
                    client.scrape_url(
                        url,
                        params=params,
                        stream=False,
                        content_type="application/json",
                    )
                )
                fixture = {
                    "request": {"endpoint": endpoint, "url": url, "params": params},
                    "response": response,
                }
                path.write_text(json.dumps(fixture, ensure_ascii=False, indent=2), encoding="utf-8")
                logger.info(f"  wrote {path} ({len(json.dumps(fixture))} bytes)")
            except Exception as exc:  # noqa: BLE001
                logger.error(f"  failed to fetch {filename}: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
