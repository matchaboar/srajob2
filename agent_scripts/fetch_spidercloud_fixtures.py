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
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urljoin, urlparse

import yaml
from dotenv import load_dotenv
from spider import AsyncSpider as SpiderAsyncSpider

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.config import get_env_dir
from job_scrape_application.workflows import activities as workflow_activities
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


def _extract_listing_job_urls(payload: Dict[str, Any]) -> List[str]:
    items = payload.get("items") if isinstance(payload, dict) else None
    if isinstance(items, dict):
        job_urls = items.get("job_urls")
        if isinstance(job_urls, list):
            urls = [url for url in job_urls if isinstance(url, str) and url.strip()]
            if urls:
                return urls
    try:
        urls = workflow_activities._extract_job_urls_from_scrape(payload)  # noqa: SLF001
    except Exception:
        urls = []
    return [url for url in urls if isinstance(url, str) and url.strip()]


class _FixtureCaptureSpider:
    def __init__(self, api_key: str, captures: List[Dict[str, Any]]) -> None:
        self._api_key = api_key
        self._captures = captures
        self._client: SpiderAsyncSpider | None = None

    async def __aenter__(self) -> "_FixtureCaptureSpider":
        self._client = SpiderAsyncSpider(api_key=self._api_key)
        await self._client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._client is None:
            return False
        return await self._client.__aexit__(exc_type, exc, tb)

    def scrape_url(self, url: str, *, params: Dict[str, Any], stream: bool, content_type: str):
        if self._client is None:
            raise RuntimeError("SpiderCloud client not initialized")
        response = self._client.scrape_url(
            url,
            params=params,
            stream=stream,
            content_type=content_type,
        )
        capture: Dict[str, Any] = {
            "request": {
                "url": url,
                "params": params,
                "stream": stream,
                "contentType": content_type,
            },
            "response": [],
        }
        self._captures.append(capture)

        async def _iterator():
            items: List[Any] = []
            try:
                if hasattr(response, "__aiter__"):
                    async for item in response:
                        items.append(item)
                        yield item
                elif hasattr(response, "__await__"):
                    result = await response
                    if result is not None:
                        items.append(result)
                        yield result
                elif response is not None:
                    items.append(response)
                    yield response
            finally:
                capture["response"] = items

        return _iterator()


async def _collect_response(response: Any) -> Any:
    if hasattr(response, "__aiter__"):
        items = []
        async for item in response:
            items.append(item)
        return items
    if hasattr(response, "__await__"):
        return await response
    return response


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

    original_async_spider = spidercloud_scraper.AsyncSpider
    try:
        for entry in entries:
            url = entry.get("url")
            if not isinstance(url, str) or not url.strip():
                continue
            slug = _slugify(str(entry.get("name") or url))
            pattern = entry.get("pattern") if isinstance(entry.get("pattern"), str) else None
            listing_path = output_dir / f"{slug}_listing.json"
            detail_path = output_dir / f"{slug}_detail.json"

            captures: List[Dict[str, Any]] = []
            spidercloud_scraper.AsyncSpider = (
                lambda api_key, captures=captures: _FixtureCaptureSpider(api_key, captures)
            )
            scraper = workflow_activities._make_spidercloud_scraper()
            listing_payload = await scraper._scrape_urls_batch(
                [url],
                source_url=url,
                pattern=pattern,
            )
            if not captures:
                raise SystemExit(f"No SpiderCloud response captured for listing {url}")
            listing_capture = captures[0]
            # Convert bytes to string in listing_capture before JSON serialization
            def decode_bytes(value: bytes) -> str:
                return value.decode("utf-8", errors="replace")

            def bytes_to_str(obj):
                if isinstance(obj, bytes):
                    return decode_bytes(obj)
                if isinstance(obj, dict):
                    return {k: bytes_to_str(v) for k, v in obj.items()}
                if isinstance(obj, list):
                    return [bytes_to_str(i) for i in obj]
                return obj

            listing_capture_clean = bytes_to_str(listing_capture)
            listing_path.write_text(
                json.dumps(listing_capture_clean, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            listing_job_urls = _extract_listing_job_urls(listing_payload)
            listing_response = (
                listing_capture_clean.get("response")
                if isinstance(listing_capture_clean, dict)
                else None
            )
            parsed_response: Any = listing_response
            if isinstance(listing_response, list):
                parsed_items: list[Any] = []
                for entry in listing_response:
                    if isinstance(entry, str):
                        try:
                            parsed_items.append(json.loads(entry))
                        except json.JSONDecodeError:
                            parsed_items.append(entry)
                    else:
                        parsed_items.append(entry)
                parsed_response = parsed_items
            if not listing_job_urls:
                listing_job_urls = _extract_listing_job_urls(
                    {"items": {"raw": parsed_response}, "sourceUrl": url}
                )
            listing_url_key = url.rstrip("/").lower()
            listing_job_urls = [
                job_url
                for job_url in listing_job_urls
                if isinstance(job_url, str) and job_url.rstrip("/").lower() != listing_url_key
            ]
            if not listing_job_urls:
                ashby_ids: set[str] = set()

                def _collect_ids(text: str) -> None:
                    ashby_ids.update(re.findall(r'jobId":"([0-9a-f-]+)"', text))
                    ashby_ids.update(re.findall(r'jobId\\":\\"([0-9a-f-]+)', text))
                    ashby_ids.update(re.findall(r'jobPostingId":"([0-9a-f-]+)"', text))
                    ashby_ids.update(
                        re.findall(r'jobPostingId\\":\\"([0-9a-f-]+)', text)
                    )

                def _walk_raw(node: Any) -> None:
                    if isinstance(node, dict):
                        content = node.get("content")
                        raw_html = content.get("raw") if isinstance(content, dict) else None
                        if isinstance(raw_html, str):
                            _collect_ids(raw_html)
                        for value in node.values():
                            _walk_raw(value)
                    elif isinstance(node, list):
                        for child in node:
                            _walk_raw(child)
                    elif isinstance(node, str):
                        _collect_ids(node)

                _walk_raw(parsed_response)

                if ashby_ids:
                    slug = url.rstrip("/").split("/")[-1]
                    listing_job_urls = [
                        f"https://jobs.ashbyhq.com/{slug}/{job_id}"
                        for job_id in sorted(ashby_ids)
                    ]
                listing_job_urls = [
                    job_url
                    for job_url in listing_job_urls
                    if isinstance(job_url, str) and job_url.rstrip("/").lower() != listing_url_key
                ]
            if not listing_job_urls:
                source_host = urlparse(url).hostname or ""
                candidate_urls: set[str] = set()

                def _collect_urls(text: str) -> None:
                    for match in re.findall(r"https?://[^\s\"'<>]+", text):
                        candidate_urls.add(match.rstrip(").,]"))

                def _walk_urls(node: Any) -> None:
                    if isinstance(node, dict):
                        for value in node.values():
                            _walk_urls(value)
                    elif isinstance(node, list):
                        for child in node:
                            _walk_urls(child)
                    elif isinstance(node, str):
                        _collect_urls(node)

                _walk_urls(parsed_response)
                listing_job_urls = [
                    candidate
                    for candidate in sorted(candidate_urls)
                    if not source_host or (urlparse(candidate).hostname or "") == source_host
                ]
                listing_job_urls = [
                    job_url
                    for job_url in listing_job_urls
                    if isinstance(job_url, str) and job_url.rstrip("/").lower() != listing_url_key
                ]
            if not listing_job_urls:
                raise SystemExit(f"No job URLs extracted for listing {url}")

            detail_url = listing_job_urls[0]
            if not detail_url.startswith(("http://", "https://")):
                detail_url = urljoin(url, detail_url)
            handler = get_site_handler(detail_url) or get_site_handler(url)
            if handler and handler.name == "greenhouse":
                api_url = handler.get_api_uri(detail_url)
                if api_url:
                    detail_url = api_url
            captures = []
            spidercloud_scraper.AsyncSpider = (
                lambda api_key, captures=captures: _FixtureCaptureSpider(api_key, captures)
            )
            scraper = workflow_activities._make_spidercloud_scraper()
            await scraper._scrape_urls_batch(
                [detail_url],
                source_url=url,
                pattern=pattern,
            )
            if not captures:
                raise SystemExit(f"No SpiderCloud response captured for detail {detail_url}")
            detail_capture = captures[0]
            def bytes_to_str_in_detail(obj):
                if isinstance(obj, bytes):
                    return decode_bytes(obj)
                if isinstance(obj, dict):
                    return {k: bytes_to_str_in_detail(v) for k, v in obj.items()}
                if isinstance(obj, list):
                    return [bytes_to_str_in_detail(i) for i in obj]
                return obj

            detail_capture_clean = bytes_to_str_in_detail(detail_capture)
            detail_path.write_text(
                json.dumps(detail_capture_clean, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"Wrote {listing_path} and {detail_path}")
    finally:
        spidercloud_scraper.AsyncSpider = original_async_spider


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
    args = parser.parse_args()

    load_dotenv()
    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        raise SystemExit("SPIDER_API_KEY (or SPIDER_KEY) is not set in environment/.env")

    if args.schedule_env:
        await _fetch_schedule_fixtures(
            env=args.schedule_env,
            output_dir=Path(args.schedule_out),
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
                print(f"Fetching {url} -> {filename} via {endpoint}")
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
                print(f"  wrote {path} ({len(json.dumps(fixture))} bytes)")
            except Exception as exc:  # noqa: BLE001
                print(f"  failed to fetch {filename}: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
