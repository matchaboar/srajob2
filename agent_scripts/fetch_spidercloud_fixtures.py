"""
Fetch live SpiderCloud scrape payloads and write them into our test fixtures.

Usage:
    uv run agent_scripts/fetch_spidercloud_fixtures.py

Requires:
    - SPIDER_API_KEY in .env (loaded via python-dotenv)
Outputs:
    - tests/job_scrape_application/workflows/fixtures/spidercloud_greenhouse_api_raw.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_greenhouse_api_commonmark.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_pinterest_marketing_commonmark.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_bloomberg_avature_search_page_1.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_bloomberg_avature_search_page_2.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_bloomberg_avature_search_page_3.json
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, Tuple

from dotenv import load_dotenv
from spider import AsyncSpider

GH_JOB_URL = "https://boards-api.greenhouse.io/v1/boards/pinterest/jobs/5572858"
PINTEREST_MARKETING_URL = "https://www.pinterestcareers.com/jobs/?gh_jid=5572858"
AVATURE_BLOOMBERG_URL = (
    "https://bloomberg.avature.net/careers/SearchJobs/engineer?"
    "1845=%5B162508%5D&1845_format=3996&1686=%5B57029%5D&1686_format=2312&"
    "listFilterMode=1&jobRecordsPerPage=12"
)
AVATURE_BLOOMBERG_PAGE_2_URL = f"{AVATURE_BLOOMBERG_URL}&jobOffset=12"
AVATURE_BLOOMBERG_PAGE_3_URL = f"{AVATURE_BLOOMBERG_URL}&jobOffset=24"

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
)

FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures")


async def _collect_response(response: Any) -> Any:
    if hasattr(response, "__aiter__"):
        items = []
        async for item in response:
            items.append(item)
        return items
    if hasattr(response, "__await__"):
        return await response
    return response


async def main() -> None:
    load_dotenv()
    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        raise SystemExit("SPIDER_API_KEY (or SPIDER_KEY) is not set in environment/.env")

    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)

    async with AsyncSpider(api_key=api_key) as client:
        for filename, endpoint, payload in FIXTURES:
            path = FIXTURE_DIR / filename
            try:
                url = payload.get("url")
                params = {k: v for k, v in payload.items() if k != "url"}
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
                path.write_text(json.dumps(response, ensure_ascii=False, indent=2), encoding="utf-8")
                print(f"  wrote {path} ({len(json.dumps(response))} bytes)")
            except Exception as exc:  # noqa: BLE001
                print(f"  failed to fetch {filename}: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
