from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv
from spider import AsyncSpider

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows.site_handlers import AvatureHandler

DEFAULT_BASE_URL = (
    "https://bloomberg.avature.net/careers/SearchJobs/engineer?"
    "1845=%5B162619%2C162522%2C162483%2C162484%2C162552%2C162508%2C162520%2C162535%5D&"
    "1845_format=3996&1686=%5B57029%5D&1686_format=2312&listFilterMode=1&jobRecordsPerPage=12&"
)
FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures")
FIXTURE_NAMES = (
    "spidercloud_bloomberg_avature_search_page_1.json",
    "spidercloud_bloomberg_avature_search_page_2.json",
    "spidercloud_bloomberg_avature_search_page_3.json",
)


def _build_urls(base_url: str) -> List[str]:
    return [
        base_url,
        f"{base_url}jobOffset=12",
        f"{base_url}jobOffset=24",
    ]


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
    parser = argparse.ArgumentParser(description="Fetch Bloomberg Avature SpiderCloud fixtures.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Base Bloomberg Avature search URL")
    args = parser.parse_args()

    load_dotenv()
    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        raise SystemExit("SPIDER_API_KEY (or SPIDER_KEY) is not set in environment/.env")

    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    urls = _build_urls(args.base_url)

    async with AsyncSpider(api_key=api_key) as client:
        handler = AvatureHandler()
        for filename, url in zip(FIXTURE_NAMES, urls):
            path = FIXTURE_DIR / filename
            payload: Dict[str, Any] = {
                "return_format": ["raw_html"],
                "url": url,
                "limit": 1,
            }
            payload.update(handler.get_spidercloud_config(url))
            print(f"Fetching {url} -> {filename}")
            response = await _collect_response(
                client.scrape_url(
                    url,
                    params={k: v for k, v in payload.items() if k != "url"},
                    stream=False,
                    content_type="application/json",
                )
            )
            path.write_text(json.dumps(response, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"  wrote {path} ({len(json.dumps(response))} bytes)")


if __name__ == "__main__":
    asyncio.run(main())
