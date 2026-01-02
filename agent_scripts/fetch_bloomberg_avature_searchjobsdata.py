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


DEFAULT_BASE_URL = (
    "https://bloomberg.avature.net/careers/SearchJobsData/engineer?"
    "1845=%5B162508%5D&1845_format=3996&1686=%5B57029%5D&1686_format=2312&"
    "listFilterMode=1&jobRecordsPerPage=12"
)
FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures")
FIXTURE_NAMES = (
    "spidercloud_bloomberg_avature_searchjobsdata_page_1.json",
    "spidercloud_bloomberg_avature_searchjobsdata_page_2.json",
    "spidercloud_bloomberg_avature_searchjobsdata_page_3.json",
)


def _build_urls(base_url: str) -> List[str]:
    return [
        base_url,
        f"{base_url}&jobOffset=12",
        f"{base_url}&jobOffset=24",
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


def _seed_url_from_base(base_url: str) -> str:
    return base_url.replace("/SearchJobsData/", "/SearchJobs/")


async def _seed_session(client: AsyncSpider, seed_url: str) -> None:
    params: Dict[str, Any] = {
        "return_format": ["raw_html"],
        "metadata": True,
        "request": "chrome",
        "follow_redirects": True,
        "redirect_policy": "Loose",
        "external_domains": ["*"],
        "preserve_host": True,
        "limit": 1,
    }
    await _collect_response(
        client.scrape_url(
            seed_url,
            params=params,
            stream=False,
            content_type="application/json",
        )
    )


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Bloomberg Avature SearchJobsData JSON via SpiderCloud."
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Base SearchJobsData URL")
    args = parser.parse_args()

    load_dotenv()
    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        raise SystemExit("SPIDER_API_KEY (or SPIDER_KEY) is not set in environment/.env")

    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    urls = _build_urls(args.base_url)

    async with AsyncSpider(api_key=api_key) as client:
        seed_url = _seed_url_from_base(args.base_url)
        await _seed_session(client, seed_url)
        for filename, url in zip(FIXTURE_NAMES, urls):
            path = FIXTURE_DIR / filename
            params: Dict[str, Any] = {
                "return_format": ["raw"],
                "metadata": True,
                "request": "http",
                "follow_redirects": True,
                "redirect_policy": "Loose",
                "external_domains": ["*"],
                "preserve_host": True,
                "limit": 1,
                "headers": {
                    "accept": "application/json, text/plain, */*",
                    "x-requested-with": "XMLHttpRequest",
                },
            }
            print(f"Fetching {url} -> {filename}")
            response = await _collect_response(
                client.scrape_url(
                    url,
                    params=params,
                    stream=False,
                    content_type="application/json",
                )
            )
            fixture = {
                "request": {"endpoint": "/scrape", "url": url, "params": params},
                "response": response,
            }
            path.write_text(json.dumps(fixture, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"  wrote {path} ({len(json.dumps(fixture))} bytes)")


if __name__ == "__main__":
    asyncio.run(main())
