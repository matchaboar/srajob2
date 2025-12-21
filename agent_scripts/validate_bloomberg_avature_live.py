from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any, Dict, List

from dotenv import load_dotenv
from spider import AsyncSpider

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows.site_handlers import AvatureHandler

AVATURE_BLOOMBERG_URL = (
    "https://bloomberg.avature.net/careers/SearchJobs/engineer?"
    "1845=%5B162508%5D&1845_format=3996&1686=%5B57029%5D&1686_format=2312&"
    "listFilterMode=1&jobRecordsPerPage=12"
)


def _build_urls(base_url: str) -> List[str]:
    return [
        base_url,
        f"{base_url}&jobOffset=12",
        f"{base_url}&jobOffset=24",
    ]


async def _collect_response(response: Any) -> List[Any]:
    if hasattr(response, "__aiter__"):
        items = []
        async for item in response:
            items.append(item)
        return items
    if hasattr(response, "__await__"):
        result = await response
        return [result] if result is not None else []
    return [response] if response is not None else []


def _gather_strings(node: Any) -> List[str]:
    if isinstance(node, str):
        return [node]
    if isinstance(node, dict):
        return [text for val in node.values() for text in _gather_strings(val)]
    if isinstance(node, list):
        return [text for val in node for text in _gather_strings(val)]
    return []


def _extract_first_html(payload: Any) -> str | None:
    def _candidate(value: Any) -> str | None:
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", errors="replace")
        if isinstance(value, str):
            return value
        return None

    def _looks_like_html(text: str) -> bool:
        lowered = text.lower()
        return "<html" in lowered or "<body" in lowered or "<div" in lowered or "<section" in lowered

    for event in payload if isinstance(payload, list) else [payload]:
        if isinstance(event, dict):
            for key in ("raw_html", "html", "content", "body", "text", "result"):
                text = _candidate(event.get(key))
                if text and _looks_like_html(text):
                    return text
        text = _candidate(event)
        if text and _looks_like_html(text):
            return text

    for text in _gather_strings(payload):
        if text and _looks_like_html(text):
            return text

    return None


async def _fetch_html(client: AsyncSpider, url: str) -> str:
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
    response = await _collect_response(
        client.scrape_url(
            url,
            params=params,
            stream=False,
            content_type="application/json",
        )
    )
    html = _extract_first_html(response)
    if not html:
        raise SystemExit(f"Unable to extract raw HTML from SpiderCloud response for {url}")
    return html


async def main() -> None:
    parser = argparse.ArgumentParser(description="Validate Bloomberg Avature live pages against handler logic.")
    parser.add_argument("--base-url", default=AVATURE_BLOOMBERG_URL, help="Base Bloomberg Avature search URL")
    args = parser.parse_args()

    load_dotenv()
    load_dotenv("job_board_application/.env.production", override=False)
    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        raise SystemExit("SPIDER_API_KEY (or SPIDER_KEY) is not set in environment/.env")

    urls = _build_urls(args.base_url)
    handler = AvatureHandler()

    async with AsyncSpider(api_key=api_key) as client:
        results = []
        for url in urls:
            html = await _fetch_html(client, url)
            links = handler.get_links_from_raw_html(html)
            detail_links = [link for link in links if "/careers/JobDetail/" in link]
            pagination_links = [link for link in links if "joboffset=" in link.lower()]
            results.append(
                {
                    "url": url,
                    "detail_links": len(detail_links),
                    "pagination_links": sorted(set(pagination_links)),
                    "sample_detail_links": detail_links[:5],
                }
            )

    print(json.dumps({"results": results}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
