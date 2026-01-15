from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from spider import AsyncSpider

WIDGETS_URL = "https://careers.adobe.com/widgets"

PAYLOAD = {
    "lang": "en_us",
    "deviceType": "desktop",
    "country": "us",
    "pageName": "search-results",
    "refNum": "ADOBUS",
    "siteType": "external",
    "pageId": "page15",
    "ddoKey": "refineSearch",
    "keywords": "engineer",
    "global": True,
    "size": 10,
    "from": 0,
    "all_fields": ["city", "state", "category", "type", "orgFunction", "country"],
    "selected_fields": {
        "state": [
            "California",
            "Colorado",
            "Delaware",
            "Massachusetts",
            "Minnesota",
            "New York",
            "Ontario",
            "Oregon",
            "Utah",
        ],
    },
}


def _load_api_key() -> str:
    load_dotenv()
    load_dotenv("job_board_application/.env.production", override=False)
    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        raise SystemExit("SPIDER_API_KEY (or SPIDER_KEY) is not set in environment/.env")
    return api_key


async def _collect_response(response: Any) -> List[Any]:
    if hasattr(response, "__aiter__"):
        items: List[Any] = []
        async for item in response:
            items.append(item)
        return items
    if hasattr(response, "__await__"):
        result = await response
        return [result] if result is not None else []
    return [response] if response is not None else []


def _extract_status(events: List[Any]) -> Optional[int]:
    for event in events:
        if isinstance(event, dict):
            status = event.get("status")
            if isinstance(status, int):
                return status
            content = event.get("content") if isinstance(event.get("content"), dict) else None
            if isinstance(content, dict):
                status = content.get("status")
                if isinstance(status, int):
                    return status
    return None


def _extract_snippet(events: List[Any]) -> str:
    for event in events:
        if isinstance(event, dict):
            for key in ("content", "raw", "text", "result"):
                value = event.get(key)
                if isinstance(value, dict):
                    raw = value.get("raw")
                    if isinstance(raw, str) and raw:
                        return raw[:200]
                if isinstance(value, str) and value:
                    return value[:200]
    return ""


async def main() -> None:
    api_key = _load_api_key()
    payload_json = json.dumps(PAYLOAD)

    param_sets: List[Dict[str, Any]] = [
        {"request": "http", "method": "POST", "body": payload_json},
        {"request": "http", "http_method": "POST", "body": payload_json},
        {"request": "http", "method": "POST", "payload": PAYLOAD},
        {"request": "http", "method": "POST", "data": PAYLOAD},
        {"request": "http", "method": "POST", "post_data": payload_json},
        {"request": "http", "method": "POST", "post_data": PAYLOAD},
        {"request": "http", "method": "POST", "body": payload_json, "headers": {"Content-Type": "application/json"}},
        {"request": "http", "method": "POST", "payload": payload_json},
        {"request": "http", "method": "POST", "json": PAYLOAD},
    ]

    base = {
        "return_format": ["raw"],
        "metadata": True,
        "follow_redirects": True,
        "redirect_policy": "Loose",
        "external_domains": ["*"],
        "preserve_host": True,
        "limit": 1,
        "headers": {"Content-Type": "application/json", "Accept": "application/json"},
    }

    async with AsyncSpider(api_key=api_key) as client:
        for idx, extra in enumerate(param_sets, start=1):
            params = dict(base)
            params.update(extra)
            response = await _collect_response(
                client.scrape_url(
                    WIDGETS_URL,
                    params=params,
                    stream=False,
                    content_type="application/json",
                )
            )
            status = _extract_status(response)
            snippet = _extract_snippet(response)
            print(f"#{idx} status={status} keys={sorted(extra.keys())} events={len(response)}")
            if snippet:
                print(snippet.replace("\n", " "))
                print("---")
            else:
                for event in response:
                    if isinstance(event, dict):
                        print(f"event_keys={sorted(event.keys())}")
                        if "error" in event:
                            print(f"error={event.get('error')}")
                        break
                    print(f"event_type={type(event).__name__} value={event!r}")
                    break


if __name__ == "__main__":
    asyncio.run(main())
