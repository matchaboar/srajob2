from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from spider import AsyncSpider

DEFAULT_QUERY = "engineer"
DEFAULT_LOCATIONS = [
    {"country": "USA", "region": "California", "city": "San Francisco"},
    {"country": "USA", "region": "California", "city": "Los Angeles"},
    {"country": "USA", "region": "California", "city": "Sunnyvale"},
    {"country": "USA", "region": "California", "city": "Culver City"},
    {"country": "USA", "region": "New York", "city": "New York"},
    {"country": "USA", "region": "Washington", "city": "Seattle"},
    {"country": "USA", "region": "Illinois", "city": "Chicago"},
    {"country": "USA", "region": "Texas", "city": "Dallas"},
    {"country": "USA", "region": "Florida", "city": "Miami"},
    {"country": "USA", "region": "Arizona", "city": "Phoenix"},
    {"country": "USA", "region": "Georgia", "city": "Atlanta"},
    {"country": "USA", "region": "District of Columbia", "city": "Washington"},
]


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


def _build_payload(page: int, *, query: str, limit: int) -> Dict[str, Any]:
    return {
        "limit": limit,
        "page": page,
        "params": {
            "query": query,
            "location": DEFAULT_LOCATIONS,
        },
    }


async def _fetch_payload(api_key: str, payload: Dict[str, Any], listing_url: str) -> tuple[List[Any], Dict[str, Any]]:
    payload_json = json.dumps(payload, separators=(",", ":"))
    script = f"""
    (function() {{
      const payload = {payload_json};
      fetch("/api/loadSearchJobsResults", {{
        method: "POST",
        headers: {{
          "content-type": "application/json",
          "accept": "application/json",
          "x-csrf-token": "x"
        }},
        credentials: "include",
        body: JSON.stringify(payload)
      }})
        .then((res) => res.json())
        .then((data) => {{
          data.__source_url = window.location.href;
          data.__page = payload.page;
          data.__limit = payload.limit;
          const pre = document.createElement("pre");
          pre.id = "uber-jobs";
          pre.textContent = JSON.stringify(data);
          document.body.innerHTML = "";
          document.body.appendChild(pre);
        }})
        .catch((err) => {{
          const pre = document.createElement("pre");
          pre.id = "uber-jobs";
          pre.textContent = JSON.stringify({{error: String(err)}});
          document.body.innerHTML = "";
          document.body.appendChild(pre);
        }});
    }})();"""
    params: Dict[str, Any] = {
        "return_format": ["raw_html"],
        "metadata": True,
        "request": "chrome",
        "follow_redirects": True,
        "redirect_policy": "Loose",
        "external_domains": ["*"],
        "preserve_host": True,
        "limit": 1,
        "execution_scripts": {"*": script},
        "wait_for": {
            "selector": {
                "selector": "#uber-jobs",
                "timeout": {"secs": 20, "nanos": 0},
            },
            "idle_network0": {"timeout": {"secs": 5, "nanos": 0}},
        },
    }
    async with AsyncSpider(api_key=api_key) as client:
        response = await _collect_response(
            client.scrape_url(
                listing_url,
                params=params,
                stream=False,
                content_type="application/json",
            )
        )
    return response, {"endpoint": "/scrape", "url": listing_url, "params": params, "listingPayload": payload}


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Uber careers API fixtures via SpiderCloud.")
    parser.add_argument("--out-dir", required=True, help="Output directory for fixtures")
    parser.add_argument("--pages", type=int, default=3, help="Number of pages to fetch")
    parser.add_argument("--start-page", type=int, default=0, help="Zero-based page offset to start from")
    parser.add_argument("--limit", type=int, default=10, help="Page size limit")
    parser.add_argument("--query", default=DEFAULT_QUERY, help="Search query")
    args = parser.parse_args()

    api_key = _load_api_key()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    base_url = "https://www.uber.com/us/en/careers/list"
    base_query = f"query={args.query}"
    location_params = "&".join(
        [
            "location=" + loc.replace(" ", "%20")
            for loc in [
                "USA-California-San Francisco",
                "USA-California-Los Angeles",
                "USA-California-Sunnyvale",
                "USA-California-Culver City",
                "USA-New York-New York",
                "USA-Washington-Seattle",
                "USA-Illinois-Chicago",
                "USA-Texas-Dallas",
                "USA-Florida-Miami",
                "USA-Arizona-Phoenix",
                "USA-Georgia-Atlanta",
                "USA-District of Columbia-Washington",
            ]
        ]
    )

    for page in range(args.start_page, args.start_page + args.pages):
        payload = _build_payload(page, query=args.query, limit=args.limit)
        page_param = "" if page == 0 else f"&page={page}"
        listing_url = f"{base_url}?{base_query}&{location_params}{page_param}"
        response, request_meta = await _fetch_payload(api_key, payload, listing_url)
        status = _extract_status(response)
        snippet = _extract_snippet(response)
        out_path = out_dir / f"spidercloud_uber_careers_api_page_{page + 1}.json"
        fixture = {"request": request_meta, "response": response}
        out_path.write_text(json.dumps(fixture, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"page": page + 1, "status": status, "out": str(out_path)}, indent=2))
        if snippet:
            print(snippet.replace("\n", " "))


if __name__ == "__main__":
    asyncio.run(main())
