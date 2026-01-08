from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv
from spider import AsyncSpider


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


async def main() -> None:
    parser = argparse.ArgumentParser(description="Dump SpiderCloud response payloads for debugging.")
    parser.add_argument("url", help="URL to scrape")
    parser.add_argument("--out", required=True, help="Path to write raw response JSON")
    parser.add_argument("--return-format", help="SpiderCloud return_format value")
    parser.add_argument("--request", default="chrome", help="SpiderCloud request type")
    parser.add_argument("--return-cookies", action="store_true", help="Include response cookies")
    parser.add_argument("--return-headers", action="store_true", help="Include response headers")
    parser.add_argument(
        "--return-page-links",
        action="store_true",
        help="Include page_links from SpiderCloud (DOM link extraction).",
    )
    parser.add_argument("--cookies", help="Cookie header string to pass through SpiderCloud")
    parser.add_argument("--track-responses", action="store_true", help="Track network responses")
    parser.add_argument("--track-requests", action="store_true", help="Track network requests")
    parser.add_argument("--track-automation", action="store_true", help="Track automation events")
    parser.add_argument(
        "--use-handler-config",
        action="store_true",
        help="Merge SpiderCloud params from the site handler for the URL.",
    )
    parser.add_argument(
        "--wait-for-selector",
        help="CSS selector to wait for before returning content",
    )
    parser.add_argument(
        "--wait-for-timeout-secs",
        type=int,
        default=20,
        help="Timeout in seconds for wait-for selector (default: 20)",
    )
    args = parser.parse_args()

    load_dotenv()
    load_dotenv("job_board_application/.env.production", override=False)
    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        raise SystemExit("SPIDER_API_KEY (or SPIDER_KEY) is not set in environment/.env")

    params: Dict[str, Any] = {}
    if args.use_handler_config:
        try:
            from job_scrape_application.workflows.site_handlers import get_site_handler
        except Exception as exc:
            raise SystemExit(f"Could not load site handlers: {exc}") from exc
        handler = get_site_handler(args.url)
        if handler:
            params.update(handler.get_spidercloud_config(args.url))
    if args.return_format is None:
        if "return_format" not in params:
            params["return_format"] = ["raw_html"]
    else:
        params["return_format"] = [args.return_format]
    params.setdefault("request", args.request)
    params.setdefault("follow_redirects", True)
    params.setdefault("redirect_policy", "Loose")
    params.setdefault("external_domains", ["*"])
    params.setdefault("preserve_host", True)
    params.setdefault("metadata", True)
    params.setdefault("limit", 1)
    if args.return_cookies:
        params["return_cookies"] = True
    if args.return_headers:
        params["return_headers"] = True
    if args.return_page_links:
        params["return_page_links"] = True
    if args.cookies:
        params["cookies"] = args.cookies
    if args.track_responses or args.track_requests or args.track_automation:
        params["event_tracker"] = {
            "responses": bool(args.track_responses),
            "requests": bool(args.track_requests),
            "automation": bool(args.track_automation),
        }
    if args.wait_for_selector:
        params["wait_for"] = {
            "selector": {
                "selector": args.wait_for_selector,
                "timeout": {"secs": args.wait_for_timeout_secs, "nanos": 0},
            },
            "idle_network0": {"timeout": {"secs": 5, "nanos": 0}},
        }

    async with AsyncSpider(api_key=api_key) as client:
        response = await _collect_response(
            client.scrape_url(
                args.url,
                params=params,
                stream=False,
                content_type="application/json",
            )
        )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(response, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"saved": str(out_path), "items": len(response)}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
