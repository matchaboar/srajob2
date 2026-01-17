from __future__ import annotations

import argparse
import asyncio
import orjson
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
    parser = argparse.ArgumentParser(description="Dump SpiderCloud POST response payloads.")
    parser.add_argument("url", help="URL to POST to")
    parser.add_argument("--body-json", help="JSON payload string")
    parser.add_argument("--out", required=True, help="Path to write raw response JSON")
    parser.add_argument("--request", default="http", help="SpiderCloud request type")
    parser.add_argument("--return-format", default="raw", help="SpiderCloud return_format value")
    parser.add_argument("--method", default="POST", help="HTTP method to use")
    args = parser.parse_args()

    load_dotenv()
    load_dotenv("job_board_application/.env.production", override=False)
    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        raise SystemExit("SPIDER_API_KEY (or SPIDER_KEY) is not set in environment/.env")

    params: Dict[str, Any] = {
        "return_format": [args.return_format],
        "metadata": True,
        "request": args.request,
        "method": args.method,
        "follow_redirects": True,
        "redirect_policy": "Loose",
        "external_domains": ["*"],
        "preserve_host": True,
        "limit": 1,
        "headers": {"Content-Type": "application/json", "Accept": "application/json"},
    }
    if args.body_json:
        params["body"] = args.body_json

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
    out_path.write_text(
        orjson.dumps(response, option=orjson.OPT_INDENT_2).decode("utf-8"),
        encoding="utf-8",
    )
    print(
        orjson.dumps(
            {"saved": str(out_path), "items": len(response)},
            option=orjson.OPT_INDENT_2,
        ).decode("utf-8")
    )


if __name__ == "__main__":
    asyncio.run(main())
