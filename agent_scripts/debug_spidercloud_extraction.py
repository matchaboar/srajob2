from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Dict, List

from dotenv import load_dotenv
from spider import AsyncSpider

URL = "https://example.com"

SCRIPT = "document.body.innerHTML = '<div id=\"spidercloud-result\">hello</div>';"


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


async def main() -> None:
    api_key = _load_api_key()
    params: Dict[str, Any] = {
        "request": "chrome",
        "return_format": ["raw_html"],
        "metadata": True,
        "follow_redirects": True,
        "redirect_policy": "Loose",
        "external_domains": ["*"],
        "preserve_host": True,
        "limit": 1,
        "execution_scripts": {"*": SCRIPT},
        "css_extraction_map": {
            "*": [
                {"name": "result", "selectors": ["#spidercloud-result"]}
            ]
        },
    }

    async with AsyncSpider(api_key=api_key) as client:
        response = await _collect_response(
            client.scrape_url(
                URL,
                params=params,
                stream=False,
                content_type="application/json",
            )
        )

    print(json.dumps(response, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
