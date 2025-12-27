from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv
from spider import AsyncSpider


def _load_script(args: argparse.Namespace) -> str:
    if args.script and args.script_file:
        raise SystemExit("Provide either --script or --script-file, not both.")
    if args.script:
        return args.script
    if args.script_file:
        return Path(args.script_file).read_text(encoding="utf-8")
    raise SystemExit("Provide --script or --script-file.")


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
    parser = argparse.ArgumentParser(
        description="Run SpiderCloud with an execution script injected into a page."
    )
    parser.add_argument("url", help="URL to scrape")
    parser.add_argument("--out", required=True, help="Path to write raw response JSON")
    parser.add_argument("--request", default="chrome", help="SpiderCloud request type")
    parser.add_argument("--return-format", default="raw_html", help="SpiderCloud return format")
    parser.add_argument(
        "--script",
        help="Inline JS to execute (mutually exclusive with --script-file)",
    )
    parser.add_argument(
        "--script-file",
        help="Path to JS file to execute (mutually exclusive with --script)",
    )
    parser.add_argument(
        "--script-path",
        default="*",
        help="Path key to use in execution_scripts map (default: '*')",
    )
    parser.add_argument(
        "--execution-key",
        default="exuecution_scripts",
        help="Request param key for execution scripts (default: exuecution_scripts)",
    )
    parser.add_argument(
        "--automation-eval",
        help="JS snippet to run via automation_scripts Evaluate step",
    )
    parser.add_argument(
        "--track-automation",
        action="store_true",
        help="Enable event_tracker.automation in SpiderCloud",
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

    script = _load_script(args)

    execution_key = args.execution_key.strip()
    if not execution_key:
        raise SystemExit("--execution-key must be a non-empty string.")

    params: Dict[str, Any] = {
        "return_format": [args.return_format],
        "metadata": True,
        "request": args.request,
        "follow_redirects": True,
        "redirect_policy": "Loose",
        "external_domains": ["*"],
        "preserve_host": True,
        "limit": 1,
        execution_key: {args.script_path: script},
    }
    if args.automation_eval:
        params["automation_scripts"] = {args.script_path: [{"Evaluate": args.automation_eval}]}
    if args.track_automation:
        params["event_tracker"] = {"automation": True, "requests": False, "responses": False}
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
