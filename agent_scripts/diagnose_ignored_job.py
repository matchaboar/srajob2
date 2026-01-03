#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_env(target_env: str) -> None:
    load_dotenv()
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production", override=True)
    else:
        load_dotenv(CONVEX_DIR / ".env", override=False)
        load_dotenv(CONVEX_DIR / ".env.local", override=False)


def _iter_strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for val in value.values():
            yield from _iter_strings(val)
    elif isinstance(value, list):
        for val in value:
            yield from _iter_strings(val)


def _row_matches(row: Dict[str, Any], needle: str) -> bool:
    lowered = needle.lower()
    for text in _iter_strings(row):
        if lowered in text.lower():
            return True
    return False


def _summarize_ignored(row: Dict[str, Any]) -> Dict[str, Any]:
    details = row.get("details") if isinstance(row.get("details"), dict) else None
    payload: Dict[str, Any] = {
        "id": row.get("_id"),
        "url": row.get("url"),
        "sourceUrl": row.get("sourceUrl"),
        "company": row.get("company"),
        "provider": row.get("provider"),
        "workflowName": row.get("workflowName"),
        "reason": row.get("reason"),
        "createdAt": row.get("createdAt"),
    }
    if details:
        payload["details"] = details
    return {k: v for k, v in payload.items() if v is not None}


def _summarize_error(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
        "id": row.get("_id"),
        "jobId": row.get("jobId"),
        "sourceUrl": row.get("sourceUrl"),
        "siteId": row.get("siteId"),
        "event": row.get("event"),
        "status": row.get("status"),
        "error": row.get("error"),
        "createdAt": row.get("createdAt"),
        "metadata": row.get("metadata"),
    }
    return {k: v for k, v in payload.items() if v is not None}


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find ignored_jobs and scrape_errors rows matching a URL substring."
    )
    parser.add_argument("url", help="URL (or substring) to search for")
    parser.add_argument(
        "--env",
        default="prod",
        choices=("dev", "prod"),
        help="Convex environment to query (default: prod).",
    )
    parser.add_argument(
        "--ignored-limit",
        type=int,
        default=400,
        help="Max ignored_jobs rows to fetch (default: 400).",
    )
    parser.add_argument(
        "--errors-limit",
        type=int,
        default=200,
        help="Max scrape_errors rows to fetch (default: 200).",
    )
    parser.add_argument(
        "--out",
        help="Optional output JSON path (writes full matches).",
    )
    args = parser.parse_args()

    _load_env(args.env)
    from job_scrape_application.services import convex_query  # noqa: E402

    ignored_rows = await convex_query("router:listIgnoredJobs", {"limit": args.ignored_limit})
    if not isinstance(ignored_rows, list):
        ignored_rows = []
    ignored_matches = [row for row in ignored_rows if _row_matches(row, args.url)]

    error_rows = await convex_query("router:listScrapeErrors", {"limit": args.errors_limit})
    if not isinstance(error_rows, list):
        error_rows = []
    error_matches = [row for row in error_rows if _row_matches(row, args.url)]

    summary = {
        "query": args.url,
        "env": args.env,
        "ignored": {
            "count": len(ignored_matches),
            "items": [_summarize_ignored(row) for row in ignored_matches],
        },
        "scrape_errors": {
            "count": len(error_matches),
            "items": [_summarize_error(row) for row in error_matches],
        },
    }

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
