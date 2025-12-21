#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from job_scrape_application.services import convex_query  # noqa: E402


def _parse_statuses(raw: str | None) -> List[str]:
    if not raw:
        return ["pending"]
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts or ["pending"]


async def _fetch_rows(provider: str | None, status: str | None, limit: int) -> List[Dict[str, Any]]:
    args: Dict[str, Any] = {"limit": limit}
    if provider:
        args["provider"] = provider
    if status:
        args["status"] = status
    rows = await convex_query("router:listQueuedScrapeUrls", args)
    return rows or []


def _unique_rows(rows: Iterable[Dict[str, Any]], *, max_rows: int) -> List[Dict[str, Any]]:
    unique: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        url = row.get("url")
        if not isinstance(url, str) or not url:
            continue
        if url in seen:
            continue
        seen.add(url)
        unique.append(row)
        if len(unique) >= max_rows:
            break
    return unique


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Export Convex scrape_url_queue rows into a fixture.")
    parser.add_argument("--provider", default="spidercloud", help="scrape_url_queue provider filter")
    parser.add_argument(
        "--statuses",
        default="pending",
        help="comma-separated status list (default: pending)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=120,
        help="max total rows to export",
    )
    parser.add_argument(
        "--per-status-limit",
        type=int,
        default=500,
        help="max rows to fetch per status (Convex cap is 500)",
    )
    parser.add_argument(
        "--output",
        default="tests/job_scrape_application/workflows/fixtures/scrape_queue_fixture.json",
    )
    parser.add_argument("--env", default=None, help="optional env label to include in metadata")
    args = parser.parse_args()

    statuses = _parse_statuses(args.statuses)
    collected: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()

    for status in statuses:
        rows = await _fetch_rows(args.provider, status, args.per_status_limit)
        for row in rows:
            url = row.get("url")
            if not isinstance(url, str) or not url:
                continue
            if url in seen_urls:
                continue
            seen_urls.add(url)
            collected.append(row)
            if len(collected) >= args.limit:
                break
        if len(collected) >= args.limit:
            break

    unique_rows = _unique_rows(collected, max_rows=args.limit)
    now_ms = int(time.time() * 1000)
    payload = {
        "meta": {
            "provider": args.provider,
            "statuses": statuses,
            "limit": args.limit,
            "count": len(unique_rows),
            "generatedAtMs": now_ms,
            "generatedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now_ms / 1000)),
            "env": args.env,
        },
        "rows": unique_rows,
    }

    output_path = Path(args.output)
    _write_json(output_path, payload)

    print(
        json.dumps(
            {
                "output": str(output_path),
                "count": len(unique_rows),
                "provider": args.provider,
                "statuses": statuses,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
