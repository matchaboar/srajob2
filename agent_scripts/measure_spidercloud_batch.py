#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from job_scrape_application.services import convex_query  # noqa: E402

try:
    from spider import AsyncSpider  # type: ignore
except Exception:
    AsyncSpider = None  # type: ignore[assignment]


def _load_runtime_batch_config(env: str) -> Dict[str, int]:
    path = REPO_ROOT / "job_scrape_application" / "config" / env / "runtime.yaml"
    data: Dict[str, Any] = {}
    if path.exists():
        try:
            raw = yaml.safe_load(path.read_text()) or {}
            if isinstance(raw, dict):
                data = raw
        except Exception:
            data = {}
    return {
        "batch_size": int(data.get("spidercloud_job_details_batch_size", 50)),
        "concurrency": int(data.get("spidercloud_job_details_concurrency", 4)),
        "timeout_seconds": int(data.get("spidercloud_http_timeout_seconds", 900)),
    }


async def _collect_queue_urls(provider: str, limit: int) -> List[str]:
    urls: List[str] = []
    seen = set()
    for status in ("pending", "processing"):
        rows = await convex_query(
            "router:listQueuedScrapeUrls",
            {"provider": provider, "status": status, "limit": 500},
        )
        for row in rows or []:
            url = row.get("url")
            if not isinstance(url, str):
                continue
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)
            if len(urls) >= limit:
                return urls
    return urls


async def _scrape_batch(
    urls: List[str],
    *,
    api_key: str,
    concurrency: int,
    timeout_seconds: int,
    return_format: str,
) -> Dict[str, Any]:
    if AsyncSpider is None:
        return {"error": "spider client not available"}
    if not urls:
        return {"error": "no urls to test"}

    async def _collect_response(response: Any) -> Any:
        if hasattr(response, "__aiter__"):
            items = []
            async for item in response:
                items.append(item)
            return items
        if hasattr(response, "__await__"):
            return await response
        return response

    semaphore = asyncio.Semaphore(max(1, min(concurrency, len(urls))))
    started = time.time()

    async with AsyncSpider(api_key=api_key) as client:
        async def _fetch(url: str) -> Dict[str, Any]:
            async with semaphore:
                result: Dict[str, Any] = {
                    "url": url,
                    "ok": False,
                    "elapsed_seconds": None,
                    "size_bytes": None,
                    "error": None,
                    "response": None,
                }
                t0 = time.time()
                try:
                    params = {
                        "return_format": [return_format],
                        "metadata": True,
                        "request": "chrome",
                        "follow_redirects": True,
                        "redirect_policy": "Loose",
                        "external_domains": ["*"],
                        "preserve_host": True,
                        "limit": 1,
                    }
                    coro = client.scrape_url(
                        url,
                        params=params,
                        stream=False,
                        content_type="application/json",
                    )
                    response = await asyncio.wait_for(
                        _collect_response(coro),
                        timeout=timeout_seconds,
                    )
                    payload = json.dumps(response, ensure_ascii=False)
                    result["ok"] = True
                    result["elapsed_seconds"] = round(time.time() - t0, 2)
                    result["size_bytes"] = len(payload)
                    result["response"] = response
                except Exception as exc:  # noqa: BLE001
                    result["elapsed_seconds"] = round(time.time() - t0, 2)
                    result["error"] = str(exc)
                return result

        tasks = [asyncio.create_task(_fetch(url)) for url in urls]
        results = await asyncio.gather(*tasks)

    elapsed = round(time.time() - started, 2)
    sizes = [r["size_bytes"] for r in results if isinstance(r.get("size_bytes"), int)]
    errors = [r for r in results if r.get("error")]
    return {
        "meta": {
            "count": len(results),
            "elapsed_seconds": elapsed,
            "avg_size_bytes": int(sum(sizes) / len(sizes)) if sizes else 0,
            "max_size_bytes": max(sizes) if sizes else 0,
            "min_size_bytes": min(sizes) if sizes else 0,
            "error_count": len(errors),
            "error_samples": errors[:5],
        },
        "results": results,
    }


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure SpiderCloud batch scrape timings/sizes and write fixtures."
    )
    parser.add_argument("--env", default="prod", choices=["dev", "prod"])
    parser.add_argument("--provider", default="spidercloud")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--return-format", default="commonmark", choices=["commonmark", "raw_html"])
    parser.add_argument("--output", default="tests/job_scrape_application/workflows/fixtures/spidercloud_batch_50.json")
    parser.add_argument("--timeout-seconds", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=None)
    args = parser.parse_args()

    config = _load_runtime_batch_config(args.env)
    batch_size = min(args.limit, config["batch_size"])
    if args.timeout_seconds is not None and args.timeout_seconds > 0:
        config["timeout_seconds"] = args.timeout_seconds
    if args.concurrency is not None and args.concurrency > 0:
        config["concurrency"] = args.concurrency
    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        raise SystemExit("SPIDER_API_KEY (or SPIDER_KEY) is not set in environment/.env")

    urls = await _collect_queue_urls(args.provider, batch_size)
    if len(urls) < batch_size:
        print(f"Warning: only collected {len(urls)} urls (requested {batch_size}).")

    payload = await _scrape_batch(
        urls,
        api_key=api_key,
        concurrency=config["concurrency"],
        timeout_seconds=config["timeout_seconds"],
        return_format=args.return_format,
    )
    payload["meta"]["provider"] = args.provider
    payload["meta"]["env"] = args.env
    payload["meta"]["return_format"] = args.return_format
    payload["meta"]["urls"] = urls

    output_path = Path(args.output)
    _write_json(output_path, payload)

    by_error = Counter([str(r.get("error") or "") for r in payload.get("results", []) if r.get("error")])
    print(
        json.dumps(
            {
                "output": str(output_path),
                "count": payload.get("meta", {}).get("count"),
                "elapsed_seconds": payload.get("meta", {}).get("elapsed_seconds"),
                "avg_size_bytes": payload.get("meta", {}).get("avg_size_bytes"),
                "max_size_bytes": payload.get("meta", {}).get("max_size_bytes"),
                "error_count": payload.get("meta", {}).get("error_count"),
                "error_samples": payload.get("meta", {}).get("error_samples"),
                "by_error": dict(by_error),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
