#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import html
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from dotenv import load_dotenv
from spider import AsyncSpider

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from job_scrape_application.workflows.site_handlers.netflix import NetflixHandler  # noqa: E402

_convex_query = None


SPIDER_PARAMS: Dict[str, Any] = {
    "return_format": ["raw_html"],
    "metadata": True,
    "request": "chrome",
    "follow_redirects": True,
    "redirect_policy": "Loose",
    "external_domains": ["*"],
    "preserve_host": True,
    "limit": 1,
}


def _load_env(target_env: str) -> None:
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production")
    else:
        load_dotenv(CONVEX_DIR / ".env")
        load_dotenv(CONVEX_DIR / ".env.local", override=False)
    load_dotenv()


def _add_query_param(url: str, key: str, value: str) -> str:
    try:
        parsed = urlparse(url)
    except Exception:
        return url
    query_items = parse_qsl(parsed.query, keep_blank_values=True)
    filtered = [(k, v) for k, v in query_items if k != key]
    filtered.append((key, value))
    new_query = urlencode(filtered, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


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


def _gather_strings(node: Any) -> Iterable[str]:
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for val in node.values():
            yield from _gather_strings(val)
    elif isinstance(node, list):
        for val in node:
            yield from _gather_strings(val)


def _find_jobs_payload(node: Any) -> Optional[Dict[str, Any]]:
    if isinstance(node, dict) and isinstance(node.get("jobs"), list):
        return node
    if isinstance(node, dict) and isinstance(node.get("positions"), list):
        return node
    if isinstance(node, dict):
        for val in node.values():
            found = _find_jobs_payload(val)
            if found:
                return found
    if isinstance(node, list):
        for val in node:
            found = _find_jobs_payload(val)
            if found:
                return found
    return None


def _extract_payload(events: List[Any]) -> Optional[Dict[str, Any]]:
    found = _find_jobs_payload(events)
    if found:
        return found
    for text in _gather_strings(events):
        try:
            parsed = json.loads(text)
        except Exception:
            continue
        if isinstance(parsed, str):
            try:
                parsed = json.loads(parsed)
            except Exception:
                pass
        found = _find_jobs_payload(parsed)
        if found:
            return found
    for text in _gather_strings(events):
        html_payload = _extract_json_from_html(text)
        if html_payload:
            return html_payload
    return None


def _extract_json_from_html(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    if "<pre" not in text.lower():
        return None
    match = re.search(r"<pre[^>]*>(?P<content>.*?)</pre>", text, flags=re.IGNORECASE | re.DOTALL)
    content = match.group("content") if match else text
    content = html.unescape(content).strip()
    if not content:
        return None
    candidate = content
    if not candidate.lstrip().startswith("{"):
        brace_match = re.search(r"{.*}", candidate, flags=re.DOTALL)
        if brace_match:
            candidate = brace_match.group(0)
    try:
        parsed = json.loads(candidate)
    except Exception:
        try:
            unescaped = candidate.encode("utf-8", errors="ignore").decode("unicode_escape")
            parsed = json.loads(unescaped)
        except Exception:
            return None
    return _find_jobs_payload(parsed) if parsed is not None else None


def _summarize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    positions = payload.get("positions") if isinstance(payload, dict) else None
    return {
        "jobsCount": len(jobs) if isinstance(jobs, list) else 0,
        "positionsCount": len(positions) if isinstance(positions, list) else 0,
        "count": payload.get("count") if isinstance(payload, dict) else None,
        "payloadKeys": sorted(payload.keys()) if isinstance(payload, dict) else [],
    }


def _pick_netflix_site(sites: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for site in sites:
        if not isinstance(site, dict):
            continue
        if str(site.get("type") or "").lower() == "netflix":
            return site
    for site in sites:
        url = site.get("url")
        name = site.get("name")
        if isinstance(url, str) and "netflix" in url.lower():
            return site
        if isinstance(name, str) and "netflix" in name.lower():
            return site
    return None


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare live Netflix listing URLs vs seen URLs in Convex."
    )
    parser.add_argument("--env", choices=["dev", "prod"], default="prod")
    parser.add_argument("--site-id", help="override site id from Convex")
    parser.add_argument("--url", help="override listing URL")
    parser.add_argument("--limit-sample", type=int, default=10)
    args = parser.parse_args()

    _load_env(args.env)
    global _convex_query
    from job_scrape_application.services import convex_query as _cq  # noqa: E402

    _convex_query = _cq
    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        raise SystemExit("SPIDER_API_KEY (or SPIDER_KEY) is not set in environment/.env")

    sites = await _convex_query("router:listSites", {"enabledOnly": False}) or []
    site: Optional[Dict[str, Any]] = None
    if args.site_id:
        for row in sites:
            if isinstance(row, dict) and str(row.get("_id")) == args.site_id:
                site = row
                break
    if site is None:
        site = _pick_netflix_site(sites)
    if site is None and not args.url:
        raise SystemExit("No Netflix site found in Convex. Pass --url to override.")

    listing_url = args.url or site.get("url")  # type: ignore[assignment]
    if not isinstance(listing_url, str) or not listing_url:
        raise SystemExit("Listing URL is missing.")

    handler = NetflixHandler()
    api_url = handler.get_listing_api_uri(listing_url) or listing_url
    api_url = _add_query_param(api_url, "includeCompensation", "false")

    async with AsyncSpider(api_key=api_key) as client:
        response = await _collect_response(
            client.scrape_url(
                api_url,
                params=SPIDER_PARAMS,
                stream=False,
                content_type="application/json",
            )
        )

    payload = _extract_payload(response)
    if not payload:
        raise SystemExit("Unable to extract jobs/positions payload from live SpiderCloud response.")

    job_urls = handler.get_links_from_json(payload)
    pagination_urls = handler.get_pagination_urls_from_json(payload, api_url)

    seen_urls: List[str] = []
    if site and isinstance(site.get("url"), str):
        seen_payload = await _convex_query(
            "router:listSeenJobUrlsForSite",
            {"sourceUrl": site.get("url"), "pattern": site.get("pattern")},
        )
        seen_urls = seen_payload.get("urls", []) if isinstance(seen_payload, dict) else []
        seen_urls = [u for u in seen_urls if isinstance(u, str)]

    seen_set = set(seen_urls)
    new_job_urls = [u for u in job_urls if u not in seen_set]
    new_pagination_urls = [u for u in pagination_urls if u not in seen_set]

    report = {
        "meta": {
            "env": args.env,
            "generatedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
        },
        "site": {
            "id": site.get("_id") if site else None,
            "name": site.get("name") if site else None,
            "url": site.get("url") if site else listing_url,
            "pattern": site.get("pattern") if site else None,
        },
        "api": {
            "listingUrl": listing_url,
            "apiUrl": api_url,
            "payloadSummary": _summarize_payload(payload),
            "jobUrls": len(job_urls),
            "paginationUrls": len(pagination_urls),
        },
        "seen": {
            "total": len(seen_urls),
            "newJobUrls": len(new_job_urls),
            "newPaginationUrls": len(new_pagination_urls),
            "newJobSamples": new_job_urls[: args.limit_sample],
            "newPaginationSamples": new_pagination_urls[: args.limit_sample],
        },
    }

    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
