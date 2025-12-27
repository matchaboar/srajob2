from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from spider import AsyncSpider

ROOT = Path(__file__).resolve().parents[1]
FIXTURES_DIR = ROOT / "tests" / "job_scrape_application" / "workflows" / "fixtures"

WIDGETS_URL = "https://careers.adobe.com/widgets"
REF_NUM = "ADOBUS"
PAGE_ID = "page15"
DETAIL_PAGE_ID = "page21-ds"
PAGE_NAME = "search-results"
DETAIL_PAGE_NAME = "job-detail"

DEFAULT_KEYWORDS = "engineer"
DEFAULT_PAGE_SIZE = 100

STATES = [
    "California",
    "Colorado",
    "Delaware",
    "Massachusetts",
    "Minnesota",
    "New York",
    "Ontario",
    "Oregon",
    "Utah",
]


def build_refine_search_payload(
    *,
    keywords: str,
    states: List[str],
    from_offset: int,
    size: int,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "lang": "en_us",
        "deviceType": "desktop",
        "country": "us",
        "pageName": PAGE_NAME,
        "refNum": REF_NUM,
        "siteType": "external",
        "pageId": PAGE_ID,
        "ddoKey": "refineSearch",
        "keywords": keywords,
        "global": True,
        "size": size,
        "from": from_offset,
        "all_fields": ["city", "state", "category", "type", "orgFunction", "country"],
        "selected_fields": {"state": states},
    }
    return payload


def build_job_detail_payload(job_seq_no: str) -> Dict[str, Any]:
    return {
        "lang": "en_us",
        "deviceType": "desktop",
        "country": "us",
        "pageName": DETAIL_PAGE_NAME,
        "refNum": REF_NUM,
        "siteType": "external",
        "pageId": DETAIL_PAGE_ID,
        "ddoKey": "jobDetail",
        "jobSeqNo": job_seq_no,
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


def _spider_params(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "request": "http",
        "return_format": ["raw"],
        "metadata": True,
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        "follow_redirects": True,
        "redirect_policy": "Loose",
        "external_domains": ["*"],
        "preserve_host": True,
        "limit": 1,
        "method": "POST",
        "body": json.dumps(payload),
    }


def _write_fixture(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _extract_payload_from_response(response: List[Any]) -> Optional[Dict[str, Any]]:
    for event in response:
        if isinstance(event, dict):
            for key in ("content", "raw", "text", "result"):
                value = event.get(key)
                if isinstance(value, str) and value.strip().startswith("{"):
                    try:
                        parsed = json.loads(value)
                    except Exception:
                        parsed = None
                    if isinstance(parsed, dict):
                        return parsed
            if "refineSearch" in event or "jobDetail" in event:
                return event
    return None


async def _fetch_payload(api_key: str, payload: Dict[str, Any]) -> List[Any]:
    async with AsyncSpider(api_key=api_key) as client:
        response = await _collect_response(
            client.scrape_url(
                WIDGETS_URL,
                params=_spider_params(payload),
                stream=False,
                content_type="application/json",
            )
        )
    return response


def _select_job_seq_no(parsed: Dict[str, Any]) -> Optional[str]:
    refine = parsed.get("refineSearch") if isinstance(parsed, dict) else None
    if isinstance(refine, dict):
        data = refine.get("data") if isinstance(refine.get("data"), dict) else None
    else:
        data = parsed.get("data") if isinstance(parsed.get("data"), dict) else None
    jobs = data.get("jobs") if isinstance(data, dict) else None
    if isinstance(jobs, list):
        for job in jobs:
            if not isinstance(job, dict):
                continue
            job_seq = job.get("jobSeqNo") or job.get("jobSeqId") or job.get("jobSequence")
            if isinstance(job_seq, (str, int)):
                job_seq_str = str(job_seq).strip()
                if job_seq_str:
                    return job_seq_str
    return None


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Adobe Phenom SpiderCloud fixtures.")
    parser.add_argument("--keywords", default=DEFAULT_KEYWORDS)
    parser.add_argument("--size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--from-offsets", default="0,100,200")
    args = parser.parse_args()

    api_key = _load_api_key()
    fetched_at = datetime.now(timezone.utc).isoformat()

    offsets = [int(val) for val in args.from_offsets.split(",") if val.strip()]

    pages: List[Dict[str, Any]] = []
    for offset in offsets:
        payload = build_refine_search_payload(
            keywords=args.keywords,
            states=STATES,
            from_offset=offset,
            size=args.size,
        )
        response = await _fetch_payload(api_key, payload)
        pages.append({"payload": payload, "response": response})

    for idx, page in enumerate(pages, start=1):
        fixture = {
            "fetched_at": fetched_at,
            "source_url": WIDGETS_URL,
            "payload": page["payload"],
            "response": page["response"],
        }
        _write_fixture(FIXTURES_DIR / f"adobe_refine_search_page_{idx}.json", fixture)

    first_payload = _extract_payload_from_response(pages[0]["response"])
    if not first_payload:
        raise SystemExit("No JSON payload found in refineSearch response.")

    job_seq_no = _select_job_seq_no(first_payload)
    if not job_seq_no:
        raise SystemExit("No jobSeqNo found in refineSearch response.")

    detail_payload = build_job_detail_payload(job_seq_no)
    detail_response = await _fetch_payload(api_key, detail_payload)

    detail_fixture = {
        "fetched_at": fetched_at,
        "source_url": WIDGETS_URL,
        "payload": detail_payload,
        "response": detail_response,
    }
    _write_fixture(FIXTURES_DIR / "adobe_job_detail.json", detail_fixture)

    print("Wrote Adobe fixtures to", FIXTURES_DIR)


if __name__ == "__main__":
    asyncio.run(main())
