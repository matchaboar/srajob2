"""
Fetch live SpiderCloud scrape payloads and write them into our test fixtures.

Usage:
    uv run python scripts/fetch_spidercloud_fixtures.py

Requires:
    - SPIDER_API_KEY in .env (loaded via python-dotenv)
Outputs:
    - tests/job_scrape_application/workflows/fixtures/spidercloud_greenhouse_api_raw.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_greenhouse_api_commonmark.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_pinterest_marketing_commonmark.json
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Tuple
from urllib import request

from dotenv import load_dotenv

BASE_URL = "https://api.spider.cloud"

GH_JOB_URL = "https://boards-api.greenhouse.io/v1/boards/pinterest/jobs/5572858"
PINTEREST_MARKETING_URL = "https://www.pinterestcareers.com/jobs/?gh_jid=5572858"

FIXTURES: Tuple[Tuple[str, str, Dict[str, Any]], ...] = (
    (
        "spidercloud_greenhouse_api_raw.json",
        "/scrape",
        {"return_format": "raw_html", "url": GH_JOB_URL, "limit": 1},
    ),
    (
        "spidercloud_greenhouse_api_commonmark.json",
        "/scrape",
        {"return_format": "commonmark", "url": GH_JOB_URL, "limit": 1},
    ),
    (
        "spidercloud_pinterest_marketing_commonmark.json",
        "/scrape",
        {"return_format": "commonmark", "url": PINTEREST_MARKETING_URL, "limit": 1},
    ),
)

FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures")


def _post_json(endpoint: str, payload: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    url = BASE_URL + endpoint
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with request.urlopen(req, timeout=30) as resp:
        text = resp.read().decode("utf-8", errors="replace")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {"raw": text}


def main() -> None:
    load_dotenv()
    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        raise SystemExit("SPIDER_API_KEY (or SPIDER_KEY) is not set in environment/.env")

    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)

    for filename, endpoint, payload in FIXTURES:
        path = FIXTURE_DIR / filename
        try:
            print(f"Fetching {payload['url']} -> {filename} via {endpoint}")
            res = _post_json(endpoint, payload, api_key)
            path.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"  wrote {path} ({len(json.dumps(res))} bytes)")
        except Exception as exc:  # noqa: BLE001
            print(f"  failed to fetch {filename}: {exc}")


if __name__ == "__main__":
    main()
