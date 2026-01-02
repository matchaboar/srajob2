from __future__ import annotations

import json
from pathlib import Path

import pytest

from job_scrape_application.workflows.helpers.scrape_utils import (
    MAX_JOB_DESCRIPTION_CHARS,
    trim_scrape_for_convex,
)


FIXTURE_PATH = Path(
    "tests/job_scrape_application/workflows/fixtures/spidercloud_batch_50.json"
)
MAX_TEMPORAL_PAYLOAD_BYTES = 8 * 1024 * 1024


def _build_scrape_payload(results: list[dict], urls: list[str]) -> dict:
    normalized = []
    raw_items = []
    for row in results:
        if not isinstance(row, dict):
            continue
        if not row.get("ok"):
            continue
        response = row.get("response")
        if not isinstance(response, dict):
            continue
        if isinstance(response.get("normalized"), dict):
            normalized.append(response["normalized"])
        if isinstance(response.get("raw"), dict):
            raw_items.append(response["raw"])
    return {
        "sourceUrl": urls[0] if urls else "",
        "startedAt": 0,
        "completedAt": 1,
        "items": {
            "normalized": normalized,
            "raw": raw_items,
            "seedUrls": urls,
            "provider": "spidercloud",
        },
        "provider": "spidercloud",
        "subUrls": urls,
    }


def test_spidercloud_batch_fixture_trim_limits() -> None:
    if not FIXTURE_PATH.exists():
        pytest.skip("Missing spidercloud batch fixture; run agent_scripts/measure_spidercloud_batch.py")
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        payload = payload.get("response")
    results = payload.get("results", [])
    urls = payload.get("meta", {}).get("urls", []) or []
    scrape_payload = _build_scrape_payload(results, urls)

    trimmed = trim_scrape_for_convex(scrape_payload)
    trimmed_bytes = len(json.dumps(trimmed, ensure_ascii=False))

    assert trimmed_bytes <= MAX_TEMPORAL_PAYLOAD_BYTES

    items = trimmed.get("items", {})
    normalized = items.get("normalized", [])
    assert isinstance(normalized, list)
    assert len(normalized) <= 400

    for row in normalized:
        if not isinstance(row, dict):
            continue
        desc = row.get("description") or row.get("job_description") or ""
        if isinstance(desc, str):
            assert len(desc) <= MAX_JOB_DESCRIPTION_CHARS


def test_spidercloud_trim_reduces_large_payload() -> None:
    normalized = []
    for i in range(450):
        normalized.append(
            {
                "url": f"https://example.com/jobs/{i}",
                "title": "Engineer",
                "description": "x" * (MAX_JOB_DESCRIPTION_CHARS + 500),
            }
        )
    scrape_payload = {
        "sourceUrl": "https://example.com",
        "startedAt": 0,
        "completedAt": 1,
        "items": {"normalized": normalized, "raw": [{"markdown": "y" * 20000}]},
        "provider": "spidercloud",
    }

    raw_bytes = len(json.dumps(scrape_payload, ensure_ascii=False))
    trimmed = trim_scrape_for_convex(scrape_payload)
    trimmed_bytes = len(json.dumps(trimmed, ensure_ascii=False))

    assert trimmed_bytes < raw_bytes
    items = trimmed.get("items", {})
    assert len(items.get("normalized", [])) <= 400
