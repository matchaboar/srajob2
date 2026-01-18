from __future__ import annotations

import orjson
from pathlib import Path

import pytest

from job_scrape_application.workflows.helpers.scrape_utils import (
    MAX_JOB_DESCRIPTION_CHARS,
    trim_scrape_for_convex,
)
from job_scrape_application.workflows.scrapers import spidercloud_scraper


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
    payload = orjson.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        payload = payload.get("response")
    results = payload.get("results", [])
    urls = payload.get("meta", {}).get("urls", []) or []
    scrape_payload = _build_scrape_payload(results, urls)

    trimmed = trim_scrape_for_convex(scrape_payload)
    trimmed_bytes = len(orjson.dumps(trimmed))

    assert trimmed_bytes <= MAX_TEMPORAL_PAYLOAD_BYTES

    items = trimmed.get("items", {})
    normalized = items.get("normalized", [])
    assert isinstance(normalized, list)
    assert len(normalized) <= 400

    for row in normalized:
        if not isinstance(row, dict):
            continue
        assert set(row.keys()) == {"url"}
        assert isinstance(row.get("url"), str)

    normalized_sample = items.get("normalizedSample", [])
    assert isinstance(normalized_sample, list)
    assert len(normalized_sample) <= 10


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

    raw_bytes = len(orjson.dumps(scrape_payload))
    trimmed = trim_scrape_for_convex(scrape_payload)
    trimmed_bytes = len(orjson.dumps(trimmed))

    assert trimmed_bytes < raw_bytes
    items = trimmed.get("items", {})
    assert len(items.get("normalized", [])) <= 400
    for row in items.get("normalized", []):
        if not isinstance(row, dict):
            continue
        assert set(row.keys()) == {"url"}


def test_spidercloud_failed_item_summary() -> None:
    failures = [
        {"url": "https://example.com/a", "reason": "timeout", "status": 504, "retryable": False},
        {"url": "https://example.com/b", "reason": "timeout", "status": 504, "retryable": False},
        {"url": "https://example.com/c", "reason": "captcha_failed", "retryable": False},
        {"url": "https://example.com/d", "reason": "captcha_failed", "status": 403, "retryable": False},
    ]

    summary = spidercloud_scraper._summarize_failed_items(failures, sample_limit=2)

    assert summary["failedCount"] == 4
    assert summary["reasonCounts"] == {"captcha_failed": 2, "timeout": 2}
    assert summary["statusCounts"] == {"403": 1, "504": 2}
    assert summary["retryableCount"] == 0
    assert summary["sampleUrls"] == ["https://example.com/a", "https://example.com/b"]
    assert summary["sampleReasons"] == ["timeout", "timeout"]
