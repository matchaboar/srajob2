from __future__ import annotations

import json
from pathlib import Path

import pytest

from job_scrape_application.workflows import activities as acts


FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures")
PAGE_1 = FIXTURE_DIR / "spidercloud_confluent_jobs_page_1.json"
PAGE_2 = FIXTURE_DIR / "spidercloud_confluent_jobs_page_2.json"
SOURCE_URL = "https://careers.confluent.io/jobs?page=1"


def _extract_first_html(payload: object) -> str:
    if isinstance(payload, dict):
        content = payload.get("content")
        if isinstance(content, dict):
            for key in ("raw", "raw_html", "html", "text", "body", "result"):
                val = content.get(key)
                if isinstance(val, str) and "<html" in val.lower():
                    return val
        for key in ("raw_html", "html", "body", "text"):
            val = payload.get(key)
            if isinstance(val, str) and "<html" in val.lower():
                return val
        for value in payload.values():
            found = _extract_first_html(value)
            if found:
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = _extract_first_html(value)
            if found:
                return found
    return ""


def _load_html(path: Path) -> str:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        payload = payload.get("response")
    html = _extract_first_html(payload)
    if not html:
        raise AssertionError(f"Unable to extract HTML from {path}")
    return html


def _build_scrape_payload(source_url: str, html: str) -> dict:
    return {
        "sourceUrl": source_url,
        "provider": "spidercloud",
        "items": {"provider": "spidercloud", "raw": [{"content": html}]},
    }


def test_confluent_jobs_listing_extracts_pagination_and_jobs():
    html = _load_html(PAGE_1)
    scrape = _build_scrape_payload(SOURCE_URL, html)

    urls = acts._extract_job_urls_from_scrape(scrape)  # noqa: SLF001

    assert any(url.startswith("https://careers.confluent.io/jobs/job/") for url in urls)
    assert "https://careers.confluent.io/jobs/?page=2" in urls
    assert "https://careers.confluent.io/jobs/?page=1" not in urls


def test_confluent_jobs_page_two_extracts_jobs_and_next_page():
    html = _load_html(PAGE_2)
    scrape = _build_scrape_payload("https://careers.confluent.io/jobs?page=2", html)

    urls = acts._extract_job_urls_from_scrape(scrape)  # noqa: SLF001

    assert "https://careers.confluent.io/jobs/job/ca3f2007-6218-4d96-93a5-32230addfd31" in urls
    assert "https://careers.confluent.io/jobs/?page=3" in urls
    assert "https://careers.confluent.io/jobs/?page=2" not in urls


@pytest.mark.asyncio
async def test_store_scrape_enqueues_confluent_pagination(monkeypatch):
    html = _load_html(PAGE_1)
    scrape_payload = {
        "sourceUrl": SOURCE_URL,
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "items": {"provider": "spidercloud", "raw": [{"content": html}]},
    }

    calls: list[dict] = []

    async def fake_mutation(name: str, args: dict):
        calls.append({"name": name, "args": args})
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            return {"inserted": 0}
        return None

    async def fake_seen(*_args, **_kwargs):
        return []

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)
    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_seen)

    await acts.store_scrape(scrape_payload)

    enqueue_calls = [call for call in calls if call["name"] == "router:enqueueScrapeUrls"]
    assert enqueue_calls, "store_scrape should enqueue Confluent pagination URLs"

    urls = enqueue_calls[0]["args"]["urls"]
    assert "https://careers.confluent.io/jobs/?page=2" in urls
    assert any(url.startswith("https://careers.confluent.io/jobs/job/") for url in urls)

    delays = enqueue_calls[0]["args"].get("delaysMs") or []
    delay_for_page_2 = None
    for url, delay in zip(urls, delays):
        if url == "https://careers.confluent.io/jobs/?page=2":
            delay_for_page_2 = delay
            break
    assert delay_for_page_2 is not None and delay_for_page_2 > 0
