from __future__ import annotations

import os
import sys
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities as acts  # noqa: E402


@pytest.mark.asyncio
async def test_store_scrape_omits_null_cost(monkeypatch):
    payload = {
        "sourceUrl": "https://example.com",
        "items": {"normalized": []},
    }

    calls: Dict[str, Any] = {}

    async def fake_mutation(name: str, args: Dict[str, Any]):
        calls["name"] = name
        calls["args"] = args
        return "scrape-id"

    monkeypatch.setattr(acts, "trim_scrape_for_convex", lambda x, **kwargs: x)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    res = await acts.store_scrape(payload)

    assert res == "scrape-id"
    assert "costMilliCents" not in calls["args"]


@pytest.mark.asyncio
async def test_store_scrape_omits_null_pattern(monkeypatch):
    payload = {
        "sourceUrl": "https://example.com",
        "pattern": None,
        "items": {"normalized": []},
    }

    calls: Dict[str, Any] = {}

    async def fake_mutation(name: str, args: Dict[str, Any]):
        calls["name"] = name
        calls["args"] = args
        return "scrape-id"

    monkeypatch.setattr(acts, "trim_scrape_for_convex", lambda x, **kwargs: x)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    res = await acts.store_scrape(payload)

    assert res == "scrape-id"
    assert "pattern" not in calls["args"]


@pytest.mark.asyncio
async def test_store_scrape_retries_on_failure(monkeypatch):
    payload = {
        "sourceUrl": "https://example.com",
        "items": {"normalized": []},
        "costMilliCents": 1500,
    }

    calls: Dict[str, Any] = {"attempts": 0}

    async def fake_mutation(name: str, args: Dict[str, Any]):
        calls["attempts"] += 1
        if calls["attempts"] == 1:
            raise RuntimeError("first failure")
        return "scrape-id"

    monkeypatch.setattr(acts, "trim_scrape_for_convex", lambda x, **kwargs: x)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    res = await acts.store_scrape(payload)

    assert res == "scrape-id"
    assert calls["attempts"] == 2


@pytest.mark.asyncio
async def test_store_scrape_ingest_jobs_failure_is_nonfatal(monkeypatch):
    now = 1_700_000_000_000
    payload = {
        "sourceUrl": "https://example.com",
        "completedAt": now,
        "items": {"normalized": [{"url": "https://example.com/job"}]},
    }

    async def fake_mutation(name: str, args: Dict[str, Any]):
        if name == "router:ingestJobsFromScrape":
            raise RuntimeError("ingest failed")
        return "scrape-id"

    monkeypatch.setattr(acts, "trim_scrape_for_convex", lambda x, **kwargs: x)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    res = await acts.store_scrape(payload)

    assert res == "scrape-id"
