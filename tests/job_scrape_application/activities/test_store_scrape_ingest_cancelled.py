from __future__ import annotations

import asyncio
from typing import Any, Dict

import pytest


from job_scrape_application.workflows import activities as acts  # noqa: E402


@pytest.mark.asyncio
async def test_store_scrape_ingest_cancelled_is_nonfatal(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "sourceUrl": "https://example.com",
        "items": {"normalized": [{"url": "https://example.com/job"}]},
    }

    async def fake_mutation(name: str, args: Dict[str, Any]):
        if name == "router:ingestJobsFromScrape":
            raise asyncio.CancelledError()
        return "scrape-id"

    monkeypatch.setattr(acts, "trim_scrape_for_convex", lambda x, **kwargs: x)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)
    monkeypatch.setattr(acts.activity, "is_cancelled", lambda: False, raising=False)

    res = await acts.store_scrape(payload)

    assert res == "scrape-id"


@pytest.mark.asyncio
async def test_store_scrape_ingest_timeout_is_nonfatal(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "sourceUrl": "https://example.com",
        "items": {"normalized": [{"url": "https://example.com/job"}]},
    }
    emitted: list[Dict[str, Any]] = []

    async def fake_mutation(name: str, args: Dict[str, Any]):
        if name == "router:ingestJobsFromScrape":
            raise asyncio.TimeoutError()
        return "scrape-id"

    def fake_log(payload: Dict[str, Any]) -> None:
        emitted.append(payload)

    monkeypatch.setattr(acts, "trim_scrape_for_convex", lambda x, **kwargs: x)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)
    monkeypatch.setattr(acts.telemetry, "emit_posthog_log", fake_log)

    res = await acts.store_scrape(payload)

    assert res == "scrape-id"
    assert any(entry.get("event") == "ingest.jobs_timeout" for entry in emitted)
