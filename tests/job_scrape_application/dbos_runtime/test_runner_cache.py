from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.abspath("."))

import job_scrape_application.dbos_runtime.runner as runner


@pytest.mark.asyncio
async def test_load_schedule_interval_uses_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[object] = []

    async def fake_query(name: str, payload: object) -> dict[str, object]:
        calls.append((name, payload))
        return {"intervalMinutes": 12}

    monkeypatch.setattr(runner, "convex_query", fake_query)
    runner._reset_cache()

    first = await runner._load_schedule_interval_minutes()
    second = await runner._load_schedule_interval_minutes()

    assert first == 12
    assert second == 12
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_enqueue_listing_sites_uses_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[object] = []

    async def fake_query(name: str, payload: object) -> list[dict[str, object]]:
        calls.append((name, payload))
        return [
            {"_id": "site-1", "url": "https://example.com/jobs", "scrapeProvider": "spidercloud"},
        ]

    monkeypatch.setattr(runner, "convex_query", fake_query)
    monkeypatch.setattr(runner, "enqueue_scrape_urls", lambda payload: {"queued": 1})
    runner._reset_cache()

    first = await runner._enqueue_listing_sites()
    second = await runner._enqueue_listing_sites()

    assert first == 1
    assert second == 1
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_interval_from_config_handles_daily() -> None:
    assert runner._interval_from_config({"mode": "daily"}) == 24 * 60
    assert runner._interval_from_config({"intervalMinutes": 5}) == 5
    assert runner._interval_from_config({}) == runner.DEFAULT_SCHEDULE_INTERVAL_MINUTES
