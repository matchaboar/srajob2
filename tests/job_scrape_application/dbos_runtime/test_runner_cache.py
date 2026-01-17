from __future__ import annotations

import sys

import pytest

import job_scrape_application.dbos_runtime.runner as runner
import job_scrape_application.services.convex_client as convex_client_module
# Import the step modules to ensure they're loaded into sys.modules
import job_scrape_application.dbos_runtime.step.load_schedule_interval_minutes  # noqa: F401
import job_scrape_application.dbos_runtime.step.enqueue_listing_sites  # noqa: F401

# Get the actual modules from sys.modules
schedule_module = sys.modules["job_scrape_application.dbos_runtime.step.load_schedule_interval_minutes"]
enqueue_sites_module = sys.modules["job_scrape_application.dbos_runtime.step.enqueue_listing_sites"]


def test_load_schedule_interval_uses_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[object] = []

    def fake_query(name: str, payload: object) -> dict[str, object]:
        calls.append((name, payload))
        return {"intervalMinutes": 12}

    # Patch the source module since the step module uses local imports
    monkeypatch.setattr(convex_client_module, "convex_query", fake_query)
    runner._reset_cache()

    first = schedule_module.load_schedule_interval_minutes()
    second = schedule_module.load_schedule_interval_minutes()

    assert first == 12
    assert second == 12
    assert len(calls) == 1


def test_enqueue_listing_sites_uses_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[object] = []

    def fake_query(name: str, payload: object) -> list[dict[str, object]]:
        calls.append((name, payload))
        return [
            {"_id": "site-1", "url": "https://example.com/jobs", "scrapeProvider": "spidercloud"},
        ]

    # Patch the source module since the step module uses local imports
    monkeypatch.setattr(convex_client_module, "convex_query", fake_query)
    monkeypatch.setattr(enqueue_sites_module, "enqueue_scrape_urls", lambda payload: {"queued": 1})
    runner._reset_cache()

    first = enqueue_sites_module.enqueue_listing_sites()
    second = enqueue_sites_module.enqueue_listing_sites()

    assert first == 1
    assert second == 1
    assert len(calls) == 1


def test_interval_from_config_handles_daily() -> None:
    assert schedule_module._interval_from_config({"mode": "daily"}) == 24 * 60
    assert schedule_module._interval_from_config({"intervalMinutes": 5}) == 5
    assert schedule_module._interval_from_config({}) == schedule_module.DEFAULT_SCHEDULE_INTERVAL_MINUTES
