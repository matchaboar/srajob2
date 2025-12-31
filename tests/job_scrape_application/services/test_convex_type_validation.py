from __future__ import annotations

import os
import sys
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities as acts  # noqa: E402
from job_scrape_application.services import convex_client, telemetry  # noqa: E402


@pytest.mark.asyncio
async def test_record_scratchpad_sanitizes_required_strings(monkeypatch):
    """Guard against sending invalid types to Convex mutations (e.g., None for required strings)."""

    captured: Dict[str, Any] = {"payload": {}}

    def fake_emit(payload: Dict[str, Any]) -> None:
        captured["payload"] = payload or {}
        for key in ("siteUrl",):
            if key in captured["payload"]:
                assert isinstance(captured["payload"][key], str)

    monkeypatch.setattr(telemetry, "emit_posthog_log", fake_emit)

    entry = {
        "runId": "run-1",
        "workflowId": "wf-1",
        "workflowName": "TestWorkflow",
        "siteUrl": None,  # should be sanitized to empty string before Convex call
        "event": "test",
        "message": "hello",
    }

    await acts.record_scratchpad(entry)

    assert captured["payload"].get("siteUrl") == ""


@pytest.mark.asyncio
async def test_lease_scrape_url_batch_payload_types(monkeypatch):
    """Ensure Convex payload contains only supported types/keys."""

    captured: Dict[str, Any] = {}

    async def fake_convex_mutation(name: str, payload: Dict[str, Any] | None = None):
        captured["name"] = name
        captured["payload"] = payload or {}
        return {"urls": [{"url": "https://example.com/job/1"}]}

    monkeypatch.setattr(convex_client, "convex_mutation", fake_convex_mutation)

    res = await acts.lease_scrape_url_batch(provider=None, limit=5)

    assert captured["name"] == "router:leaseScrapeUrlBatch"
    payload = captured["payload"]
    assert "provider" not in payload  # None should be stripped
    assert payload["limit"] == 5
    assert isinstance(payload["processingExpiryMs"], int)
    assert res["urls"] == [{"url": "https://example.com/job/1"}]
    assert res.get("skippedUrls") == []


def test_jobs_from_scrape_items_produces_convex_safe_payload():
    """Ensure required Convex job fields are never None/invalid."""

    default_ts = 123456
    items = {
        "normalized": [
            {
                # Minimal normalized payload with intentional gaps
                "title": None,
                "job_title": "Untitled role",
                "company": None,
                "description": "Role details.\nRemote first.\n$120,000",
                "location": "",
                "remote": None,
                "level": None,
                "url": "https://example.com/jobs/1",
                "posted_at": None,
            }
        ]
    }

    jobs = acts._jobs_from_scrape_items(  # noqa: SLF001
        items,
        default_posted_at=default_ts,
        scraped_at=default_ts,
        scraped_with="spidercloud",
        workflow_name="test-workflow",
        scraped_cost_milli_cents=5000,
    )

    assert len(jobs) == 1
    job = jobs[0]
    # Required Convex fields should be present and non-None.
    for key in ("title", "company", "description", "location", "url"):
        assert key in job
        assert isinstance(job[key], str)
    assert isinstance(job["remote"], bool)
    assert isinstance(job["totalCompensation"], int)
    assert job["totalCompensation"] >= 0
    assert isinstance(job["postedAt"], int)
    assert job["postedAt"] == default_ts
    assert "scrapedCostMilliCents" in job
