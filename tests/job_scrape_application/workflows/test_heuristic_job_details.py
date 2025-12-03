from __future__ import annotations

import os
import sys
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows.activities import process_pending_job_details_batch  # noqa: E402


@pytest.mark.asyncio
async def test_process_pending_job_details_batch_updates_jobs(monkeypatch):
    jobs: list[dict[str, Any]] = [
        {
            "_id": "job1",
            "title": "Senior Software Engineer",
            "description": "Location: New York, NY\nCompensation: $180,000",
            "url": "https://example.com/jobs/1",
            "location": "Unknown",
            "totalCompensation": 0,
            "compensationReason": "pending markdown structured extraction",
            "compensationUnknown": True,
            "heuristicAttempts": 2,
        }
    ]

    configs: list[dict[str, Any]] = []
    recorded: list[dict[str, Any]] = []
    updated: list[dict[str, Any]] = []

    async def fake_query(name: str, args: Dict[str, Any] | None = None):
        if name == "router:listPendingJobDetails":
            return jobs
        if name == "router:listJobDetailConfigs":
            return configs
        raise AssertionError(f"unexpected query {name}")

    async def fake_mutation(name: str, args: Dict[str, Any] | None = None):
        if name == "router:recordJobDetailHeuristic":
            recorded.append(args or {})
            return {"created": True}
        if name == "router:updateJobWithHeuristic":
            updated.append(args or {})
            return {"updated": True}
        raise AssertionError(f"unexpected mutation {name}")

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_query", fake_query)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    result = await process_pending_job_details_batch()

    assert result["processed"] == 1
    assert any(call.get("field") == "location" for call in recorded)
    assert any(call.get("field") == "compensation" for call in recorded)
    assert updated
    assert updated[0]["location"] == "New York, NY"
    assert updated[0]["totalCompensation"] == 180000
    assert updated[0]["heuristicAttempts"] == 3  # starts at 2, incremented by 1
    assert "heuristicLastTried" in updated[0]


@pytest.mark.asyncio
async def test_process_pending_job_details_batch_defaults_domain(monkeypatch):
    jobs: list[dict[str, Any]] = [
        {
            "_id": "job2",
            "title": "Engineer",
            "description": "Location: Austin, TX\n$150k",
            "url": "",  # triggers default domain fallback
            "location": "Unknown",
            "totalCompensation": 0,
            "compensationReason": "pending markdown structured extraction",
            "compensationUnknown": True,
        }
    ]

    recorded: list[dict[str, Any]] = []
    updated: list[dict[str, Any]] = []

    async def fake_query(name: str, args: Dict[str, Any] | None = None):
        if name == "router:listPendingJobDetails":
            return jobs
        if name == "router:listJobDetailConfigs":
            return []
        raise AssertionError(f"unexpected query {name}")

    async def fake_mutation(name: str, args: Dict[str, Any] | None = None):
        if name == "router:recordJobDetailHeuristic":
            recorded.append(args or {})
            return { "created": True }
        if name == "router:updateJobWithHeuristic":
            updated.append(args or {})
            return { "updated": True }
        raise AssertionError(f"unexpected mutation {name}")

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_query", fake_query)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    result = await process_pending_job_details_batch()

    assert result["processed"] == 1
    assert recorded, "expected heuristic to be recorded"
    assert recorded[0]["domain"] == "default"
    assert any("location" in upd for upd in updated)
    assert updated[0]["heuristicAttempts"] == 1
    assert "heuristicLastTried" in updated[0]


@pytest.mark.asyncio
async def test_process_pending_job_details_batch_accepts_non_us_location(monkeypatch):
    jobs: list[dict[str, Any]] = [
        {
            "_id": "job3",
            "title": "Senior Software Engineer",
            "description": "Role overview\nBangalore, India\n₹4,500,000 — ₹6,500,000 INR\nMore details...",
            "url": "https://careers.airbnb.com/jobs/123",
            "location": "Unknown",
            "totalCompensation": 0,
            "compensationReason": "pending markdown structured extraction",
            "compensationUnknown": True,
        }
    ]

    recorded: list[dict[str, Any]] = []
    updated: list[dict[str, Any]] = []

    async def fake_query(name: str, args: Dict[str, Any] | None = None):
        if name == "router:listPendingJobDetails":
            return jobs
        if name == "router:listJobDetailConfigs":
            return []
        raise AssertionError(f"unexpected query {name}")

    async def fake_mutation(name: str, args: Dict[str, Any] | None = None):
        if name == "router:recordJobDetailHeuristic":
            recorded.append(args or {})
            return {"created": True}
        if name == "router:updateJobWithHeuristic":
            updated.append(args or {})
            return {"updated": True}
        raise AssertionError(f"unexpected mutation {name}")

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_query", fake_query)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    result = await process_pending_job_details_batch()

    assert result["processed"] == 1
    assert updated and updated[0]["location"] == "Bangalore, India"
    assert updated[0]["currencyCode"] == "INR"
    assert updated[0]["heuristicAttempts"] == 1
