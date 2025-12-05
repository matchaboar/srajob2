from __future__ import annotations

import os
import sys
from typing import Any, Dict
from pathlib import Path
import types

import pytest

sys.path.insert(0, os.path.abspath("."))

# Stub firecrawl dependency to avoid import errors when running in isolation.
firecrawl_mod = types.ModuleType("firecrawl")
firecrawl_mod.Firecrawl = type("Firecrawl", (), {})  # dummy class
sys.modules.setdefault("firecrawl", firecrawl_mod)
firecrawl_v2 = types.ModuleType("firecrawl.v2")
firecrawl_v2_types = types.ModuleType("firecrawl.v2.types")
firecrawl_v2_types.PaginationConfig = type("PaginationConfig", (), {})
sys.modules.setdefault("firecrawl.v2", firecrawl_v2)
sys.modules.setdefault("firecrawl.v2.types", firecrawl_v2_types)
fetchfox_mod = types.ModuleType("fetchfox_sdk")
fetchfox_mod.FetchFox = type("FetchFox", (), {})
sys.modules.setdefault("fetchfox_sdk", fetchfox_mod)

try:
    import temporalio  # noqa: F401
except ImportError:  # pragma: no cover
    pytest.skip("temporalio not installed", allow_module_level=True)

from job_scrape_application.workflows.activities import HEURISTIC_VERSION, process_pending_job_details_batch  # noqa: E402
from job_scrape_application.workflows.helpers.scrape_utils import parse_markdown_hints, strip_known_nav_blocks  # noqa: E402


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


@pytest.fixture
def datadog_markdown() -> str:
    path = Path("tests/fixtures/datadog-commonmark-spidercloud.md")
    return path.read_text(encoding="utf-8")


@pytest.fixture
def airbnb_markdown() -> str:
    path = Path("tests/fixtures/airbnb-commonmark-spidercloud.md")
    return path.read_text(encoding="utf-8")


def test_strip_known_nav_blocks(datadog_markdown):
    cleaned = strip_known_nav_blocks(datadog_markdown)

    assert "Pup Culture Blog" not in cleaned
    assert "All Jobs" not in cleaned
    assert "Madrid, Spain" in cleaned


def test_parse_markdown_hints_ignores_nav_block(datadog_markdown):
    hints = parse_markdown_hints(datadog_markdown)

    assert hints.get("locations") == ["Madrid, Spain", "Paris, France"]
    assert hints.get("remote") is None


@pytest.mark.asyncio
async def test_process_pending_job_details_batch_handles_multiple_locations(monkeypatch, datadog_markdown):
    jobs: list[dict[str, Any]] = [
        {
            "_id": "job-datadog",
            "title": "Senior Software Engineer - Full Stack",
            "description": datadog_markdown,
            "url": "https://www.datadoghq.com/careers/123",
            "location": "Unknown",
            "locations": [],
            "totalCompensation": 0,
            "compensationReason": "pending markdown structured extraction",
            "compensationUnknown": True,
            "heuristicAttempts": 4,
            "heuristicVersion": 1,
            "remote": False,
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
    assert updated, "expected heuristic update for datadog fixture"
    patch = updated[0]
    assert patch["locations"] == ["Madrid, Spain", "Paris, France"]
    assert patch["location"] == "Madrid, Spain"
    assert patch["heuristicAttempts"] == 5
    assert patch["heuristicVersion"] == HEURISTIC_VERSION
    assert patch["compensationUnknown"] is True
    assert patch.get("totalCompensation") in (None, 0)
    assert patch.get("remote") is None
    assert any(call.get("field") == "location" for call in recorded)


@pytest.mark.asyncio
async def test_process_pending_job_details_batch_updates_description_when_cleaned(monkeypatch, datadog_markdown):
    jobs: list[dict[str, Any]] = [
        {
            "_id": "job-datadog-legacy",
            "title": "Senior Software Engineer - Full Stack",
            "description": datadog_markdown,
            "url": "https://www.datadoghq.com/careers/legacy",
            "location": "Unknown",
            "locations": [],
            "totalCompensation": 0,
            "compensationReason": "pending markdown structured extraction",
            "compensationUnknown": True,
            "heuristicAttempts": 1,
            "heuristicVersion": 1,
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
    assert updated, "expected heuristic update for cleaned description"
    patch = updated[0]
    assert "Pup Culture" not in patch.get("description", "")
    assert patch["heuristicVersion"] == HEURISTIC_VERSION
    assert any(call.get("field") == "location" for call in recorded)


@pytest.mark.asyncio
async def test_process_pending_job_details_batch_handles_brazil_location(monkeypatch, airbnb_markdown):
    jobs: list[dict[str, Any]] = [
        {
            "_id": "job-airbnb",
            "title": "Senior Software Engineer, Payments",
            "description": airbnb_markdown,
            "url": "https://careers.airbnb.com/jobs/999",
            "location": "Unknown",
            "locations": [],
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
    assert updated, "expected heuristic update for airbnb fixture"
    patch = updated[0]
    assert patch["location"] == "Sao Paulo, Brazil"
    assert patch["locations"] == ["Sao Paulo, Brazil"]
    assert patch["countries"] == ["Brazil"]
    assert patch["country"] == "Brazil"
    assert any(call.get("field") == "location" for call in recorded)
