from __future__ import annotations

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath("."))
from job_scrape_application.workflows import activities as acts  # noqa: E402
from job_scrape_application.services import convex_client  # noqa: E402


@pytest.mark.asyncio
async def test_record_workflow_run_handles_cancelled(monkeypatch):
    async def fake_convex_mutation(name: str, args: dict | None = None):
        raise asyncio.CancelledError

    monkeypatch.setattr(convex_client, "convex_mutation", fake_convex_mutation)

    # Should not raise on cancellation (best-effort logging only)
    await acts.record_workflow_run({"workflowId": "abc", "status": "cancelled"})


@pytest.mark.asyncio
async def test_record_workflow_run_raises_on_other_errors(monkeypatch):
    async def fake_convex_mutation(name: str, args: dict | None = None):
        raise ValueError("boom")

    monkeypatch.setattr(convex_client, "convex_mutation", fake_convex_mutation)

    with pytest.raises(RuntimeError):
        await acts.record_workflow_run({"workflowId": "abc", "status": "failed"})


def test_trim_scrape_for_convex_truncates_and_strips_raw():
    long_description = "x" * 2000
    scrape = {
        "sourceUrl": "https://example.com",
        "items": {
            "normalized": [
                {"url": "https://example.com/1", "description": long_description},
                {"url": "https://example.com/2", "description": "short"},
            ],
            "raw": {"huge": "y" * 10_000},
        },
    }

    trimmed = acts.trim_scrape_for_convex(
        scrape, max_items=1, max_description=100, raw_preview_chars=50
    )

    items = trimmed["items"]
    assert len(items["normalized"]) == 1  # limited by max_items
    assert len(items["normalized"][0]["description"]) == 100  # truncated description
    assert "raw" not in items
    assert "rawPreview" in items  # preview retained instead of raw blob


def test_jobs_from_scrape_items_filters_and_defaults():
    items = {
        "normalized": [
            {"url": "https://example.com/1", "title": "Engineer", "company": None, "remote": None},
            {"title": "Missing URL"},
        ]
    }

    jobs = acts._jobs_from_scrape_items(items, default_posted_at=1234)

    assert len(jobs) == 1
    job = jobs[0]
    assert job["title"] == "Engineer"
    assert job["company"] == "Unknown"  # default fallback
    assert job["remote"] is False
    assert job["level"] == "mid"
    assert job["totalCompensation"] == 0
    assert job.get("compensationUnknown") is True
    assert "compensationReason" in job
    assert job["postedAt"] == 1234
