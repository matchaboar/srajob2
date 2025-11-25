from __future__ import annotations

import os
import sys

import pytest

# Ensure repo root is importable for job_scrape_application
sys.path.insert(0, os.path.abspath("."))
from job_scrape_application.workflows import activities as acts  # noqa: E402
from job_scrape_application.services import convex_client  # noqa: E402


def test_normalize_fetchfox_items_emits_convex_shape():
    payload = {
        "normalized": [
            {
                "job_title": "Sr. Backend Engineer",
                "company": "Example Co",
                "description": "Role building APIs",
                "location": "Remote - US",
                "remote": "hybrid",
                "level": "Principal Engineer",
                "salary": "$200,000",
                "url": "https://example.com/job/1",
                "posted_at": "2024-10-01T12:30:00Z",
            },
            {
                "title": "Data Intern",
                "employer": "Beta Corp",
                "description": "Summer internship",
                "city": "Austin, TX",
                "remote": None,
                "salary": "$50,000 - $60,000",
                "url": "https://example.com/job/2",
                "_timestamp": 1_700_000_000,
            },
        ]
    }

    normalized = acts.normalize_fetchfox_items(payload)
    assert len(normalized) == 2

    required_keys = {
        "title",
        "company",
        "location",
        "remote",
        "level",
        "total_compensation",
        "url",
        "description",
        "posted_at",
    }
    allowed_levels = {"junior", "mid", "senior", "staff"}

    first, second = normalized

    for row in normalized:
        assert required_keys.issubset(row.keys())
        assert isinstance(row["remote"], bool)
        assert row["level"] in allowed_levels
        assert isinstance(row["total_compensation"], int)
        assert isinstance(row["posted_at"], int)
        assert row["url"]

    # Spot check coercions
    assert first["remote"] is True  # "hybrid" should coerce to True
    assert second["remote"] is False  # no remote markers in title/location
    assert first["level"] == "staff"  # "Principal" -> staff
    assert second["level"] == "junior"  # internship should down-rank


@pytest.mark.asyncio
async def test_store_scrape_retries_on_transient_failure(monkeypatch):
    attempts: list[dict[str, object]] = []

    async def fake_convex_mutation(name: str, args: dict[str, object] | None = None):
        attempts.append({"name": name, "args": args})
        if len(attempts) == 1:
            raise RuntimeError("transient")
        return "abc123"

    monkeypatch.setattr(convex_client, "convex_mutation", fake_convex_mutation)

    payload = {
        "sourceUrl": "https://example.com",
        "items": {"normalized": [{"url": "https://example.com/job/1", "title": "Engineer"}]},
        "startedAt": 0,
        "completedAt": 0,
    }

    scrape_id = await acts.store_scrape(payload)

    assert scrape_id == "abc123"
    assert len(attempts) == 3  # insert fails once, retry succeeds, then ingest jobs

    # Second call should contain the fallback payload with a truncated marker
    second_args = attempts[1]["args"]
    assert isinstance(second_args, dict)
    items = second_args.get("items", {})
    assert isinstance(items, dict)
    assert items.get("truncated") is True

    # Third call should be the ingestJobs mutation
    assert attempts[2]["name"] == "router:ingestJobsFromScrape"
