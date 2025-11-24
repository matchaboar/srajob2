from __future__ import annotations

import os
import sys

import httpx
import pytest

# Ensure repo root is importable for job_scrape_application
sys.path.insert(0, os.path.abspath("."))
from job_scrape_application.workflows import activities as acts  # noqa: E402


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

    class FakeResponse:
        def __init__(self, status_code: int, data: dict[str, object] | None = None):
            self.status_code = status_code
            self._data = data or {}

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                request = httpx.Request("POST", "https://convex.test/api/scrapes")
                response = httpx.Response(self.status_code, request=request)
                raise httpx.HTTPStatusError("error", request=request, response=response)

        def json(self) -> dict[str, object]:
            return self._data

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url: str, json: dict[str, object]):
            attempts.append({"url": url, "json": json})
            if len(attempts) == 1:
                return FakeResponse(429)
            return FakeResponse(201, {"scrapeId": "abc123"})

    async def _noop_sleep(_duration: float):
        return None

    monkeypatch.setattr(acts.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(acts.asyncio, "sleep", _noop_sleep)
    monkeypatch.setattr(acts.settings, "convex_http_url", "https://convex.test")

    payload = {
        "sourceUrl": "https://example.com",
        "items": {"normalized": []},
        "startedAt": 0,
        "completedAt": 0,
    }

    scrape_id = await acts.store_scrape(payload)

    assert scrape_id == "abc123"
    assert len(attempts) == 2  # one failure (429) + one successful retry
