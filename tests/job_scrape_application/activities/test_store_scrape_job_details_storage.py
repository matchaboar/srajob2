from __future__ import annotations

from typing import Any, Dict

import pytest


from job_scrape_application.workflows import activities as acts  # noqa: E402


@pytest.mark.asyncio
async def test_store_scrape_ingest_uses_untrimmed_descriptions(monkeypatch: pytest.MonkeyPatch) -> None:
    long_description = "A" * 5000
    payload = {
        "sourceUrl": "https://example.com/jobs",
        "items": {
            "normalized": [
                {
                    "url": "https://example.com/jobs/1",
                    "title": "Software Engineer",
                    "company": "Example Co",
                    "location": "Remote",
                    "remote": True,
                    "level": "mid",
                    "total_compensation": 0,
                    "description": long_description,
                }
            ]
        },
    }

    calls: Dict[str, Any] = {"insert": None, "ingest": None}
    trim_args: Dict[str, Any] = {}

    async def fake_mutation(name: str, args: Dict[str, Any]):
        if name == "router:insertScrapeRecord":
            calls["insert"] = args
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            calls["ingest"] = args
            return None
        if name == "router:recordJobDetailHeuristic":
            return None
        return None

    async def fake_query(name: str, args: Dict[str, Any] | None = None):  # noqa: ARG001
        return []

    def fake_trim(scrape: Dict[str, Any], **kwargs):
        trim_args.update(kwargs)
        trimmed = {**scrape}
        items = trimmed.get("items") if isinstance(trimmed.get("items"), dict) else {}
        normalized = items.get("normalized") if isinstance(items, dict) else None
        if isinstance(normalized, list) and normalized:
            normalized = [dict(normalized[0], description="TRIMMED")]
            items = {**items, "normalized": normalized}
            trimmed["items"] = items
        return trimmed

    monkeypatch.setattr(acts, "trim_scrape_for_convex", fake_trim)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_query", fake_query)

    res = await acts.store_scrape(payload)

    assert res == "scrape-id"
    assert trim_args.get("max_description") == 2000
    assert calls["insert"]["items"]["normalized"][0]["description"] == "TRIMMED"
    assert calls["ingest"]["jobs"][0]["description"] == long_description
