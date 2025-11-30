from __future__ import annotations

import os
import sys
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities as acts  # noqa: E402


@pytest.mark.asyncio
async def test_store_scrape_sets_truncated_on_fallback(monkeypatch):
    payload = {
        "sourceUrl": "https://example.com",
        "items": {"normalized": [{"url": "https://example.com/job"}]},
    }

    calls: Dict[str, Any] = {"attempts": 0, "args": []}

    async def fake_mutation(name: str, args: Dict[str, Any]):
        if name == "router:insertScrapeRecord":
            calls["attempts"] += 1
            calls["args"].append(args)
            if calls["attempts"] == 1:
                raise RuntimeError("first failure")
            return "scrape-id"
        return None

    def fake_trim(scrape: Dict[str, Any], **kwargs):
        return {**scrape, "items": {"normalized": scrape["items"]["normalized"]}}

    monkeypatch.setattr(acts, "trim_scrape_for_convex", fake_trim)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    res = await acts.store_scrape(payload)

    assert res == "scrape-id"
    assert calls["attempts"] == 2
    assert calls["args"][1]["items"].get("truncated") is True
