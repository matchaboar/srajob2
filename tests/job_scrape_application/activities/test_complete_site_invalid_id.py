from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows.activities import complete_site, fail_site


@pytest.mark.asyncio
async def test_complete_site_ignores_non_convex_id(monkeypatch):
    called = {"mut": False}

    async def fake_mut(name: str, args):
        called["mut"] = True
        raise RuntimeError("should not be called")

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mut)

    # Should no-op and not raise
    await complete_site("manual-site-1")
    assert called["mut"] is False


@pytest.mark.asyncio
async def test_fail_site_ignores_non_convex_id(monkeypatch):
    called = {"mut": False}

    async def fake_mut(name: str, args):
        called["mut"] = True
        raise RuntimeError("should not be called")

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mut)

    await fail_site({"id": "manual-site-1", "error": "boom"})
    assert called["mut"] is False
