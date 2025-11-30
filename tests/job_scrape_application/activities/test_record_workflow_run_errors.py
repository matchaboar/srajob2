from __future__ import annotations

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities as acts  # noqa: E402


@pytest.mark.asyncio
async def test_record_workflow_run_swallows_cancelled(monkeypatch):
    payload = {"runId": "r1"}

    async def fake_mutation(name: str, args):
        raise asyncio.CancelledError()

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    # Should not raise
    await acts.record_workflow_run(payload)


@pytest.mark.asyncio
async def test_record_workflow_run_raises_other_errors(monkeypatch):
    payload = {"runId": "r2"}

    async def fake_mutation(name: str, args):
        raise RuntimeError("db down")

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    with pytest.raises(RuntimeError):
        await acts.record_workflow_run(payload)
