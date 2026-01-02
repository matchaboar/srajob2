from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime
from typing import Any, Dict, List

import pytest

sys.path.insert(0, os.path.abspath("."))

try:
    from temporalio.exceptions import ApplicationError
except Exception:  # pragma: no cover - optional dependency
    pytest.skip("temporalio not installed", allow_module_level=True)

from job_scrape_application.workflows import scrape_workflow as sw


class _Info:
    run_id = "run-1"
    workflow_id = "wf-1"
    task_queue = "test-queue"


async def _noop_sleep(_duration) -> None:
    return None


class _ActivityHarness:
    def __init__(self) -> None:
        self.calls: List[str] = []
        self.complete_calls: List[Dict[str, Any]] = []
        self.workflow_runs: List[Dict[str, Any]] = []
        self.batch: Dict[str, Any] | None = None
        self.process_result: Dict[str, Any] | None = None
        self.process_error: Exception | None = None
        self.store_outcomes: List[Any] = []

    async def execute(self, activity, args=None, **kwargs):  # type: ignore[override]
        name = getattr(activity, "__name__", str(activity))
        self.calls.append(name)

        if activity is sw.lease_scrape_url_batch:
            return self.batch

        if activity is sw.process_spidercloud_job_batch:
            if self.process_error:
                raise self.process_error
            return self.process_result

        if activity is sw.complete_scrape_urls:
            payload = args[0] if isinstance(args, list) else args
            if isinstance(payload, dict):
                self.complete_calls.append(payload)
            return None

        if activity is sw.record_workflow_run:
            payload = args[0] if isinstance(args, list) else args
            if isinstance(payload, dict):
                self.workflow_runs.append(payload)
            return None

        if activity is sw.store_scrape:
            if not self.store_outcomes:
                return "scr-default"
            outcome = self.store_outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        raise AssertionError(f"Unexpected activity {name}")

    def start_activity(self, activity, args=None, **kwargs):  # type: ignore[override]
        async def _runner():
            return await self.execute(activity, args=args, **kwargs)

        return asyncio.create_task(_runner())


@pytest.mark.asyncio
async def test_job_details_no_urls_returns_empty_summary(monkeypatch):
    harness = _ActivityHarness()
    harness.batch = {"urls": []}

    monkeypatch.setattr(sw.workflow, "execute_activity", harness.execute)
    monkeypatch.setattr(sw.workflow, "start_activity", harness.start_activity)
    monkeypatch.setattr(sw.workflow, "sleep", _noop_sleep)
    monkeypatch.setattr(sw.workflow, "now", lambda: datetime.fromtimestamp(1_700_000_000))
    monkeypatch.setattr(sw.workflow, "info", lambda: _Info())

    wf = sw.SpidercloudJobDetailsWorkflow()
    summary = await wf.run()

    assert summary.site_count == 0
    assert summary.scrape_ids == []
    assert harness.workflow_runs


@pytest.mark.asyncio
async def test_job_details_records_skipped_urls_and_completes(monkeypatch):
    harness = _ActivityHarness()
    harness.batch = {
        "urls": [{"url": "https://example.com/a"}, {"url": "https://example.com/b"}],
        "skippedUrls": ["https://skip.example/1", "https://skip.example/2"],
    }
    harness.process_result = {
        "scrapes": [
            {"subUrls": ["https://example.com/a"], "sourceUrl": "https://example.com/a"},
            {"subUrls": ["https://example.com/b"], "sourceUrl": "https://example.com/b"},
        ]
    }
    harness.store_outcomes = ["scr-1", "scr-2"]

    monkeypatch.setattr(sw.workflow, "execute_activity", harness.execute)
    monkeypatch.setattr(sw.workflow, "start_activity", harness.start_activity)
    monkeypatch.setattr(sw.workflow, "sleep", _noop_sleep)
    monkeypatch.setattr(sw.workflow, "now", lambda: datetime.fromtimestamp(1_700_000_010))
    monkeypatch.setattr(sw.workflow, "info", lambda: _Info())

    wf = sw.SpidercloudJobDetailsWorkflow()
    summary = await wf.run()

    assert summary.site_count == 1
    assert summary.scrape_ids == ["scr-1", "scr-2"]

    completed_calls = [c for c in harness.complete_calls if c.get("status") == "completed"]
    assert completed_calls
    assert all(len(call.get("urls", [])) == 1 for call in completed_calls)
    completed_urls = sorted(call["urls"][0] for call in completed_calls)
    assert completed_urls == ["https://example.com/a", "https://example.com/b"]


@pytest.mark.asyncio
async def test_job_details_marks_invalid_scrapes(monkeypatch):
    harness = _ActivityHarness()
    harness.batch = {"urls": [{"url": "https://example.com/a"}, {"url": "https://example.com/b"}]}
    harness.process_result = {
        "scrapes": [
            {"subUrls": ["https://example.com/a"], "sourceUrl": "https://example.com/a"},
            {"subUrls": ["https://example.com/b"], "sourceUrl": "https://example.com/b"},
        ]
    }
    harness.store_outcomes = [
        ApplicationError("bad payload", type="invalid_scrape"),
        "scr-ok",
    ]

    monkeypatch.setattr(sw.workflow, "execute_activity", harness.execute)
    monkeypatch.setattr(sw.workflow, "start_activity", harness.start_activity)
    monkeypatch.setattr(sw.workflow, "sleep", _noop_sleep)
    monkeypatch.setattr(sw.workflow, "now", lambda: datetime.fromtimestamp(1_700_000_020))
    monkeypatch.setattr(sw.workflow, "info", lambda: _Info())

    wf = sw.SpidercloudJobDetailsWorkflow()
    summary = await wf.run()

    assert summary.scrape_ids == ["scr-ok"]

    invalid_calls = [c for c in harness.complete_calls if c.get("status") == "invalid"]
    assert invalid_calls
    assert all(len(call.get("urls", [])) == 1 for call in invalid_calls)
    assert invalid_calls[0]["urls"][0] == "https://example.com/a"

    completed_calls = [c for c in harness.complete_calls if c.get("status") == "completed"]
    assert completed_calls
    assert all(len(call.get("urls", [])) == 1 for call in completed_calls)
    assert completed_calls[0]["urls"][0] == "https://example.com/b"


@pytest.mark.asyncio
async def test_job_details_marks_failed_scrapes(monkeypatch):
    harness = _ActivityHarness()
    harness.batch = {"urls": [{"url": "https://example.com/a"}]}
    harness.process_result = {
        "scrapes": [
            {"subUrls": ["https://example.com/a"], "sourceUrl": "https://example.com/a"},
        ]
    }
    harness.store_outcomes = [RuntimeError("store failed")]

    monkeypatch.setattr(sw.workflow, "execute_activity", harness.execute)
    monkeypatch.setattr(sw.workflow, "start_activity", harness.start_activity)
    monkeypatch.setattr(sw.workflow, "sleep", _noop_sleep)
    monkeypatch.setattr(sw.workflow, "now", lambda: datetime.fromtimestamp(1_700_000_030))
    monkeypatch.setattr(sw.workflow, "info", lambda: _Info())

    wf = sw.SpidercloudJobDetailsWorkflow()
    summary = await wf.run()

    assert summary.scrape_ids == []

    failed_calls = [c for c in harness.complete_calls if c.get("status") == "failed"]
    assert failed_calls
    assert all(len(call.get("urls", [])) == 1 for call in failed_calls)
    assert failed_calls[0]["urls"][0] == "https://example.com/a"


@pytest.mark.asyncio
async def test_job_details_batch_failure_releases_urls(monkeypatch):
    harness = _ActivityHarness()
    harness.batch = {
        "urls": [{"url": "https://example.com/a"}, {"url": "https://example.com/b"}],
    }
    harness.process_error = RuntimeError("batch failed")

    monkeypatch.setattr(sw.workflow, "execute_activity", harness.execute)
    monkeypatch.setattr(sw.workflow, "start_activity", harness.start_activity)
    monkeypatch.setattr(sw.workflow, "sleep", _noop_sleep)
    monkeypatch.setattr(sw.workflow, "now", lambda: datetime.fromtimestamp(1_700_000_040))
    monkeypatch.setattr(sw.workflow, "info", lambda: _Info())

    wf = sw.SpidercloudJobDetailsWorkflow()
    summary = await wf.run()

    assert summary.scrape_ids == []
    failed_calls = [c for c in harness.complete_calls if c.get("status") == "failed"]
    assert failed_calls
    assert all(len(call.get("urls", [])) == 1 for call in failed_calls)
    failed_urls = sorted(call["urls"][0] for call in failed_calls)
    assert failed_urls == ["https://example.com/a", "https://example.com/b"]
