from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest


try:
    from temporalio.exceptions import ActivityError, ApplicationError, TimeoutError, TimeoutType
except Exception:  # pragma: no cover - optional dependency
    pytest.skip("temporalio not installed", allow_module_level=True)

from job_scrape_application.workflows._archive import temporal_heuristic_workflow as hw
from job_scrape_application.workflows._archive import temporal_scrape_workflow as sw


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
        self.batch_responses: List[Dict[str, Any] | None] | None = None
        self.lease_calls = 0
        self.process_result: Dict[str, Any] | None = None
        self.process_results: List[Dict[str, Any]] | None = None
        self.process_calls = 0
        self.process_error: Exception | None = None
        self.store_outcomes: List[Any] = []

    async def execute(self, activity, args=None, **kwargs):  # type: ignore[override]
        name = getattr(activity, "__name__", str(activity))
        self.calls.append(name)

        if activity is sw.lease_scrape_url_batch:
            self.lease_calls += 1
            if self.batch_responses is not None:
                idx = self.lease_calls - 1
                if idx < len(self.batch_responses):
                    return self.batch_responses[idx]
                return {"urls": []}
            if self.batch is None:
                return None
            if self.lease_calls > 1:
                return {"urls": []}
            return self.batch

        if activity is sw.process_spidercloud_job_batch:
            if self.process_error:
                raise self.process_error
            if self.process_results is not None:
                idx = self.process_calls
                self.process_calls += 1
                if idx < len(self.process_results):
                    return self.process_results[idx]
                return self.process_results[-1]
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


@pytest.fixture(autouse=True)
def _fast_workflow_checkpoint(monkeypatch):
    async def _noop_checkpoint(*_args, **_kwargs):
        return None

    monkeypatch.setattr(sw, "workflow_checkpoint", _noop_checkpoint)


@pytest.mark.asyncio
async def test_job_details_no_urls_returns_empty_summary(monkeypatch):
    harness = _ActivityHarness()
    harness.batch = {"urls": []}

    monkeypatch.setattr(sw.settings, "persist_scrapes_in_activity", True)
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
async def test_listing_workflow_uses_listing_batch_size(monkeypatch):
    calls: list[dict[str, Any]] = []

    async def fake_execute(activity, args=None, **kwargs):  # type: ignore[override]
        if activity is sw.lease_scrape_url_batch:
            calls.append({"args": args})
            return {"urls": []}
        if activity is sw.process_spidercloud_listing_batch:
            return {"queued": 0, "listingCompleted": 0}
        if activity in (sw.complete_scrape_urls, sw.record_workflow_run):
            return None
        return {"scrapes": []}

    monkeypatch.setattr(sw.settings, "persist_scrapes_in_activity", True)
    monkeypatch.setattr(sw.workflow, "execute_activity", fake_execute)
    monkeypatch.setattr(sw.workflow, "sleep", _noop_sleep)
    monkeypatch.setattr(sw.workflow, "now", lambda: datetime.fromtimestamp(1_700_000_020))
    monkeypatch.setattr(sw.workflow, "info", lambda: _Info())

    wf = sw.SpidercloudListingWorkflow()
    summary = await wf.run()

    assert summary.site_count == 0
    assert calls
    lease_args = calls[0]["args"] or []
    assert lease_args[1] == sw.runtime_config.spidercloud_listing_batch_size
    assert lease_args[2] == "listing"


@pytest.mark.asyncio
async def test_listing_batch_failure_records_reason(monkeypatch):
    batch = {"urls": [{"url": "https://example.com/jobs?page=1"}]}
    recorded_runs: list[dict[str, Any]] = []
    captured_error: dict[str, Any] = {"error": None}

    timeout = TimeoutError(
        "activity ScheduleToClose timeout",
        type=TimeoutType.SCHEDULE_TO_CLOSE,
        last_heartbeat_details=[],
    )
    listing_error: ActivityError | None = None
    try:
        raise ActivityError(
            "activity ScheduleToClose timeout",
            scheduled_event_id=1,
            started_event_id=2,
            identity="worker-1",
            activity_type="process_spidercloud_listing_batch",
            activity_id="act-2",
            retry_state=None,
        ) from timeout
    except ActivityError as exc:
        listing_error = exc

    state = {"leases": 0}

    async def fake_execute(activity, args=None, **kwargs):  # type: ignore[override]
        if activity is sw.lease_scrape_url_batch:
            state["leases"] += 1
            return batch if state["leases"] == 1 else {"urls": []}
        if activity is sw.process_spidercloud_listing_batch:
            if listing_error is None:
                raise AssertionError("listing_error not set")
            raise listing_error
        if activity is sw.complete_scrape_urls:
            payload = args[0] if isinstance(args, list) else args
            if isinstance(payload, dict):
                captured_error["error"] = payload.get("error")
            return None
        if activity is sw.record_workflow_run:
            payload = args[0] if isinstance(args, list) else args
            if isinstance(payload, dict):
                recorded_runs.append(payload)
            return None
        return None

    def fake_decision(_exc, *, source=None):
        return SimpleNamespace(
            action="fail",
            error=sw._format_failure_reason(listing_error) if listing_error else "unknown",
            retry_after_seconds=None,
        )

    monkeypatch.setattr(sw, "decision_for_exception", fake_decision)
    monkeypatch.setattr(sw.workflow, "execute_activity", fake_execute)
    monkeypatch.setattr(sw.workflow, "sleep", _noop_sleep)
    monkeypatch.setattr(sw.workflow, "now", lambda: datetime.fromtimestamp(1_700_000_060))
    monkeypatch.setattr(sw.workflow, "info", lambda: _Info())

    await sw.SpidercloudListingWorkflow().run()

    assert "activity=process_spidercloud_listing_batch" in (captured_error["error"] or "")
    assert "timeout=SCHEDULE_TO_CLOSE" in (captured_error["error"] or "")
    assert "https://example.com/jobs?page=1" in (captured_error["error"] or "")
    assert recorded_runs
    assert any(
        "activity=process_spidercloud_listing_batch" in (run.get("error") or "")
        and "timeout=SCHEDULE_TO_CLOSE" in (run.get("error") or "")
        and "https://example.com/jobs?page=1" in (run.get("error") or "")
        for run in recorded_runs
    )


@pytest.mark.asyncio
async def test_job_details_uses_activity_scrape_ids(monkeypatch):
    harness = _ActivityHarness()
    harness.batch = {
        "urls": [{"url": "https://example.com/a"}, {"url": "https://example.com/b"}],
        "skippedUrls": ["https://skip.example/1", "https://skip.example/2"],
    }
    harness.process_result = {
        "scrapeIds": ["scr-1", "scr-2"],
        "stored": 2,
        "invalid": 0,
        "failed": 0,
    }

    monkeypatch.setattr(sw.settings, "persist_scrapes_in_activity", True)
    monkeypatch.setattr(sw.workflow, "execute_activity", harness.execute)
    monkeypatch.setattr(sw.workflow, "start_activity", harness.start_activity)
    monkeypatch.setattr(sw.workflow, "sleep", _noop_sleep)
    monkeypatch.setattr(sw.workflow, "now", lambda: datetime.fromtimestamp(1_700_000_010))
    monkeypatch.setattr(sw.workflow, "info", lambda: _Info())

    wf = sw.SpidercloudJobDetailsWorkflow()
    summary = await wf.run()

    assert summary.site_count == 1
    assert summary.scrape_ids == ["scr-1", "scr-2"]
    assert harness.complete_calls == []


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

    monkeypatch.setattr(sw.settings, "persist_scrapes_in_activity", False)
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
    invalid_items = [item for call in invalid_calls for item in (call.get("items") or [])]
    assert len(invalid_items) == 1
    assert invalid_items[0]["url"] == "https://example.com/a"

    completed_calls = [c for c in harness.complete_calls if c.get("status") == "completed"]
    assert completed_calls
    completed_items = [item for call in completed_calls for item in (call.get("items") or [])]
    assert len(completed_items) == 1
    assert completed_items[0]["url"] == "https://example.com/b"


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

    monkeypatch.setattr(sw.settings, "persist_scrapes_in_activity", False)
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
    failed_items = [item for call in failed_calls for item in (call.get("items") or [])]
    assert len(failed_items) == 1
    assert failed_items[0]["url"] == "https://example.com/a"


@pytest.mark.asyncio
async def test_job_details_batch_failure_releases_urls(monkeypatch):
    harness = _ActivityHarness()
    harness.batch = {
        "urls": [{"url": "https://example.com/a"}, {"url": "https://example.com/b"}],
    }
    timeout = TimeoutError(
        "activity ScheduleToClose timeout",
        type=TimeoutType.SCHEDULE_TO_CLOSE,
        last_heartbeat_details=[],
    )
    try:
        raise ActivityError(
            "activity ScheduleToClose timeout",
            scheduled_event_id=1,
            started_event_id=2,
            identity="worker-1",
            activity_type="process_spidercloud_job_batch",
            activity_id="act-1",
            retry_state=None,
        ) from timeout
    except ActivityError as exc:
        harness.process_error = exc

    def fake_decision(_exc, *, source=None):
        return SimpleNamespace(
            action="fail",
            error=sw._format_failure_reason(harness.process_error) if harness.process_error else "unknown",
            retry_after_seconds=None,
        )

    monkeypatch.setattr(sw, "decision_for_exception", fake_decision)
    monkeypatch.setattr(sw.settings, "persist_scrapes_in_activity", True)
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
    failed_items = [item for call in failed_calls for item in (call.get("items") or [])]
    failed_urls = sorted(item["url"] for item in failed_items)
    assert failed_urls == ["https://example.com/a", "https://example.com/b"]
    assert any(
        "activity=process_spidercloud_job_batch" in (call.get("error") or "")
        and "timeout=SCHEDULE_TO_CLOSE" in (call.get("error") or "")
        and "https://example.com/a" in (call.get("error") or "")
        for call in failed_calls
    )

    assert harness.workflow_runs
    assert any(
        "activity=process_spidercloud_job_batch" in (run.get("error") or "")
        and "timeout=SCHEDULE_TO_CLOSE" in (run.get("error") or "")
        and "https://example.com/a" in (run.get("error") or "")
        for run in harness.workflow_runs
    )


@pytest.mark.asyncio
async def test_job_details_yields_on_large_batches(monkeypatch):
    harness = _ActivityHarness()
    item_count = 26
    harness.batch = {
        "urls": [{"url": f"https://example.com/{idx}"} for idx in range(item_count)],
    }
    harness.process_result = {
        "scrapes": [
            {"subUrls": [f"https://example.com/{idx}"], "sourceUrl": f"https://example.com/{idx}"}
            for idx in range(item_count)
        ]
    }

    sleep_calls: List[object] = []

    async def fake_sleep(duration) -> None:
        sleep_calls.append(duration)

    monkeypatch.setattr(sw.settings, "persist_scrapes_in_activity", False)
    monkeypatch.setattr(sw.workflow, "execute_activity", harness.execute)
    monkeypatch.setattr(sw.workflow, "start_activity", harness.start_activity)
    monkeypatch.setattr(sw.workflow, "sleep", fake_sleep)
    monkeypatch.setattr(sw.workflow, "now", lambda: datetime.fromtimestamp(1_700_000_050))
    monkeypatch.setattr(sw.workflow, "info", lambda: _Info())

    wf = sw.SpidercloudJobDetailsWorkflow()
    await wf.run()

    assert sleep_calls, "Expected workflow.sleep to be called to yield in large batches"


@pytest.mark.asyncio
async def test_job_details_workflow_processes_multiple_batches(monkeypatch):
    harness = _ActivityHarness()
    harness.batch_responses = [
        {"urls": [{"url": "https://example.com/a"}]},
        {"urls": [{"url": "https://example.com/b"}]},
        {"urls": []},
    ]
    harness.process_results = [
        {"scrapeIds": ["scr-a"], "stored": 1, "invalid": 0, "failed": 0},
        {"scrapeIds": ["scr-b"], "stored": 1, "invalid": 0, "failed": 0},
    ]

    monkeypatch.setattr(sw.settings, "persist_scrapes_in_activity", True)
    monkeypatch.setattr(sw.workflow, "execute_activity", harness.execute)
    monkeypatch.setattr(sw.workflow, "start_activity", harness.start_activity)
    monkeypatch.setattr(sw.workflow, "sleep", _noop_sleep)
    monkeypatch.setattr(sw.workflow, "now", lambda: datetime.fromtimestamp(1_700_000_070))
    monkeypatch.setattr(sw.workflow, "info", lambda: _Info())

    summary = await sw.SpidercloudJobDetailsWorkflow().run()

    assert summary.site_count == 2
    assert summary.scrape_ids == ["scr-a", "scr-b"]

@pytest.mark.asyncio
async def test_heuristic_workflow_uses_dynamic_batch_limit(monkeypatch):
    calls: list[list[int]] = []
    base_time = datetime(2024, 1, 1, 0, 0, 0)
    ticks = {"count": 0}

    def fake_now():
        ticks["count"] += 1
        return base_time + timedelta(seconds=ticks["count"])

    async def fake_execute(_activity, args=None, **kwargs):  # type: ignore[override]
        if isinstance(args, list):
            calls.append(args)
        if len(calls) == 1:
            return {"processed": 1, "remaining": 50, "fetched": 50}
        return {"processed": 0, "remaining": 0, "fetched": 0}

    monkeypatch.setattr(hw.workflow, "execute_activity", fake_execute)
    monkeypatch.setattr(hw.workflow, "now", fake_now)

    await hw.HeuristicJobDetailsWorkflow().run()

    assert calls[0] == [hw.BATCH_LIMIT_DEFAULT]
