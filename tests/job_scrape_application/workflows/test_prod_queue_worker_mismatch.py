from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.services import convex_client  # noqa: E402
from job_scrape_application.workflows import create_schedule as cs  # noqa: E402
from job_scrape_application.workflows import worker as worker_mod  # noqa: E402


FIXTURE_PATH = Path(
    "tests/job_scrape_application/workflows/fixtures/convex_prod_queue_state.json"
)


def _load_fixture() -> Dict[str, Any]:
    if not FIXTURE_PATH.exists():
        pytest.skip("Missing prod queue fixture; run agent_scripts/export_convex_queue_state_fixture.py")
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        pytest.skip("Queue fixture payload missing or invalid")
    tables = payload.get("tables")
    if not isinstance(tables, dict):
        pytest.skip("Queue fixture tables missing")
    if not tables.get("scrape_url_queue"):
        pytest.skip("Queue fixture has no scrape_url_queue rows")
    return payload


class FakeConvex:
    def __init__(self, payload: Dict[str, Any]) -> None:
        tables = payload.get("tables", {}) if isinstance(payload, dict) else {}
        self.sites = tables.get("sites", []) if isinstance(tables, dict) else []
        self.queue_rows = tables.get("scrape_url_queue", []) if isinstance(tables, dict) else []
        self.seen_sources = tables.get("seen_job_urls", []) if isinstance(tables, dict) else []
        self.ignored_jobs = tables.get("ignored_jobs", []) if isinstance(tables, dict) else []
        self.reset_processing_calls = 0

    async def query(self, name: str, args: Dict[str, Any] | None = None) -> Any:
        args = args or {}
        if name == "router:listQueuedScrapeUrls":
            return self._list_queue(args)
        if name == "router:listSeenJobUrlsForSite":
            return self._list_seen(args)
        if name == "router:listSites":
            return self._list_sites(args)
        if name == "router:listIgnoredJobs":
            return self._list_ignored(args)
        raise RuntimeError(f"Unexpected query {name}")

    def _list_sites(self, args: Dict[str, Any]) -> List[Dict[str, Any]]:
        enabled_only = bool(args.get("enabledOnly"))
        if not enabled_only:
            return list(self.sites)
        return [row for row in self.sites if row.get("enabled") is True]

    def _list_queue(self, args: Dict[str, Any]) -> List[Dict[str, Any]]:
        provider = args.get("provider")
        status = args.get("status")
        site_id = args.get("siteId")
        limit = int(args.get("limit") or 200)
        limit = max(1, min(limit, 500))

        rows = []
        for row in self.queue_rows:
            if provider and row.get("provider") != provider:
                continue
            if status and row.get("status") != status:
                continue
            if site_id and row.get("siteId") != site_id:
                continue
            rows.append(row)
            if len(rows) >= limit:
                break
        return rows

    def _list_seen(self, args: Dict[str, Any]) -> Dict[str, Any]:
        source_url = args.get("sourceUrl")
        pattern = args.get("pattern")
        urls: List[str] = []
        if isinstance(source_url, str):
            for row in self.seen_sources:
                if row.get("sourceUrl") != source_url:
                    continue
                if pattern is not None and row.get("pattern") != pattern:
                    continue
                urls.extend([u for u in row.get("urls", []) if isinstance(u, str)])
        return {"sourceUrl": source_url, "urls": urls}

    def _list_ignored(self, args: Dict[str, Any]) -> List[Dict[str, Any]]:
        limit = int(args.get("limit") or 200)
        limit = max(1, min(limit, 400))
        return list(self.ignored_jobs)[:limit]

    def reset_processing_queue(self) -> int:
        updated = 0
        for row in self.queue_rows:
            if row.get("status") == "processing":
                row["status"] = "pending"
                updated += 1
        self.reset_processing_calls += 1
        return updated


class FakeWorkerPool:
    def __init__(self, queues: List[str]) -> None:
        self.queues = {q for q in queues if isinstance(q, str)}

    def can_run(self, task_queue: str | None) -> bool:
        if not task_queue:
            return False
        return task_queue in self.queues


@pytest.mark.asyncio
async def test_prod_queue_pending_without_job_details_worker(monkeypatch):
    payload = _load_fixture()
    fake = FakeConvex(payload)

    async def fake_query(name: str, args: Dict[str, Any] | None = None):
        return await fake.query(name, args)

    monkeypatch.setattr(convex_client, "convex_query", fake_query)

    pending = await convex_client.convex_query(
        "router:listQueuedScrapeUrls",
        {"status": "pending", "provider": "spidercloud", "limit": 200},
    )
    assert pending, "fixture should include pending scrape_url_queue rows"

    sample_source = pending[0].get("sourceUrl")
    if isinstance(sample_source, str) and sample_source:
        seen = await convex_client.convex_query(
            "router:listSeenJobUrlsForSite",
            {"sourceUrl": sample_source},
        )
        assert isinstance(seen, dict)

    # start_worker.ps1 runs AFTER state exists: it resets processing rows, then sets queues,
    # updates schedules, and launches workers.
    fake.reset_processing_queue()
    assert fake.reset_processing_calls == 1

    # start_worker.ps1 sets these before creating schedules/workers.
    monkeypatch.setattr(cs.settings, "job_details_task_queue", "spidercloud-job-details-queue")
    monkeypatch.setattr(cs.settings, "task_queue", "scraper-task-queue")
    monkeypatch.setattr(worker_mod.settings, "job_details_task_queue", "spidercloud-job-details-queue")
    monkeypatch.setattr(worker_mod.settings, "task_queue", "scraper-task-queue")

    cfgs = cs.load_schedule_configs()
    cfg = next(cfg for cfg in cfgs if cfg.workflow == "SpidercloudJobDetails")
    schedule = cs.build_schedule(cfg)
    schedule_queue = schedule.action.task_queue

    # start_worker.ps1 spawns both "all" and "job-details" workers.
    monkeypatch.setattr(worker_mod.settings, "worker_role", "all")
    general_queue, _, _ = worker_mod._select_worker_config()
    monkeypatch.setattr(worker_mod.settings, "worker_role", "job-details")
    job_details_queue, _, _ = worker_mod._select_worker_config()
    pool = FakeWorkerPool([general_queue, job_details_queue])

    assert pool.can_run(
        schedule_queue
    ), "job-details schedule queue should be serviced by the worker pool"
